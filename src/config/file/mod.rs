// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Configuration files for the MessageHub

use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use astarte_device_sdk::transport::mqtt::Credential;
use color_eyre::Section;
use eyre::{Report, WrapErr, eyre};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::error::ConfigError;
use crate::store::StoreDir;

pub mod dynamic;
pub mod sdk;

use self::dynamic::DynamicConfig;
use self::sdk::DeviceSdkOptions;

use super::{DEFAULT_GRPC_PORT, DEFAULT_HOST, MessageHubOptions, Override};

/// Default configuration file name for the dynamic config
pub const CONFIG_FILE_NAME_NO_EXT: &str = "50-message-hub-config";
/// Config file name for the dynamic config
pub const CONFIG_FILE_NAME: &str = "50-message-hub-config.toml";

/// Old configuration file name in the root of the config directory
///
/// This is now moved to the store `config/` directory.
pub const LEGACY_CONFIG_FILE_NAME: &str = "message-hub-config.toml";
/// Credential secret after the device is registered.
///
/// Legacy file, currently the pairing is handled directly from the [`astarte_device_sdk`].
const LEGACY_CREDENTIAL_FILE: &str = "credentials_secret";

/// Struct to deserialize the configuration options for the Astarte message hub.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    /// The Astarte realm the device belongs to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
    /// A unique ID for the device.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    /// The URL of the Astarte pairing API.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pairing_url: Option<String>,
    /// The credentials secret used to authenticate with Astarte.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_secret: Option<String>,
    /// Token used to register the device.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pairing_token: Option<String>,
    /// Directory containing the Astarte interfaces.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interfaces_directory: Option<PathBuf>,
    /// Whether to ignore SSL errors when connecting to Astarte.
    // DEPRECATED: since 0.5.3. Use the astarte map 'ignore_ssl' to configure the SDK
    #[serde(skip_serializing_if = "Option::is_none")]
    pub astarte_ignore_ssl: Option<bool>,
    /// The gRPC host to use bind.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc_socket_host: Option<IpAddr>,
    /// The gRPC port to use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc_socket_port: Option<u16>,
    /// Directory used by Astarte-Message-Hub to retain configuration and other persistent data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_directory: Option<PathBuf>,
    /// Dynamic configuration options
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dynamic_config: Option<DynamicConfig>,
    /// Astarte device SDK options.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub astarte: Option<DeviceSdkOptions>,
}

impl Config {
    pub(crate) fn default_storage_dir() -> PathBuf {
        match dirs::data_dir() {
            Some(mut p) => {
                p.push("message-hub");

                p
            }
            None => {
                warn!("couldn't get store_directory, using CWD");

                PathBuf::from("./")
            }
        }
    }

    pub(crate) fn has_device_id(&self) -> bool {
        self.device_id
            .as_ref()
            .is_some_and(|device_id| !device_id.is_empty())
    }

    /// Reads a configuration file.
    pub async fn read(path: impl AsRef<Path>) -> eyre::Result<Option<Self>> {
        let path = path.as_ref();
        let config = match tokio::fs::read_to_string(path).await {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                error!(path = %path.display(), "couldn't read configuration file");

                return Err(Report::new(err).wrap_err("couldn't read configuration file"));
            }
        };

        toml::from_str(&config)
            .map(Some)
            .wrap_err_with(|| format!("couldn't decode config file {}", path.display()))
    }

    /// Reads a config file and merges it into this one
    ///
    /// # Errors
    ///
    /// If the config exists but couldn't be read, or is invalid.
    ///
    /// Doesn't error if the file doesn't exists.
    pub async fn read_and_merge(&mut self, path: impl AsRef<Path>) -> eyre::Result<()> {
        let path = path.as_ref();

        if let Some(config) = Self::read(path).await? {
            info!(path = %path.display(), "config red");

            self.merge(config);
        } else {
            debug!(path = %path.display(), "config file missing");
        }

        Ok(())
    }

    /// Attempts to read a config file and merges it into this one
    ///
    /// # Errors
    ///
    /// If the config file couldn't be read or is invalid. Errors if the file doesn't exist
    pub async fn try_read_and_merge(&mut self, path: impl AsRef<Path>) -> eyre::Result<()> {
        let path = path.as_ref();

        if let Some(config) = Self::read(path).await? {
            info!(path = %path.display(), "config red");

            self.merge(config);

            Ok(())
        } else {
            error!(path = %path.display(), "configuration file doesn't exists");

            Err(eyre!("couldn't read configuration file"))
        }
    }

    /// Obtains the credential secret from the stored path or by registering the device.
    pub async fn legacy_credential_secret(&mut self, storage_dir: &StoreDir) -> eyre::Result<()> {
        if self
            .credentials_secret
            .as_ref()
            .is_some_and(|c| !c.is_empty())
        {
            debug!("credentials already set");

            return Ok(());
        }

        let store_dir = storage_dir.get_store_dir().await?;

        let legacy_cred_file = store_dir.join(LEGACY_CREDENTIAL_FILE);

        match tokio::fs::read_to_string(&legacy_cred_file).await {
            Ok(cred) => {
                debug!(
                    path = %legacy_cred_file.display(),
                    "using stored credentials"
                );

                self.credentials_secret = Some(cred);

                Ok(())
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                debug!("credentials not found");

                Ok(())
            }
            Err(err) => Err(Report::new(err)
                .wrap_err("couldn't read credential secret")
                .note(legacy_cred_file.display().to_string())),
        }
    }

    /// Validate the values are present for the server configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.realm.as_ref().is_none_or(|realm| realm.is_empty()) {
            return Err(ConfigError::MissingField("realm"));
        }

        if self
            .device_id
            .as_ref()
            .is_none_or(|device_id| device_id.is_empty())
        {
            return Err(ConfigError::MissingField("device_id"));
        }

        let secret = self
            .credentials_secret
            .as_ref()
            .is_some_and(|credentials_secret| !credentials_secret.is_empty());

        let token = self
            .pairing_token
            .as_ref()
            .is_some_and(|pairing_token| !pairing_token.is_empty());

        if !secret && !token {
            return Err(ConfigError::Credentials);
        }

        if self
            .pairing_url
            .as_ref()
            .is_none_or(|pairing_url| pairing_url.is_empty())
        {
            return Err(ConfigError::MissingField("pairing_url"));
        }

        if self
            .interfaces_directory
            .as_ref()
            .is_some_and(|p| !p.is_dir())
        {
            return Err(ConfigError::InvalidInterfaceDirectory(
                self.interfaces_directory.clone(),
            ));
        }

        Ok(())
    }
}

impl Override for Config {
    fn merge(&mut self, other: Self) {
        let Config {
            realm,
            device_id,
            pairing_url,
            credentials_secret,
            pairing_token,
            interfaces_directory,
            astarte_ignore_ssl,
            grpc_socket_host,
            grpc_socket_port,
            store_directory,
            astarte,
            dynamic_config,
        } = other;

        self.realm.merge(realm);
        self.device_id.merge(device_id);
        self.pairing_url.merge(pairing_url);
        self.credentials_secret.merge(credentials_secret);
        self.pairing_token.merge(pairing_token);
        self.interfaces_directory.merge(interfaces_directory);
        self.astarte_ignore_ssl.merge(astarte_ignore_ssl);
        self.grpc_socket_host.merge(grpc_socket_host);
        self.grpc_socket_port.merge(grpc_socket_port);
        self.store_directory.merge(store_directory);
        self.astarte.merge(astarte);
        self.dynamic_config.merge(dynamic_config);
    }
}

impl TryFrom<Config> for MessageHubOptions {
    type Error = ConfigError;

    fn try_from(value: Config) -> Result<Self, Self::Error> {
        let device_id = value
            .device_id
            .filter(|realm| !realm.is_empty())
            .ok_or(ConfigError::MissingField("realm"))?;

        let realm = value
            .realm
            .filter(|realm| !realm.is_empty())
            .ok_or(ConfigError::MissingField("realm"))?;

        let pairing_url = value
            .pairing_url
            .filter(|url| !url.is_empty())
            .ok_or(ConfigError::MissingField("pairing_url"))?;

        let credential = value
            .credentials_secret
            .filter(|s| !s.is_empty())
            .map(Credential::secret)
            .or_else(|| {
                value
                    .pairing_token
                    .filter(|t| !t.is_empty())
                    .map(Credential::paring_token)
            })
            .ok_or(ConfigError::Credentials)?;

        if value
            .interfaces_directory
            .as_ref()
            .is_some_and(|interfaces_directory| !interfaces_directory.is_dir())
        {
            return Err(ConfigError::InvalidInterfaceDirectory(
                value.interfaces_directory,
            ));
        }

        let grpc_socket_host = value.grpc_socket_host.unwrap_or(DEFAULT_HOST);
        let grpc_socket_port = value.grpc_socket_port.unwrap_or(DEFAULT_GRPC_PORT);

        let mut astarte = value.astarte.unwrap_or_default();

        if let Some(astarte_ignore_ssl) = value.astarte_ignore_ssl {
            astarte.ignore_ssl = Some(astarte_ignore_ssl);
        }

        Ok(MessageHubOptions {
            realm,
            device_id,
            pairing_url,
            credential,
            interfaces_directory: value.interfaces_directory,
            grpc_socket_host,
            grpc_socket_port,
            astarte,
        })
    }
}

#[cfg(test)]
mod test {
    use std::net::Ipv4Addr;
    use std::num::NonZero;

    use astarte_device_sdk::store::sqlite::Size;
    use insta::assert_toml_snapshot;
    use pretty_assertions::assert_eq;
    use rstest::{Context, rstest};

    use crate::config::file::dynamic::{Grpc, Http};
    use crate::config::file::sdk::{DeviceSdkStoreOptions, DeviceSdkVolatileOptions};
    use crate::config::{DEFAULT_GRPC_CONFIG_PORT, DEFAULT_HTTP_PORT};
    use crate::tests::with_settings;

    use super::*;

    #[rstest]
    #[case::credential_secret(Config {
        realm: Some("1".to_string()),
        device_id: Some("2".to_string()),
        pairing_url: Some("3".to_string()),
        credentials_secret: Some("4".to_string()),
        ..Default::default()
    })]
    #[case::pairing_token(Config {
        realm: Some("1".to_string()),
        device_id: Some("2".to_string()),
        pairing_url: Some("3".to_string()),
        pairing_token: Some("4".to_string()),
        ..Default::default()
    })]
    fn config_is_valid_options(#[case] config: Config) {
        config.validate().expect("config is valid");

        MessageHubOptions::try_from(config).unwrap();
    }

    #[rstest]
    #[case::empty_realm(Config {
        realm: Some("".to_string()),
        device_id: Some("2".to_string()),
        pairing_url: Some("3".to_string()),
        pairing_token: Some("4".to_string()),
        ..Default::default()
    })]
    #[case::empty_device_id(Config {
        realm: Some("1".to_string()),
        device_id: Some("".to_string()),
        pairing_url: Some("3".to_string()),
        pairing_token: Some("4".to_string()),
        ..Default::default()
    })]
    #[case::empty_pairing_url(Config {
        realm: Some("1".to_string()),
        device_id: Some("2".to_string()),
        pairing_url: Some("".to_string()),
        pairing_token: Some("4".to_string()),
        ..Default::default()
    })]
    #[case::empty_credential_secret(Config {
        realm: Some("1".to_string()),
        device_id: Some("2".to_string()),
        pairing_url: Some("3".to_string()),
        credentials_secret: Some("".to_string()),
        ..Default::default()
    })]
    #[case::empty_pairing_token(Config {
        realm: Some("1".to_string()),
        device_id: Some("2".to_string()),
        pairing_url: Some("3".to_string()),
        pairing_token: Some("".to_string()),
        ..Default::default()
    })]
    #[case::invalid_interface_dir(Config {
        realm: Some("1".to_string()),
        device_id: Some("2".to_string()),
        pairing_url: Some("3".to_string()),
        pairing_token: Some("4".to_string()),
        interfaces_directory: Some(PathBuf::from("")),
        ..Default::default()
    })]
    #[case::missing_cred_and_pairing(Config {
        realm: Some("1".to_string()),
        device_id: Some("2".to_string()),
        pairing_url: Some("3".to_string()),
        ..Default::default()
    })]
    fn config_is_invalid_options(#[case] config: Config) {
        assert!(config.validate().is_err());
        MessageHubOptions::try_from(config).unwrap_err();
    }

    #[tokio::test]
    async fn legacy_stored_credential() {
        let expected = "32".to_string();

        let dir = tempfile::TempDir::new().unwrap();
        tokio::fs::write(dir.path().join(LEGACY_CREDENTIAL_FILE), &expected)
            .await
            .unwrap();

        let mut opt = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            store_directory: Some(dir.path().to_path_buf()),
            ..Default::default()
        };

        let store_dir = StoreDir::create(dir.path().to_path_buf()).await.unwrap();

        opt.legacy_credential_secret(&store_dir).await.unwrap();

        assert_eq!(opt.credentials_secret.unwrap(), expected);
    }

    #[tokio::test]
    async fn load_toml_config() {
        let expected = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            pairing_token: Some("42".to_string()),
            ..Default::default()
        };

        let dir = tempfile::TempDir::new().unwrap();

        let path = dir.path().join(CONFIG_FILE_NAME);

        tokio::fs::write(&path, toml::to_string(&expected).unwrap())
            .await
            .unwrap();

        let options = Config::read(path).await.unwrap().unwrap();

        assert_eq!(options, expected);
    }

    #[tokio::test]
    async fn load_from_store_path() {
        let dir = tempfile::TempDir::new().unwrap();

        let expected = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            pairing_token: Some("42".to_string()),
            store_directory: Some(dir.path().to_path_buf()),
            ..Default::default()
        };

        let path = dir.path().join(CONFIG_FILE_NAME);

        tokio::fs::write(&path, toml::to_string(&expected).unwrap())
            .await
            .unwrap();

        let opt = Config::read(path).await.unwrap().unwrap();

        assert_eq!(opt, expected);
    }

    /// Make sure the example config is keep in sync with the code
    #[test]
    fn deserialize_example_config() {
        let config = include_str!("../../../examples/message-hub-config.toml");

        let config = toml::from_str::<Config>(config).unwrap();

        let expected = Config {
            realm: Some("example_realm".to_string()),
            device_id: Some("YOUR_UNIQUE_DEVICE_ID".to_string()),
            pairing_url: Some("https://api.astarte.EXAMPLE.COM/pairing".to_string()),
            pairing_token: Some("YOUR_PAIRING_TOKEN".to_string()),
            interfaces_directory: Some(PathBuf::from("/usr/share/message-hub/astarte-interfaces/")),
            grpc_socket_port: Some(50051),
            store_directory: Some(PathBuf::from("/var/lib/message-hub")),
            astarte: Some(DeviceSdkOptions {
                ignore_ssl: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(config, expected);
    }

    #[rstest]
    #[case(Config {
        realm: Some("1".to_string()),
        device_id: Some("2".to_string()),
        pairing_url: Some("3".to_string()),
        pairing_token: Some("4".to_string()),
        astarte_ignore_ssl: Some(true),
        grpc_socket_port: Some(5),
        astarte: Some(DeviceSdkOptions {
            ignore_ssl: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    })]
    #[case(Config {
        realm: Some("1".to_string()),
        device_id: Some("2".to_string()),
        pairing_url: Some("3".to_string()),
        credentials_secret: Some("4".to_string()),
        pairing_token: Some("5".to_string()),
        astarte_ignore_ssl: Some(true),
        grpc_socket_port: Some(6),
        astarte: Some(DeviceSdkOptions {
            ignore_ssl: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    })]
    #[case(Config {
        realm: Some("1".to_string()),
        device_id: Some("2".to_string()),
        pairing_url: Some("3".to_string()),
        credentials_secret: Some("4".to_string()),
        pairing_token: Some("5".to_string()),
        astarte_ignore_ssl: Some(true),
        grpc_socket_host: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        grpc_socket_port: Some(6),
        interfaces_directory: Some(PathBuf::from("/foo")),
        store_directory: Some(PathBuf::from("/bar")),
        astarte: Some(DeviceSdkOptions {
            ignore_ssl:Some(true),
            keep_alive_secs: Some(30),
            timeout_secs: Some(10),
            volatile:Some(DeviceSdkVolatileOptions{
                max_retention_items: Some(NonZero::<usize>::new(10).unwrap())
            }),
            store: Some(DeviceSdkStoreOptions{
                max_db_size: Some(Size::MiB(NonZero::<u64>::new(1000).unwrap())),
                max_db_journal_size: Some(Size::MiB(NonZero::<u64>::new(2000).unwrap())),
                max_retention_items: NonZero::<usize>::new(10),
            }),
        }),
        dynamic_config: Some(DynamicConfig {
            http: Some(Http{
                enabled: Some(false),
                host: Some(DEFAULT_HOST),
                port: Some(DEFAULT_HTTP_PORT),
            }),
            grpc: Some(Grpc{
                enabled: Some(true),
                host: Some(DEFAULT_HOST),
                port: Some(DEFAULT_GRPC_CONFIG_PORT),
            })
        })
    })]
    #[test]
    fn ser_and_de_config(#[context] ctx: Context, #[case] config: Config) {
        let ser = toml::to_string_pretty(&config).unwrap();

        let res: Config = toml::from_str(&ser).unwrap();

        assert_eq!(res, config);

        with_settings!({
            assert_toml_snapshot!(format!("{}", ctx.case.unwrap()), config);
        });
    }
}
