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
use eyre::{Context, Report};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::ReadDirStream;
use tracing::{debug, error, info};

use crate::error::ConfigError;

pub mod dynamic;
pub mod sdk;

use self::dynamic::DynamicConfig;
use self::sdk::DeviceSdkOptions;

use super::{DEFAULT_GRPC_PORT, DEFAULT_HOST, MessageHubOptions, Override};

/// Default configuration file name
pub const CONFIG_FILE_NAME: &str = "message-hub-config.toml";

/// Credential secret after the device is registered.
///
/// Legacy file, currently the pairing is handled directly from the [`astarte_device_sdk`].
const LEGACY_CREDENTIAL_FILE: &str = "credentials_secret";

const CONFIG_FILE: &str = "config.toml";
const CONFIG_D: &str = "config.d";

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

    /// Reads the configurations files.
    ///
    /// The order of the configurations files is as follows:
    ///
    /// 1. System default configuration file (e.g. /etc/message-hub/config.toml)
    /// 2. User default configuration file (e.g. $HOME/.config/message-hub/config.toml)
    /// 3. All the drop-ins configuration from various locations sorted alphabetical with the
    ///    following stable order:
    ///    1. System directory: /etc/message-hub/config.d/*.toml
    ///    1. User directory:  $HOME/.config/message-hub/config.d/*.toml
    ///    2. Storage directory: /var/lib/message-hub/config/*.toml
    ///
    pub async fn read_files(
        config_dir: Option<&Path>,
        store_dir: Option<&Path>,
    ) -> eyre::Result<Config> {
        let mut config = Config::default();
        let mut config_d = Vec::new();

        // Reads either the config dir or the directory
        if let Some(config_dir) = config_dir {
            info!(dir = %config_dir.display(), "using custom directory");

            config.read_config_dir(config_dir, &mut config_d).await?;
        } else {
            // TODO: window support
            #[cfg(unix)]
            {
                const SYSTEM_CONFIG: &str = "/etc/message-hub/";

                config
                    .read_config_dir(Path::new(SYSTEM_CONFIG), &mut config_d)
                    .await?;
            }

            if let Some(user_config_dir) = dirs::config_dir() {
                // $HOME/.config/message-hub
                let user_config_dir = user_config_dir.join("message-hub");

                config
                    .read_config_dir(&user_config_dir, &mut config_d)
                    .await?;
            }
        }

        if let Some(store_dir) = store_dir {
            info!("searching configs in storage directory");

            Self::append_config_d(&mut config_d, &store_dir.join("config")).await?;
        }

        Self::sort_config_d(&mut config_d);

        for file in config_d {
            config.read_and_merge(file).await?;
        }

        Ok(config)
    }

    // TODO: use lexicographical sorting
    fn sort_config_d(config_d: &mut [PathBuf]) {
        // Important to be a stable sort
        config_d.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    }

    /// Reads a config file and merges it into this one
    pub async fn read_and_merge(&mut self, path: impl AsRef<Path>) -> eyre::Result<()> {
        let path = path.as_ref();

        if let Some(config) = Self::read(path).await? {
            info!(path = %path.display(), "config red");

            self.merge(config);
        }

        Ok(())
    }

    /// Reads a config.toml and config.d files in the directory.
    async fn read_config_dir(
        &mut self,
        config_dir: &Path,
        config_d: &mut Vec<PathBuf>,
    ) -> eyre::Result<()> {
        self.read_and_merge(config_dir.join(CONFIG_FILE)).await?;

        Self::append_config_d(config_d, &config_dir.join(CONFIG_D)).await?;

        Ok(())
    }

    /// Appends the configuration files to the vector
    async fn append_config_d(config_d: &mut Vec<PathBuf>, path: &Path) -> eyre::Result<()> {
        let dir = match tokio::fs::read_dir(path).await {
            Ok(dir) => dir,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(eyre::Report::new(err).wrap_err("couldn't read config.d directory"));
            }
        };

        ReadDirStream::new(dir)
            .map_err(|error| {
                eyre::Report::new(error).wrap_err("couldn't read configuration directory")
            })
            .try_filter_map(|entry| async move {
                let path = entry.path();

                if path.extension().is_none_or(|ext| ext != "toml") {
                    return Ok(None);
                }

                let file_type = entry.file_type().await.wrap_err("couldn't get file type")?;

                if !file_type.is_file() {
                    return Ok(None);
                }

                Ok(Some(path))
            })
            .try_for_each(|p| {
                config_d.push(p);

                futures::future::ok(())
            })
            .await?;

        Ok(())
    }

    /// Obtains the credential secret from the stored path or by registering the device.
    pub async fn legacy_credential_secret(&mut self) -> eyre::Result<()> {
        if self
            .credentials_secret
            .as_ref()
            .is_some_and(|c| !c.is_empty())
        {
            debug!("credentials already set");

            return Ok(());
        }

        // Load the credential fiele for backwards compatibility
        let Some(store_dir) = &self.store_directory else {
            debug!("no storage dir set");

            return Ok(());
        };

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

        let store_directory = value
            .store_directory
            .unwrap_or_else(MessageHubOptions::default_store_directory);

        info!(
            store_directory = %store_directory.display(),
            "storage directory configured"
        );

        let mut astarte = value.astarte.unwrap_or_default();

        if let Some(astarte_ignore_ssl) = value.astarte_ignore_ssl {
            astarte.ignore_ssl = Some(astarte_ignore_ssl);
        }

        let introspection_cache = store_directory.join("interfaces");

        Ok(MessageHubOptions {
            realm,
            device_id,
            pairing_url,
            credential,
            interfaces_directory: value.interfaces_directory,
            grpc_socket_host,
            grpc_socket_port,
            store_directory,
            astarte,
            introspection_cache,
        })
    }
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use tempfile::TempDir;

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
    async fn list_config_files() -> eyre::Result<()> {
        let dir = TempDir::with_prefix("list_config_files")?;

        tokio::fs::write(dir.path().join("config.toml"), "").await?;

        let config_d = dir.path().join("config.d");
        tokio::fs::create_dir(&config_d).await?;

        tokio::fs::write(dir.path().join("config.toml"), "").await?;
        tokio::fs::write(config_d.join("10-low.toml"), "").await?;
        tokio::fs::write(config_d.join("50-medium.toml"), "").await?;
        tokio::fs::write(config_d.join("99-high.toml"), "").await?;

        let store = dir.path().join("storage");
        let storage_config = store.join("config");

        tokio::fs::create_dir_all(&storage_config).await?;

        tokio::fs::write(storage_config.join("60-dynamic.toml"), "").await?;

        let mut files = Vec::new();
        Config::append_config_d(&mut files, &config_d).await?;
        Config::append_config_d(&mut files, &storage_config).await?;
        Config::sort_config_d(&mut files);

        let exp = [
            "config.d/10-low.toml",
            "config.d/50-medium.toml",
            "storage/config/60-dynamic.toml",
            "config.d/99-high.toml",
        ]
        .map(|path| dir.path().join(path));

        assert_eq!(files, exp);

        Ok(())
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

        opt.legacy_credential_secret().await.unwrap();

        assert_eq!(opt.credentials_secret.unwrap(), expected);
    }

    // TODO: add cargo insta for snapshot testing for encode and decode
    // #[tokio::test]
    // async fn load_toml_config() {
    //     let expected = Config {
    //         realm: Some("1".to_string()),
    //         device_id: Some("2".to_string()),
    //         pairing_url: Some("3".to_string()),
    //         pairing_token: Some("42".to_string()),
    //         ..Default::default()
    //     };

    //     let dir = tempfile::TempDir::new().unwrap();

    //     let path = dir.path().join(CONFIG_FILES[0]);

    //     fs::write(&path, toml::to_string(&expected).unwrap())
    //         .await
    //         .unwrap();

    //     let path = Some(path);

    //     let options = Config::find_config(path.as_deref(), None)
    //         .await
    //         .unwrap()
    //         .unwrap();

    //     assert_eq!(options, expected);
    // }

    // #[tokio::test]
    // async fn load_from_store_path() {
    //     let dir = tempfile::TempDir::new().unwrap();

    //     #[allow(deprecated)]
    //     let expected = Config {
    //         realm: Some("1".to_string()),
    //         device_id: Some("2".to_string()),
    //         pairing_url: Some("3".to_string()),
    //         pairing_token: Some("42".to_string()),
    //         store_directory: Some(dir.path().to_path_buf()),
    //         ..Default::default()
    //     };

    //     let path = dir.path().join(CONFIG_FILE_NAME);

    //     fs::write(&path, toml::to_string(&expected).unwrap())
    //         .await
    //         .unwrap();

    //     let opt = Config::find_config(None, Some(dir.path()))
    //         .await
    //         .unwrap()
    //         .unwrap();

    //     assert_eq!(opt, expected);
    // }

    // /// Make sure the example config is keep in sync with the code
    // #[test]
    // fn deserialize_example_config() {
    //     let config = include_str!("../../examples/message-hub-config.toml");

    //     let opts = toml::from_str::<Config>(config);

    //     assert!(opts.is_ok(), "error deserializing config: {opts:?}");
    //     let opts = opts.unwrap();

    //     let expected = Config {
    //         realm: Some("example_realm".to_string()),
    //         device_id: Some("YOUR_UNIQUE_DEVICE_ID".to_string()),
    //         pairing_url: Some("https://api.astarte.EXAMPLE.COM".to_string()),
    //         pairing_token: Some("YOUR_PAIRING_TOKEN".to_string()),
    //         interfaces_directory: Some(PathBuf::from("/usr/share/message-hub/astarte-interfaces/")),
    //         grpc_socket_port: Some(50051),
    //         store_directory: Some(PathBuf::from("/var/lib/message-hub")),
    //         ..Default::default()
    //     };

    //     assert_eq!(opts, expected);
    // }

    // #[tokio::test]
    // async fn obtain_device_id_configured_none() {
    //     let expected = "mock-id".to_string();

    //     let dir = tempfile::TempDir::new().unwrap();
    //     fs::write(dir.path().join(CREDENTIAL_FILE), &expected)
    //         .await
    //         .unwrap();

    //     let mut opt = Config {
    //         realm: Some("1".to_string()),
    //         pairing_url: Some("3".to_string()),
    //         store_directory: Some(dir.path().to_path_buf()),
    //         ..Default::default()
    //     };

    //     opt.device_id_from_hardware_id().await.unwrap();

    //     assert_eq!(opt.device_id.unwrap(), expected);
    // }

    // #[tokio::test]
    // async fn obtain_device_id_configured_some_but_empty() {
    //     let expected = "mock-id".to_string();

    //     let dir = tempfile::TempDir::new().unwrap();
    //     fs::write(dir.path().join(CREDENTIAL_FILE), &expected)
    //         .await
    //         .unwrap();

    //     #[allow(deprecated)]
    //     let mut opt = Config {
    //         realm: Some("1".to_string()),
    //         device_id: Some("".to_string()),
    //         pairing_url: Some("3".to_string()),
    //         store_directory: Some(dir.path().to_path_buf()),
    //         ..Default::default()
    //     };

    //     let device_id = opt.device_id_from_hardware_id().await;

    //     assert!(
    //         device_id.is_ok(),
    //         "error obtaining stored credential {device_id:?}"
    //     );
    //     assert_eq!(opt.device_id.unwrap(), expected);
    // }

    // #[tokio::test]
    // async fn test_read_options_from_toml_cred_secred_ok() {
    //     const TOML_FILE: &str = r#"
    //         realm = "1"
    //         device_id = "2"
    //         pairing_url = "3"
    //         credentials_secret = "4"
    //         astarte_ignore_ssl = false
    //         grpc_socket_port = 5

    //         [astarte]
    //         ignore_ssl = false
    //     "#;

    //     let res = toml::from_str::<Config>(TOML_FILE);
    //     let config = res.expect("Parsing of TOML file failed");

    //     let exp = Config {
    //         realm: Some("1".to_string()),
    //         device_id: Some("2".to_string()),
    //         pairing_url: Some("3".to_string()),
    //         credentials_secret: Some("4".to_string()),
    //         astarte_ignore_ssl: Some(false),
    //         grpc_socket_port: Some(5),
    //         ..Default::default()
    //     };

    //     assert_eq!(config, exp)
    // }

    // #[test]
    // fn test_read_options_from_toml_pairing_token_ok() {
    //     const TOML_FILE: &str = r#"
    //         realm = "1"
    //         device_id = "2"
    //         pairing_url = "3"
    //         pairing_token = "4"
    //         astarte_ignore_ssl = true
    //         grpc_socket_port = 5
    //         [astarte]
    //         ignore_ssl = true
    //     "#;

    //     let res = toml::from_str::<Config>(TOML_FILE);
    //     let options = res.expect("Parsing of TOML file failed");
    //     assert_eq!(options.realm.unwrap(), "1");
    //     assert_eq!(options.device_id, Some("2".to_string()));
    //     assert_eq!(options.pairing_url.unwrap(), "3");
    //     assert_eq!(options.credentials_secret, None);
    //     assert_eq!(options.pairing_token, Some("4".to_string()));
    //     #[allow(deprecated)]
    //     let ignore_ssl = options.astarte_ignore_ssl;
    //     assert!(ignore_ssl);
    //     assert!(options.astarte.volatile.max_retention_items.is_none());
    //     assert!(options.astarte.store.max_db_size.is_none());
    //     assert!(options.astarte.store.max_db_journal_size.is_none());
    //     assert!(options.astarte.store.max_retention_items.is_none());
    //     assert_eq!(options.grpc_socket_port, Some(5));
    // }

    // #[test]
    // fn test_read_options_from_toml_both_pairing_and_cred_sec_ok() {
    //     const TOML_FILE: &str = r#"
    //         realm = "1"
    //         device_id = "2"
    //         pairing_url = "3"
    //         credentials_secret = "4"
    //         pairing_token = "5"
    //         astarte_ignore_ssl = true
    //         grpc_socket_port = 6
    //         [astarte]
    //         ignore_ssl = true
    //     "#;

    //     let res = toml::from_str::<Config>(TOML_FILE);
    //     let options = res.expect("Parsing of TOML file failed");
    //     assert_eq!(options.realm.unwrap(), "1");
    //     assert_eq!(options.device_id.unwrap(), "2");
    //     assert_eq!(options.pairing_url.unwrap(), "3");
    //     assert_eq!(options.credentials_secret, Some("4".to_string()));
    //     assert_eq!(options.pairing_token, Some("5".to_string()));
    //     #[allow(deprecated)]
    //     let ignore_ssl = options.astarte_ignore_ssl;
    //     assert!(ignore_ssl);
    //     assert!(options.astarte.ignore_ssl);
    //     assert!(options.astarte.volatile.max_retention_items.is_none());
    //     assert!(options.astarte.store.max_db_size.is_none());
    //     assert!(options.astarte.store.max_db_journal_size.is_none());
    //     assert!(options.astarte.store.max_retention_items.is_none());
    //     assert_eq!(options.grpc_socket_port, Some(6));
    // }

    // #[test]
    // fn test_read_options_from_toml_missing_pairing_and_cred_sec_err() {
    //     const TOML_FILE: &str = r#"
    //         realm = "1"
    //         device_id = "2"
    //         pairing_url = "3"
    //         astarte_ignore_ssl = true
    //         grpc_socket_port = 4
    //         [astarte]
    //         ignore_ssl = true
    //     "#;

    //     let config = toml::from_str::<Config>(TOML_FILE).unwrap();
    //     assert!(config.validate().is_err());
    //     MessageHubOptions::try_from(config).unwrap_err();
    // }

    // #[test]
    // fn test_read_options_from_toml_missing_realm_err() {
    //     const TOML_FILE: &str = r#"
    //         device_id = "1"
    //         pairing_url = "2"
    //         credentials_secret = "3"
    //         pairing_token = "4"
    //         astarte_ignore_ssl = true
    //         grpc_socket_port = 5
    //         [astarte]
    //         ignore_ssl = true
    //     "#;

    //     let config = toml::from_str::<Config>(TOML_FILE).unwrap();
    //     assert!(config.validate().is_err());
    //     MessageHubOptions::try_from(config).unwrap_err();
    // }

    // #[test]
    // fn test_read_options_from_toml_both_ok_with_size() {
    //     const TOML_FILE: &str = r#"
    //         realm = "1"
    //         device_id = "2"
    //         pairing_url = "3"
    //         credentials_secret = "4"
    //         pairing_token = "5"
    //         astarte_ignore_ssl = true
    //         grpc_socket_port = 6
    //         [astarte]
    //         ignore_ssl = true
    //         [astarte.volatile]
    //         max_retention_items = 10
    //         [astarte.store]
    //         max_db_size = { value = 5, unit = "mb" }
    //         max_retention_items = 10
    //     "#;

    //     let res = toml::from_str::<Config>(TOML_FILE);
    //     let options = res.expect("Parsing of TOML file failed");
    //     assert_eq!(options.realm.unwrap(), "1");
    //     assert_eq!(options.device_id.unwrap(), "2");
    //     assert_eq!(options.pairing_url.unwrap(), "3");
    //     assert_eq!(options.credentials_secret, Some("4".to_string()));
    //     assert_eq!(options.pairing_token, Some("5".to_string()));
    //     #[allow(deprecated)]
    //     let ignore_ssl = options.astarte_ignore_ssl;
    //     assert!(ignore_ssl);
    //     assert!(options.astarte.ignore_ssl);
    //     assert_eq!(options.grpc_socket_port, Some(6));
    //     assert_eq!(
    //         options.astarte.volatile.max_retention_items,
    //         Some(10.try_into().unwrap())
    //     );
    //     assert!(options.astarte.store.max_db_journal_size.is_none());
    //     assert_eq!(
    //         options.astarte.store.max_db_size,
    //         Some(Size::Mb(5.try_into().unwrap()))
    //     );
    //     assert_eq!(
    //         options.astarte.store.max_retention_items,
    //         Some(10.try_into().unwrap())
    //     )
    // }
}
