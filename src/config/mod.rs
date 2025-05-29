/*
 * This file is part of Astarte.
 *
 * Copyright 2022 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
//! Helper module to retrieve the configuration of the Astarte message hub.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use astarte_device_sdk::transport::mqtt::Credential;
use log::debug;
use serde::{Deserialize, Serialize};
use tokio::{fs, sync::Notify};

use crate::config::http::HttpConfigProvider;
use crate::config::protobuf::ProtobufConfigProvider;
use crate::error::{AstarteMessageHubError, ConfigError};

pub mod http;
pub mod protobuf;

const CREDENTIAL_FILE: &str = "credentials_secret";

/// Default host to bind the HTTP/gRPC server to.
pub const DEFAULT_HOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
/// Default port to bind the gRPC server to.
pub const DEFAULT_GRPC_PORT: u16 = 50051;
/// Default port to bind for the dynamic configuration over HTTP.
pub const DEFAULT_HTTP_PORT: u16 = 40041;

/// Default configuration file name
pub const CONFIG_FILE_NAME: &str = "message-hub-config.toml";

/// Locations for the configuration files
pub const CONFIG_FILES: [&str; 2] = [CONFIG_FILE_NAME, "/etc/message-hub/config.toml"];

/// Struct to deserialize the configuration options for the Astarte message hub.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Config {
    /// The Astarte realm the device belongs to.
    pub realm: Option<String>,
    /// A unique ID for the device.
    pub device_id: Option<String>,
    /// The URL of the Astarte pairing API.
    pub pairing_url: Option<String>,
    /// The credentials secret used to authenticate with Astarte.
    pub credentials_secret: Option<String>,
    /// Token used to register the device.
    pub pairing_token: Option<String>,
    /// Directory containing the Astarte interfaces.
    pub interfaces_directory: Option<PathBuf>,
    /// Whether to ignore SSL errors when connecting to Astarte.
    #[deprecated(
        since = "0.5.3",
        note = "Use the astarte map 'ignore_ssl' to configure the SDK"
    )]
    #[serde(default)]
    pub astarte_ignore_ssl: bool,
    /// The gRPC host to use bind.
    pub grpc_socket_host: Option<IpAddr>,
    /// The gRPC port to use.
    pub grpc_socket_port: Option<u16>,
    /// Directory used by Astarte-Message-Hub to retain configuration and other persistent data.
    pub store_directory: Option<PathBuf>,
    /// Astarte device SDK options.
    #[serde(skip_serializing_if = "DeviceSdkOptions::is_default", default)]
    pub astarte: DeviceSdkOptions,
}

impl Config {
    /// Searches for the configuration file in the store directory or [default locations](CONFIG_FILES).
    pub async fn find_config(
        custom_config: Option<&Path>,
        store_path: Option<&Path>,
    ) -> Result<Option<Self>, ConfigError> {
        if let Some(config) = custom_config {
            let file = Self::read_from_file(config).await?;

            if file.is_some() {
                return Ok(file);
            }
        }

        if let Some(store_path) = store_path {
            let config = store_path.join(CONFIG_FILE_NAME);

            let file = Self::read_from_file(config).await?;

            if file.is_some() {
                return Ok(file);
            }
        }

        for path in CONFIG_FILES {
            let file = Self::read_from_file(path).await?;

            if file.is_some() {
                return Ok(file);
            }
        }

        Ok(None)
    }

    /// Read the configuration from a Toml file.
    pub async fn read_from_file(path: impl AsRef<Path>) -> Result<Option<Self>, ConfigError> {
        let path = path.as_ref();

        if !path.exists() {
            return Ok(None);
        }

        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(ConfigError::File)?;

        let config = toml::from_str::<Self>(&content).map_err(ConfigError::Toml)?;

        Ok(Some(config))
    }

    /// Validate the values are present for the server configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.realm.as_ref().is_some_and(|realm| !realm.is_empty()) {
            return Err(ConfigError::MissingField("realm"));
        }

        if !self
            .device_id
            .as_ref()
            .is_some_and(|device_id| !device_id.is_empty())
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

        if !self
            .pairing_url
            .as_ref()
            .is_some_and(|pairing_url| !pairing_url.is_empty())
        {
            return Err(ConfigError::MissingField("pairing_url"));
        }

        if self.store_directory.is_none() {
            return Err(ConfigError::MissingField("store_directory"));
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

    /// Gets the device id from the Hardware Id using DBUS.
    #[cfg(not(test))]
    pub async fn device_id_from_hardware_id(&mut self) -> Result<(), AstarteMessageHubError> {
        use crate::device::DeviceProxy;

        let connection = zbus::Connection::system().await?;
        let proxy = DeviceProxy::new(&connection).await?;
        let device_id: String = proxy.get_hardware_id("").await?;
        if device_id.is_empty() {
            return Err(AstarteMessageHubError::Fatal(
                "No hardware id provided".to_string(),
            ));
        }

        self.device_id = Some(device_id);

        Ok(())
    }

    /// Gets the device id from the Hardware Id using DBUS.
    #[cfg(test)]
    pub async fn device_id_from_hardware_id(&mut self) -> Result<(), AstarteMessageHubError> {
        use log::info;

        info!("retrieve mock-id");

        self.device_id = Some("mock-id".to_string());

        Ok(())
    }

    /// Obtains the credential secret from the stored path or by registering the device.
    pub async fn read_credential_secret(&mut self) -> Result<(), AstarteMessageHubError> {
        if self
            .credentials_secret
            .as_ref()
            .is_some_and(|c| !c.is_empty())
        {
            debug!("credentials already set");

            return Ok(());
        }

        // Load the credential fiele for backwards compatibility
        let path = self
            .store_directory
            .as_ref()
            .ok_or(ConfigError::MissingField("store_directory"))?
            .join(CREDENTIAL_FILE);

        match fs::read_to_string(&path).await {
            Ok(cred) => {
                debug!("using stored credentials from {:?}", path);

                self.credentials_secret = Some(cred);
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                debug!("credentials not found");
            }
            Err(err) => {
                return Err(AstarteMessageHubError::Fatal(format!(
                    "failed to read {}: {}",
                    path.to_string_lossy(),
                    err
                )))
            }
        };

        Ok(())
    }

    /// Function that get the configurations needed by the Message Hub.
    /// The configuration file is first retrieved from one of two default base locations.
    /// If no valid configuration file is found in either of these locations, or if the content
    /// of the first found file is not valid HTTP and Protobuf APIs are exposed to provide a valid
    /// configuration.
    pub async fn listen_dynamic_config(
        store_directory: &Path,
        http: SocketAddr,
        grpc: SocketAddr,
    ) -> Result<Config, AstarteMessageHubError> {
        let notify_config = Arc::new(Notify::new());

        let config_file = store_directory.join(CONFIG_FILE_NAME);

        let web_server =
            HttpConfigProvider::serve(&http, Arc::clone(&notify_config), &config_file).await?;

        let protobuf_server = ProtobufConfigProvider::new(
            grpc,
            Arc::clone(&notify_config),
            config_file.clone(),
            store_directory.to_path_buf(),
        )
        .await?;

        notify_config.notified().await;

        web_server.stop().await?;
        protobuf_server.stop().await?;

        let Some(config) = Self::read_from_file(&config_file).await? else {
            return Err(AstarteMessageHubError::Config(ConfigError::Dynamic(
                config_file,
            )));
        };

        Ok(config)
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

        let mut astarte = value.astarte;

        #[allow(deprecated)]
        {
            astarte.ignore_ssl |= value.astarte_ignore_ssl;
        }

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
        })
    }
}

/// Struct containing all the configuration options for the Astarte message hub.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct MessageHubOptions {
    /// The Astarte realm the device belongs to.
    pub realm: String,
    /// A unique ID for the device.
    pub device_id: String,
    /// The URL of the Astarte pairing API.
    pub pairing_url: String,
    /// The credentials secret used to authenticate with Astarte.
    pub credential: Credential,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Directory containing the Astarte interfaces.
    pub interfaces_directory: Option<PathBuf>,
    /// The gRPC host to use bind.
    pub grpc_socket_host: IpAddr,
    /// The gRPC port to use.
    pub grpc_socket_port: u16,
    /// Directory used by Astarte-Message-Hub to retain configuration and other persistent data.
    pub store_directory: PathBuf,
    /// Astarte device SDK options.
    #[serde(skip_serializing_if = "DeviceSdkOptions::is_default", default)]
    pub astarte: DeviceSdkOptions,
}

impl MessageHubOptions {
    /// Default the store directory to the current working directory.
    fn default_store_directory() -> PathBuf {
        PathBuf::from(".")
    }
}

/// Options to pass to the [Astarte device SDK](`astarte_device_sdk`).
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename = "astarte")]
pub struct DeviceSdkOptions {
    /// Keep alive interval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_alive_secs: Option<u64>,
    /// Connection timeout.
    ///
    /// Should be less than the keep alive interval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    /// Whether to ignore SSL errors when connecting to Astarte.
    #[serde(default)]
    pub ignore_ssl: bool,
}

impl DeviceSdkOptions {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_is_valid_cred_sec_ok() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: Some("4".to_string()),
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(5),
            store_directory: Some(MessageHubOptions::default_store_directory()),
            astarte: DeviceSdkOptions::default(),
        };

        let res = expected_msg_hub_opts.validate();
        assert!(res.is_ok(), "{}", res.unwrap_err());

        MessageHubOptions::try_from(expected_msg_hub_opts).unwrap();
    }

    #[test]
    fn test_is_valid_pairing_token_ok() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(5),
            store_directory: Some(MessageHubOptions::default_store_directory()),
            astarte: DeviceSdkOptions::default(),
        };

        let res = expected_msg_hub_opts.validate();
        assert!(res.is_ok(), "{res:?}");
        MessageHubOptions::try_from(expected_msg_hub_opts).unwrap();
    }

    #[test]
    fn test_is_valid_empty_realm_err() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = Config {
            realm: Some("".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(5),
            store_directory: Some(MessageHubOptions::default_store_directory()),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_err());
        MessageHubOptions::try_from(expected_msg_hub_opts).unwrap_err();
    }

    #[test]
    fn test_is_valid_empty_device_id() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = Config {
            realm: Some("1".to_string()),
            device_id: Some("".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(5),
            store_directory: Some(MessageHubOptions::default_store_directory()),
            astarte: DeviceSdkOptions::default(),
        };
        let res = expected_msg_hub_opts.validate();
        assert!(res.is_err());
        MessageHubOptions::try_from(expected_msg_hub_opts).unwrap_err();
    }

    #[test]
    fn test_is_valid_empty_pairing_url_err() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("".to_string()),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(5),
            store_directory: Some(MessageHubOptions::default_store_directory()),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_err());
        MessageHubOptions::try_from(expected_msg_hub_opts).unwrap_err();
    }

    #[test]
    fn test_is_valid_empty_credentials_secred_err() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: Some("".to_string()),
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(5),
            store_directory: Some(MessageHubOptions::default_store_directory()),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_err());
        MessageHubOptions::try_from(expected_msg_hub_opts).unwrap_err();
    }

    #[test]
    fn test_is_valid_empty_pairing_token_err() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: None,
            pairing_token: Some("".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(5),
            store_directory: Some(MessageHubOptions::default_store_directory()),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_err());
        MessageHubOptions::try_from(expected_msg_hub_opts).unwrap_err();
    }

    #[test]
    fn test_is_valid_invalid_interf_dir_err() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: Some(PathBuf::from("")),
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(5),
            store_directory: Some(MessageHubOptions::default_store_directory()),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_err());
        MessageHubOptions::try_from(expected_msg_hub_opts).unwrap_err();
    }

    #[test]
    fn test_is_valid_missing_credentials_secret_and_pairing_token_err() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: None,
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(655),
            store_directory: Some(MessageHubOptions::default_store_directory()),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_err());
        MessageHubOptions::try_from(expected_msg_hub_opts).unwrap_err();
    }

    #[tokio::test]
    async fn obtain_stored_credential() {
        let expected = "32".to_string();

        let dir = tempfile::TempDir::new().unwrap();
        fs::write(dir.path().join(CREDENTIAL_FILE), &expected)
            .await
            .unwrap();

        #[allow(deprecated)]
        let mut opt = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: None,
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(655),
            store_directory: Some(dir.path().to_path_buf()),
            astarte: DeviceSdkOptions::default(),
        };

        opt.read_credential_secret().await.unwrap();

        assert_eq!(opt.credentials_secret.unwrap(), expected,);
    }

    #[tokio::test]
    async fn obtain_credential_paring_token() {
        let dir = tempfile::TempDir::new().unwrap();

        #[allow(deprecated)]
        let mut opt = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: None,
            pairing_token: Some("42".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(655),
            store_directory: Some(dir.path().to_path_buf()),
            astarte: DeviceSdkOptions::default(),
        };

        let res = opt.read_credential_secret().await;

        assert!(
            res.is_ok(),
            "error obtaining stored credential {}",
            res.unwrap_err()
        );

        assert_eq!(opt.pairing_token.unwrap(), "42");
    }

    #[tokio::test]
    async fn load_toml_config() {
        #[allow(deprecated)]
        let expected = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: None,
            pairing_token: Some("42".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(655),
            store_directory: Some(MessageHubOptions::default_store_directory()),
            astarte: DeviceSdkOptions::default(),
        };

        let dir = tempfile::TempDir::new().unwrap();

        let path = dir.path().join(CONFIG_FILES[0]);

        fs::write(&path, toml::to_string(&expected).unwrap())
            .await
            .unwrap();

        let path = Some(path);

        let options = Config::find_config(path.as_deref(), None)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(options, expected);
    }

    #[tokio::test]
    async fn load_from_store_path() {
        let dir = tempfile::TempDir::new().unwrap();

        #[allow(deprecated)]
        let expected = Config {
            realm: Some("1".to_string()),
            device_id: Some("2".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: None,
            pairing_token: Some("42".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(655),
            store_directory: Some(dir.path().to_path_buf()),
            astarte: DeviceSdkOptions::default(),
        };

        let path = dir.path().join(CONFIG_FILE_NAME);

        fs::write(&path, toml::to_string(&expected).unwrap())
            .await
            .unwrap();

        let opt = Config::find_config(None, Some(dir.path()))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(opt, expected);
    }

    /// Make sure the example config is keep in sync with the code
    #[test]
    fn deserialize_example_config() {
        let config = include_str!("../../examples/message-hub-config.toml");

        let opts = toml::from_str::<Config>(config);

        assert!(opts.is_ok(), "error deserializing config: {opts:?}");
        let opts = opts.unwrap();

        #[allow(deprecated)]
        let expected = Config {
            realm: Some("example_realm".to_string()),
            device_id: Some("YOUR_UNIQUE_DEVICE_ID".to_string()),
            credentials_secret: None,
            pairing_url: Some("https://api.astarte.EXAMPLE.COM".to_string()),
            pairing_token: Some("YOUR_PAIRING_TOKEN".to_string()),
            interfaces_directory: Some(PathBuf::from("/usr/share/message-hub/astarte-interfaces/")),
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(50051),
            store_directory: Some(PathBuf::from("/var/lib/message-hub")),
            astarte: DeviceSdkOptions::default(),
        };

        assert_ne!(opts, expected);
    }

    #[tokio::test]
    async fn obtain_device_id_configured_none() {
        let expected = "mock-id".to_string();

        let dir = tempfile::TempDir::new().unwrap();
        fs::write(dir.path().join(CREDENTIAL_FILE), &expected)
            .await
            .unwrap();

        #[allow(deprecated)]
        let mut opt = Config {
            realm: Some("1".to_string()),
            device_id: None,
            pairing_url: Some("3".to_string()),
            credentials_secret: None,
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(655),
            store_directory: Some(dir.path().to_path_buf()),
            astarte: DeviceSdkOptions::default(),
        };

        opt.device_id_from_hardware_id().await.unwrap();

        assert_eq!(opt.device_id.unwrap(), expected);
    }

    #[tokio::test]
    async fn obtain_device_id_configured_some_but_empty() {
        let expected = "mock-id".to_string();

        let dir = tempfile::TempDir::new().unwrap();
        fs::write(dir.path().join(CREDENTIAL_FILE), &expected)
            .await
            .unwrap();

        #[allow(deprecated)]
        let mut opt = Config {
            realm: Some("1".to_string()),
            device_id: Some("".to_string()),
            pairing_url: Some("3".to_string()),
            credentials_secret: None,
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_host: None,
            grpc_socket_port: Some(655),
            store_directory: Some(dir.path().to_path_buf()),
            astarte: DeviceSdkOptions::default(),
        };

        let device_id = opt.device_id_from_hardware_id().await;

        assert!(
            device_id.is_ok(),
            "error obtaining stored credential {device_id:?}",
        );
        assert_eq!(opt.device_id.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_read_options_from_toml_cred_secred_ok() {
        const TOML_FILE: &str = r#"
            realm = "1"
            device_id = "2"
            pairing_url = "3"
            credentials_secret = "4"
            astarte_ignore_ssl = false
            grpc_socket_port = 5

            [astarte]
            ignore_ssl = false
        "#;

        let res = toml::from_str::<Config>(TOML_FILE);
        let options = res.expect("Parsing of TOML file failed");
        assert_eq!(options.realm.unwrap(), "1");
        assert_eq!(options.device_id, Some("2".to_string()));
        assert_eq!(options.pairing_url.unwrap(), "3");
        assert_eq!(options.credentials_secret, Some("4".to_string()));
        assert_eq!(options.pairing_token, None);
        #[allow(deprecated)]
        let ignore_ssl = !options.astarte_ignore_ssl;
        assert!(ignore_ssl);
        assert!(!options.astarte.ignore_ssl);
        assert_eq!(options.grpc_socket_port, Some(5));
    }

    #[test]
    fn test_read_options_from_toml_pairing_token_ok() {
        const TOML_FILE: &str = r#"
            realm = "1"
            device_id = "2"
            pairing_url = "3"
            pairing_token = "4"
            astarte_ignore_ssl = true
            grpc_socket_port = 5
            [astarte]
            ignore_ssl = true
        "#;

        let res = toml::from_str::<Config>(TOML_FILE);
        let options = res.expect("Parsing of TOML file failed");
        assert_eq!(options.realm.unwrap(), "1");
        assert_eq!(options.device_id, Some("2".to_string()));
        assert_eq!(options.pairing_url.unwrap(), "3");
        assert_eq!(options.credentials_secret, None);
        assert_eq!(options.pairing_token, Some("4".to_string()));
        #[allow(deprecated)]
        let ignore_ssl = options.astarte_ignore_ssl;
        assert!(ignore_ssl);
        assert!(options.astarte.ignore_ssl);
        assert_eq!(options.grpc_socket_port, Some(5));
    }

    #[test]
    fn test_read_options_from_toml_both_pairing_and_cred_sec_ok() {
        const TOML_FILE: &str = r#"
            realm = "1"
            device_id = "2"
            pairing_url = "3"
            credentials_secret = "4"
            pairing_token = "5"
            astarte_ignore_ssl = true
            grpc_socket_port = 6
            [astarte]
            ignore_ssl = true
        "#;

        let res = toml::from_str::<Config>(TOML_FILE);
        let options = res.expect("Parsing of TOML file failed");
        assert_eq!(options.realm.unwrap(), "1");
        assert_eq!(options.device_id.unwrap(), "2");
        assert_eq!(options.pairing_url.unwrap(), "3");
        assert_eq!(options.credentials_secret, Some("4".to_string()));
        assert_eq!(options.pairing_token, Some("5".to_string()));
        #[allow(deprecated)]
        let ignore_ssl = options.astarte_ignore_ssl;
        assert!(ignore_ssl);
        assert!(options.astarte.ignore_ssl);
        assert_eq!(options.grpc_socket_port, Some(6));
    }

    #[test]
    fn test_read_options_from_toml_missing_pairing_and_cred_sec_err() {
        const TOML_FILE: &str = r#"
            realm = "1"
            device_id = "2"
            pairing_url = "3"
            astarte_ignore_ssl = true
            grpc_socket_port = 4
            [astarte]
            ignore_ssl = true
        "#;

        let config = toml::from_str::<Config>(TOML_FILE).unwrap();
        assert!(config.validate().is_err());
        MessageHubOptions::try_from(config).unwrap_err();
    }

    #[test]
    fn test_read_options_from_toml_missing_realm_err() {
        const TOML_FILE: &str = r#"
            device_id = "1"
            pairing_url = "2"
            credentials_secret = "3"
            pairing_token = "4"
            astarte_ignore_ssl = true
            grpc_socket_port = 5
            [astarte]
            ignore_ssl = true
        "#;

        let config = toml::from_str::<Config>(TOML_FILE).unwrap();
        assert!(config.validate().is_err());
        MessageHubOptions::try_from(config).unwrap_err();
    }
}
