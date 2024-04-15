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
use std::path::{Path, PathBuf};
use std::sync::Arc;

use astarte_device_sdk::transport::mqtt::Credential;
use log::debug;
use serde::{Deserialize, Serialize};
use tokio::{fs, sync::Notify};

use crate::config::http::HttpConfigProvider;
use crate::config::protobuf::ProtobufConfigProvider;
use crate::error::{AstarteMessageHubError, ConfigValidationError};

use self::file::CONFIG_FILE_NAMES;

pub mod file;
pub mod http;
pub mod protobuf;

const CREDENTIAL_FILE: &str = "credentials_secret";

/// A macro to simplify the creation of a `Result` with an `AstarteMessageHubError` error type.
macro_rules! ensure {
    ($cond:expr, $err:expr) => {
        if !($cond) {
            return Err($err);
        }
    };
}

/// Struct containing all the configuration options for the Astarte message hub.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct MessageHubOptions {
    /// The Astarte realm the device belongs to.
    pub realm: String,
    /// A unique ID for the device.
    pub device_id: Option<String>,
    /// The credentials secret used to authenticate with Astarte.
    pub credentials_secret: Option<String>,
    /// The URL of the Astarte pairing API.
    pub pairing_url: String,
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
    /// The gRPC port to use.
    pub grpc_socket_port: u16,
    /// Directory used by Astarte-Message-Hub to retain configuration and other persistent data.
    #[serde(default = "MessageHubOptions::default_store_directory")]
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

    /// Function that get the configurations needed by the Message Hub.
    /// The configuration file is first retrieved from one of two default base locations.
    /// If no valid configuration file is found in either of these locations, or if the content
    /// of the first found file is not valid HTTP and Protobuf APIs are exposed to provide a valid
    /// configuration.
    pub async fn get(
        toml_file: Option<String>,
        store_directory: Option<&Path>,
    ) -> Result<MessageHubOptions, AstarteMessageHubError> {
        let mut opt = if let Some(toml_file) = toml_file {
            let toml_str = tokio::fs::read_to_string(toml_file).await?;
            file::get_options_from_toml(&toml_str)
        } else if let Some(store_directory) = store_directory {
            if !store_directory.is_dir() {
                let err_msg = "Provided store directory for HTTP and ProtoBuf does not exists.";
                return Err(AstarteMessageHubError::FatalError(err_msg.to_string()));
            }
            let configuration_file = store_directory.join(CONFIG_FILE_NAMES[0]);
            if !configuration_file.exists() {
                let notify_config = Arc::new(Notify::new());

                let web_server = HttpConfigProvider::serve(
                    "127.0.0.1:40041",
                    Arc::clone(&notify_config),
                    configuration_file.to_str().unwrap(),
                )
                .await?;

                let protobuf_server = ProtobufConfigProvider::new(
                    "[::1]:50051",
                    Arc::clone(&notify_config),
                    configuration_file.to_str().unwrap(),
                )
                .await;

                notify_config.notified().await;

                web_server.stop().await?;
                protobuf_server.stop().await;
            }
            let toml_str = std::fs::read_to_string(configuration_file)?;

            file::get_options_from_toml(&toml_str)
        } else {
            file::get_options_from_base_toml()
        }?;

        if let Some(store_directory) = store_directory {
            opt.store_directory = store_directory.to_path_buf();
        }

        Ok(opt)
    }

    /// Validates the configuration and return the reason if it is not valid.
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        ensure!(
            !self.realm.is_empty(),
            ConfigValidationError::MissingField("realm")
        );

        ensure!(
            !self.pairing_url.is_empty(),
            ConfigValidationError::MissingField("pairing_url")
        );

        let valid_secret = self
            .credentials_secret
            .as_ref()
            .map(|e| !e.is_empty())
            .unwrap_or(false);

        let valid_token = self
            .pairing_token
            .as_ref()
            .map(|e| !e.is_empty())
            .unwrap_or(false);

        ensure!(
            valid_secret || valid_token,
            ConfigValidationError::MissingPairingAndCredentials
        );

        let valid_interface_dir = self
            .interfaces_directory
            .as_ref()
            .map(|dir| dir.is_dir())
            // If no interface directory is specified, it's valid.
            .unwrap_or(true);

        ensure!(
            valid_interface_dir,
            ConfigValidationError::InvalidInterfaceDirectory(self.interfaces_directory.clone())
        );

        Ok(())
    }

    /// Obtains the credential secret from the stored path or by registering the device.
    pub async fn obtain_credential(&self) -> Result<Credential, AstarteMessageHubError> {
        if let Some(ref cred_sec) = self.credentials_secret {
            return Ok(Credential::secret(cred_sec));
        }

        // Load the credential fiele for backwards compatibility
        let path = self.store_directory.join(CREDENTIAL_FILE);

        match fs::read_to_string(&path).await {
            Ok(cred) => {
                debug!("using stored credentials from {:?}", path);

                return Ok(Credential::secret(cred));
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                debug!("credentials not found");
            }
            Err(err) => {
                return Err(AstarteMessageHubError::FatalError(format!(
                    "failed to read {}: {}",
                    path.to_string_lossy(),
                    err
                )))
            }
        };

        let token = self.pairing_token.as_ref().ok_or_else(|| {
            AstarteMessageHubError::FatalError("missing pairing token".to_string())
        })?;

        Ok(Credential::paring_token(token))
    }

    /// Obtains the device id from option or by zbus service.
    pub async fn obtain_device_id(&mut self) -> Result<&str, AstarteMessageHubError> {
        if self
            .device_id
            .as_ref()
            .filter(|device_id| !device_id.is_empty())
            .is_none()
        {
            let device_id = self.get_hardware_id_from_dbus().await?;
            self.device_id = Some(device_id);
        }

        Ok(self.device_id.as_ref().unwrap())
    }

    #[cfg(not(test))]
    async fn get_hardware_id_from_dbus(&self) -> Result<String, AstarteMessageHubError> {
        use crate::device::DeviceProxy;

        let connection = zbus::Connection::system().await?;
        let proxy = DeviceProxy::new(&connection).await?;
        let device_id: String = proxy.get_hardware_id("").await?;
        if device_id.is_empty() {
            return Err(AstarteMessageHubError::FatalError(
                "No hardware id provided".to_string(),
            ));
        }
        Ok(device_id)
    }

    #[cfg(test)]
    async fn get_hardware_id_from_dbus(&self) -> Result<String, AstarteMessageHubError> {
        use log::info;

        info!("retrieve mock-id");
        Ok("mock-id".to_string())
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
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: Some("4".to_string()),
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
            store_directory: MessageHubOptions::default_store_directory(),
            astarte: DeviceSdkOptions::default(),
        };

        let res = expected_msg_hub_opts.validate();
        assert!(res.is_ok(), "{:?}", res);
    }

    #[test]
    fn test_is_valid_pairing_token_ok() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
            store_directory: MessageHubOptions::default_store_directory(),
            astarte: DeviceSdkOptions::default(),
        };

        let res = expected_msg_hub_opts.validate();
        assert!(res.is_ok(), "{:?}", res);
    }

    #[test]
    fn test_is_valid_empty_realm_err() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
            store_directory: MessageHubOptions::default_store_directory(),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_err());
    }

    #[test]
    fn test_is_valid_empty_device_id() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
            store_directory: MessageHubOptions::default_store_directory(),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_ok());
    }

    #[test]
    fn test_is_valid_empty_pairing_url_err() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "".to_string(),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
            store_directory: MessageHubOptions::default_store_directory(),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_err());
    }

    #[test]
    fn test_is_valid_empty_credentials_secred_err() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: Some("".to_string()),
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
            store_directory: MessageHubOptions::default_store_directory(),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_err());
    }

    #[test]
    fn test_is_valid_empty_pairing_token_err() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
            store_directory: MessageHubOptions::default_store_directory(),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_err());
    }

    #[test]
    fn test_is_valid_invalid_interf_dir_err() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: Some(PathBuf::from("")),
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
            store_directory: MessageHubOptions::default_store_directory(),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_err());
    }

    #[test]
    fn test_is_valid_missing_credentials_secret_and_pairing_token_err() {
        #[allow(deprecated)]
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 655,
            store_directory: MessageHubOptions::default_store_directory(),
            astarte: DeviceSdkOptions::default(),
        };
        assert!(expected_msg_hub_opts.validate().is_err());
    }

    #[tokio::test]
    async fn obtain_stored_credential() {
        let expected = "32".to_string();

        let dir = tempfile::TempDir::new().unwrap();
        fs::write(dir.path().join(CREDENTIAL_FILE), &expected)
            .await
            .unwrap();

        #[allow(deprecated)]
        let opt = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 655,
            store_directory: dir.path().to_path_buf(),
            astarte: DeviceSdkOptions::default(),
        };

        let Credential::Secret { credentials_secret } = opt.obtain_credential().await.unwrap()
        else {
            panic!("not a credential secret");
        };

        assert_eq!(credentials_secret, expected);
    }

    #[tokio::test]
    async fn obtain_credential_paring_token() {
        let dir = tempfile::TempDir::new().unwrap();

        #[allow(deprecated)]
        let opt = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("42".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 655,
            store_directory: dir.path().to_path_buf(),
            astarte: DeviceSdkOptions::default(),
        };

        let secret = opt.obtain_credential().await;

        assert!(
            secret.is_ok(),
            "error obtaining stored credential {:?}",
            secret
        );

        let Credential::ParingToken { pairing_token } = secret.unwrap() else {
            panic!("expected a pairing token");
        };

        assert_eq!(pairing_token, "42");
    }

    #[tokio::test]
    async fn load_toml_config() {
        #[allow(deprecated)]
        let expected = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("42".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 655,
            store_directory: MessageHubOptions::default_store_directory(),
            astarte: DeviceSdkOptions::default(),
        };

        let dir = tempfile::TempDir::new().unwrap();

        let path = dir.path().join(CONFIG_FILE_NAMES[0]);

        fs::write(&path, toml::to_string(&expected).unwrap())
            .await
            .unwrap();

        let path = Some(path.to_string_lossy().to_string());

        let options = MessageHubOptions::get(path, None).await;

        assert!(options.is_ok(), "error loading config {:?}", options);
        assert_eq!(options.unwrap(), expected);
    }

    #[tokio::test]
    async fn load_from_store_path() {
        #[allow(deprecated)]
        let mut expected = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("42".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 655,
            store_directory: MessageHubOptions::default_store_directory(),
            astarte: DeviceSdkOptions::default(),
        };

        let dir = tempfile::TempDir::new().unwrap();

        let path = dir.path().join(CONFIG_FILE_NAMES[0]);

        fs::write(&path, toml::to_string(&expected).unwrap())
            .await
            .unwrap();

        let options = MessageHubOptions::get(None, Some(dir.path())).await;

        // Set the store directory to the passed value
        expected.store_directory = dir.path().to_path_buf();

        assert!(options.is_ok(), "error loading config {:?}", options);
        assert_eq!(options.unwrap(), expected);
    }

    /// Make sure the example config is keep in sync with the code
    #[test]
    fn deserialize_example_config() {
        let config = include_str!("../../examples/message-hub-config.toml");

        let opts = toml::from_str::<MessageHubOptions>(config);

        assert!(opts.is_ok(), "error deserializing config: {:?}", opts);
        let opts = opts.unwrap();

        #[allow(deprecated)]
        let expected = MessageHubOptions {
            realm: "example_realm".to_string(),
            device_id: Some("YOUR_UNIQUE_DEVICE_ID".to_string()),
            credentials_secret: None,
            pairing_url: "https://api.astarte.EXAMPLE.COM".to_string(),
            pairing_token: Some("YOUR_PAIRING_TOKEN".to_string()),
            interfaces_directory: Some(PathBuf::from("/usr/share/message-hub/astarte-interfaces/")),
            astarte_ignore_ssl: false,
            grpc_socket_port: 50051,
            store_directory: PathBuf::from("/var/lib/message-hub"),
            astarte: DeviceSdkOptions::default(),
        };

        assert_ne!(opts, expected);
    }

    #[tokio::test]
    async fn obtain_configured_device_id() {
        let expected = "2".to_string();

        let dir = tempfile::TempDir::new().unwrap();
        fs::write(dir.path().join(CREDENTIAL_FILE), &expected)
            .await
            .unwrap();

        #[allow(deprecated)]
        let mut opt = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("2".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 655,
            store_directory: dir.path().to_path_buf(),
            astarte: DeviceSdkOptions::default(),
        };

        let device_id = opt.obtain_device_id().await;

        assert!(
            device_id.is_ok(),
            "error obtaining stored credential {:?}",
            device_id
        );
        assert_eq!(device_id.unwrap(), expected);
    }

    #[tokio::test]
    async fn obtain_device_id_configured_none() {
        let expected = "mock-id".to_string();

        let dir = tempfile::TempDir::new().unwrap();
        fs::write(dir.path().join(CREDENTIAL_FILE), &expected)
            .await
            .unwrap();

        #[allow(deprecated)]
        let mut opt = MessageHubOptions {
            realm: "1".to_string(),
            device_id: None,
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 655,
            store_directory: dir.path().to_path_buf(),
            astarte: DeviceSdkOptions::default(),
        };

        let device_id = opt.obtain_device_id().await;

        assert!(
            device_id.is_ok(),
            "error obtaining stored credential {:?}",
            device_id
        );
        assert_eq!(device_id.unwrap(), expected);
    }

    #[tokio::test]
    async fn obtain_device_id_configured_some_but_empty() {
        let expected = "mock-id".to_string();

        let dir = tempfile::TempDir::new().unwrap();
        fs::write(dir.path().join(CREDENTIAL_FILE), &expected)
            .await
            .unwrap();

        #[allow(deprecated)]
        let mut opt = MessageHubOptions {
            realm: "1".to_string(),
            device_id: Some("".to_string()),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 655,
            store_directory: dir.path().to_path_buf(),
            astarte: DeviceSdkOptions::default(),
        };

        let device_id = opt.obtain_device_id().await;

        assert!(
            device_id.is_ok(),
            "error obtaining stored credential {:?}",
            device_id
        );
        assert_eq!(device_id.unwrap(), expected);
    }
}
