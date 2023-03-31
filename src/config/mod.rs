/*
 * This file is part of Edgehog.
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

use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::channel;

use crate::config::http::HttpConfigProvider;
use crate::config::protobuf::ProtobufConfigProvider;
use crate::error::AstarteMessageHubError;

pub mod file;
pub mod http;
pub mod protobuf;

use file::CONFIG_FILE_NAMES;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct MessageHubOptions {
    pub realm: String,
    pub device_id: String,
    pub credentials_secret: Option<String>,
    pub pairing_url: String,
    pub pairing_token: Option<String>,
    pub interfaces_directory: Option<String>,
    pub astarte_ignore_ssl: bool,
    pub grpc_socket_port: u32,
}

impl MessageHubOptions {
    /// Function that get the configurations needed by the Message Hub.
    /// The configuration file is first retrieved from one of two default base locations.
    /// If no valid configuration file is found in either of these locations, or if the content
    /// of the first found file is not valid HTTP and Protobuf APIs are exposed to provide a valid
    /// configuration.
    pub async fn get() -> Result<MessageHubOptions, AstarteMessageHubError> {
        if let Ok(msg_hub_opt) = file::get_options_from_base_toml() {
            return Ok(msg_hub_opt);
        }

        let (tx, mut rx) = channel(1);

        let web_server =
            HttpConfigProvider::new("127.0.0.1:40041", tx.clone(), CONFIG_FILE_NAMES[0]);
        let protobuf_server =
            ProtobufConfigProvider::new("[::1]:50051", tx.clone(), CONFIG_FILE_NAMES[0]).await;

        rx.recv().await.unwrap();

        web_server.stop().await;
        protobuf_server.stop().await;

        let toml_str = std::fs::read_to_string(CONFIG_FILE_NAMES[0])?;
        file::get_options_from_toml(&toml_str)
    }

    pub fn is_valid(&self) -> bool {
        let valid_secret = Some(true) == self.credentials_secret.clone().map(|e| !e.is_empty());
        let valid_token = Some(true) == self.pairing_token.clone().map(|e| !e.is_empty());
        let valid_interface_dir = self.interfaces_directory.is_none()
            || (Some(true)
                == self.interfaces_directory.clone().map(|e| {
                    let id_path = Path::new(&e);
                    id_path.exists() && id_path.is_dir()
                }));

        (!self.realm.is_empty())
            && (!self.device_id.is_empty())
            && (valid_secret || valid_token)
            && valid_interface_dir
            && (!self.pairing_url.is_empty())
            && (self.grpc_socket_port <= 655356)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_is_valid_cred_sec_ok() {
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: "2".to_string(),
            pairing_url: "3".to_string(),
            credentials_secret: Some("4".to_string()),
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
        };
        assert!(expected_msg_hub_opts.is_valid());
    }

    #[test]
    fn test_is_valid_pairing_token_ok() {
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: "2".to_string(),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
        };
        assert!(expected_msg_hub_opts.is_valid());
    }

    #[test]
    fn test_is_valid_empty_realm_err() {
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "".to_string(),
            device_id: "2".to_string(),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
        };
        assert!(!expected_msg_hub_opts.is_valid());
    }

    #[test]
    fn test_is_valid_empty_device_id_err() {
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: "".to_string(),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
        };
        assert!(!expected_msg_hub_opts.is_valid());
    }

    #[test]
    fn test_is_valid_empty_pairing_url_err() {
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: "2".to_string(),
            pairing_url: "".to_string(),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
        };
        assert!(!expected_msg_hub_opts.is_valid());
    }

    #[test]
    fn test_is_valid_empty_credentials_secred_err() {
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: "2".to_string(),
            pairing_url: "3".to_string(),
            credentials_secret: Some("".to_string()),
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
        };
        assert!(!expected_msg_hub_opts.is_valid());
    }

    #[test]
    fn test_is_valid_empty_pairing_token_err() {
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: "2".to_string(),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
        };
        assert!(!expected_msg_hub_opts.is_valid());
    }

    #[test]
    fn test_is_valid_invalid_grpc_socket_port_err() {
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: "2".to_string(),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 655357,
        };
        assert!(!expected_msg_hub_opts.is_valid());
    }

    #[test]
    fn test_is_valid_invalid_interf_dir_err() {
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: "2".to_string(),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: Some("4".to_string()),
            interfaces_directory: Some("".to_string()),
            astarte_ignore_ssl: false,
            grpc_socket_port: 5,
        };
        assert!(!expected_msg_hub_opts.is_valid());
    }

    #[test]
    fn test_is_valid_missing_credentials_secret_and_pairing_token_err() {
        let expected_msg_hub_opts = MessageHubOptions {
            realm: "1".to_string(),
            device_id: "2".to_string(),
            pairing_url: "3".to_string(),
            credentials_secret: None,
            pairing_token: None,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: 655,
        };
        assert!(!expected_msg_hub_opts.is_valid());
    }
}
