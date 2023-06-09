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

//! Load the toml file in the [MessageHubOptions] struct.

use std::fs;
use std::path::Path;

use crate::config::MessageHubOptions;
use crate::error::AstarteMessageHubError;

/// Locations for the configuration files
pub const CONFIG_FILE_NAMES: [&str; 2] =
    ["message-hub-config.toml", "/etc/message-hub/config.toml"];

/// Get the message hub options from one of the default locations for .toml configuration files.
pub fn get_options_from_base_toml() -> Result<MessageHubOptions, AstarteMessageHubError> {
    let existing_toml_path = CONFIG_FILE_NAMES.iter().map(Path::new).find(|f| f.exists());

    if let Some(toml_path) = existing_toml_path {
        let toml_str = fs::read_to_string(toml_path)?;
        get_options_from_toml(&toml_str)
    } else {
        let err_msg = "No configuration file found in the base locations.";
        Err(AstarteMessageHubError::FatalError(err_msg.to_string()))
    }
}

/// Get the message hub options from the toml file passed as input.
pub fn get_options_from_toml(toml_str: &str) -> Result<MessageHubOptions, AstarteMessageHubError> {
    toml::from_str::<MessageHubOptions>(toml_str)
        .map_err(AstarteMessageHubError::ConfigFileError)
        .and_then(|opt| {
            opt.validate().map_err(|err| {
                AstarteMessageHubError::FatalError(format!("Invalid configuration file: {}", err))
            })?;

            Ok(opt)
        })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_read_options_from_toml_cred_secred_ok() {
        const TOML_FILE: &str = r#"
            realm = "1"
            device_id = "2"
            pairing_url = "3"
            credentials_secret = "4"
            astarte_ignore_ssl = false
            grpc_socket_port = 5
        "#;

        let res = get_options_from_toml(TOML_FILE);
        let options = res.expect("Parsing of TOML file failed");
        assert_eq!(options.realm, "1");
        assert_eq!(options.device_id, Some("2".to_string()));
        assert_eq!(options.pairing_url, "3");
        assert_eq!(options.credentials_secret, Some("4".to_string()));
        assert_eq!(options.pairing_token, None);
        assert!(!options.astarte_ignore_ssl);
        assert_eq!(options.grpc_socket_port, 5);
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
        "#;

        let res = get_options_from_toml(TOML_FILE);
        let options = res.expect("Parsing of TOML file failed");
        assert_eq!(options.realm, "1");
        assert_eq!(options.device_id, Some("2".to_string()));
        assert_eq!(options.pairing_url, "3");
        assert_eq!(options.credentials_secret, None);
        assert_eq!(options.pairing_token, Some("4".to_string()));
        assert!(options.astarte_ignore_ssl);
        assert_eq!(options.grpc_socket_port, 5);
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
        "#;

        let res = get_options_from_toml(TOML_FILE);
        let options = res.expect("Parsing of TOML file failed");
        assert_eq!(options.realm, "1");
        assert_eq!(options.device_id, Some("2".to_string()));
        assert_eq!(options.pairing_url, "3");
        assert_eq!(options.credentials_secret, Some("4".to_string()));
        assert_eq!(options.pairing_token, Some("5".to_string()));
        assert!(options.astarte_ignore_ssl);
        assert_eq!(options.grpc_socket_port, 6);
    }

    #[test]
    fn test_read_options_from_toml_missing_pairing_and_cred_sec_err() {
        const TOML_FILE: &str = r#"
            realm = "1"
            device_id = "2"
            pairing_url = "3"
            astarte_ignore_ssl = true
            grpc_socket_port = 4
        "#;

        let res = get_options_from_toml(TOML_FILE);
        assert!(res.is_err());
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
        "#;

        let res = get_options_from_toml(TOML_FILE);
        assert!(res.is_err());
    }
}
