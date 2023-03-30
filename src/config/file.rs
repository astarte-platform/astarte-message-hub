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

use std::fs;
use std::path::Path;

use toml::Value;

use crate::config::MessageHubOptions;
use crate::error::AstarteMessageHubError;

pub const CONFIG_FILE_NAME: &str = "message-hub-config.toml";

#[derive(Debug, PartialEq, Eq)]
enum BaseTomlFileContent {
    MsgHubOpts(MessageHubOptions),
    RedirectDir(String),
}

pub fn get_options(
    base_toml_path: &Path,
) -> Result<(Option<MessageHubOptions>, String), AstarteMessageHubError> {
    let base_toml_str = fs::read_to_string(base_toml_path)?;
    match read_options_from_base_toml(base_toml_str)? {
        BaseTomlFileContent::MsgHubOpts(msg_hub_opt) => Ok((Some(msg_hub_opt), "".to_string())),
        BaseTomlFileContent::RedirectDir(redir_dir) => {
            let fallback_toml_path = Path::new(&redir_dir).join(CONFIG_FILE_NAME);
            let fallback_toml_str = fs::read_to_string(fallback_toml_path)?;
            let fallback_toml_opt = read_options_from_toml(&fallback_toml_str);
            Ok((fallback_toml_opt, redir_dir))
        }
    }
}

fn read_options_from_base_toml(
    toml_str: String,
) -> Result<BaseTomlFileContent, AstarteMessageHubError> {
    if let Some(msg_hub_opt) = read_options_from_toml(&toml_str) {
        return Ok(BaseTomlFileContent::MsgHubOpts(msg_hub_opt));
    }
    if let Some(Value::String(redir_dir)) = toml_str.parse::<Value>()?.get("redirect_directory") {
        return Ok(BaseTomlFileContent::RedirectDir(redir_dir.clone()));
    }
    let err_msg = "Base toml file does not contain valid options or a redirect directory.";
    Err(AstarteMessageHubError::FatalError(err_msg.to_string()))
}

pub fn read_options_from_toml(toml_str: &str) -> Option<MessageHubOptions> {
    toml::from_str::<MessageHubOptions>(toml_str)
        .ok()
        .filter(|opt| opt.is_valid())
}

#[cfg(test)]
mod test {
    use super::read_options_from_base_toml;
    use super::read_options_from_toml;
    use super::BaseTomlFileContent;
    use super::MessageHubOptions;

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

        let res = read_options_from_toml(TOML_FILE);
        let options = res.expect("Parsing of TOML file failed");
        assert_eq!(options.realm, "1");
        assert_eq!(options.device_id, "2");
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

        let res = read_options_from_toml(TOML_FILE);
        let options = res.expect("Parsing of TOML file failed");
        assert_eq!(options.realm, "1");
        assert_eq!(options.device_id, "2");
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

        let res = read_options_from_toml(TOML_FILE);
        let options = res.expect("Parsing of TOML file failed");
        assert_eq!(options.realm, "1");
        assert_eq!(options.device_id, "2");
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

        let res = read_options_from_toml(TOML_FILE);
        assert!(res.is_none());
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

        let res = read_options_from_toml(TOML_FILE);
        assert!(res.is_none());
    }

    #[test]
    fn test_read_base_toml_file_only_redirect_dir_ok() {
        const TOML_FILE: &str = r#"
            redirect_directory = "1"
        "#;

        let res = read_options_from_base_toml(TOML_FILE.to_string());
        assert_eq!(
            res.expect("Parsing of base toml failed."),
            BaseTomlFileContent::RedirectDir("1".to_string())
        );
    }

    #[test]
    fn test_read_base_toml_file_redirect_dir_and_valid_options_ok() {
        const TOML_FILE: &str = r#"
            realm = "1"
            device_id = "2"
            pairing_url = "3"
            credentials_secret = "4"
            astarte_ignore_ssl = false
            grpc_socket_port = 5
            redirect_directory = "6"
        "#;

        let res = read_options_from_base_toml(TOML_FILE.to_string());
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
        assert_eq!(
            res.expect("Parsing of base toml failed."),
            BaseTomlFileContent::MsgHubOpts(expected_msg_hub_opts)
        );
    }

    #[test]
    fn test_read_base_toml_file_no_redirect_dir_and_invalid_opts_err() {
        const TOML_FILE: &str = r#"
            realm = "1"
            device_id = "2"
            credentials_secret = "3"
            astarte_ignore_ssl = false
            grpc_socket_port = 4
        "#;

        let res = read_options_from_base_toml(TOML_FILE.to_string());
        assert!(res.is_err());
    }
}
