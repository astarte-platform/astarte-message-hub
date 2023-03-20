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
//! Helper module to retreive the configuration of the Astarte message hub.

use std::path::Path;

use log::info;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::channel;

use crate::config::http::HttpConfigProvider;
use crate::config::protobuf::ProtobufConfigProvider;
use crate::error::AstarteMessageHubError;

pub mod file;
pub mod http;
pub mod protobuf;

const CONFIG_FILE_NAME: &str = "message-hub-config.toml";

/// Struct containing all the configuration options for the Astarte message hub.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageHubOptions {
    pub realm: Option<String>,
    pub device_id: Option<String>,
    pub credentials_secret: Option<String>,
    pub pairing_url: Option<String>,
    pub pairing_token: Option<String>,
    pub interfaces_directory: String,
    pub store_directory: String,
    pub astarte_ignore_ssl: Option<bool>,
}

impl MessageHubOptions {
    /// Getter function for the configuration options of the Message Hub.
    ///
    /// Expects a configuration file to be present in one of the base locations.
    /// The configuration file present in the base locations might be missing some key information.
    /// If the configuration file contains at least a correct path to another configuration file,
    /// this fallback configuration file is used.
    /// When this fallback configuration file is not present, HTTP and Protobuf APIs are exposed
    /// waiting for a valid configuration on either of them.
    pub async fn get() -> Result<MessageHubOptions, AstarteMessageHubError> {
        let base_config = read_options_from_base_locations()?;
        if base_config.is_valid() {
            return Ok(base_config);
        }

        let store_directory = base_config.store_directory.clone();
        let path = Path::new(&store_directory).join(CONFIG_FILE_NAME);

        if path.exists() {
            return file::read_options(&path);
        }

        let (tx, mut rx) = channel(1);

        let web_server =
            HttpConfigProvider::new("127.0.0.1:40041", store_directory.clone(), tx.clone());
        let protobuf_server =
            ProtobufConfigProvider::new("[::1]:50051", store_directory.clone(), tx.clone()).await;

        rx.recv().await.unwrap();

        web_server.stop().await;
        protobuf_server.stop().await;
        file::read_options(&path)
    }

    /// Checks if the MessageHubOptions object contains a valid configuration.
    fn is_valid(&self) -> bool {
        self.device_id.is_some()
            && self.realm.is_some()
            && self.pairing_url.is_some()
            && (self.pairing_token.is_some() || self.credentials_secret.is_some())
    }
}

/// Function that retrieves the configuration options from a `.toml` file in
/// one of the base locations.
fn read_options_from_base_locations() -> Result<MessageHubOptions, AstarteMessageHubError> {
    let paths = ["message-hub-config.toml", "/etc/message-hub/config.toml"]
        .iter()
        .map(|f| f.to_string());

    if let Some(path) = paths.into_iter().next() {
        info!("Found configuration file {path}");
        file::read_options(Path::new(&path))
    } else {
        Err(AstarteMessageHubError::FatalError(
            "Configuration file not found".to_string(),
        ))
    }
}
