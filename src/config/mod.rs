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
    pub async fn get() -> Result<MessageHubOptions, AstarteMessageHubError> {
        let base_config = file::read_options(None)?;
        let store_directory = base_config.store_directory.clone();
        let path = format!("{}message_hub_config.toml", store_directory);

        if Path::new(&path).exists() {
            return file::get_options_from_path(path);
        }

        if base_config.is_valid() {
            return Ok(base_config);
        }

        let (tx, mut rx) = channel(1);

        let web_server =
            HttpConfigProvider::new("127.0.0.1:40041", store_directory.clone(), tx.clone());
        let protobuf_server =
            ProtobufConfigProvider::new("[::1]:50051", store_directory.clone(), tx.clone()).await;

        let _ = rx.recv().await.unwrap();

        web_server.stop().await;
        protobuf_server.stop().await;
        return file::get_options_from_path(path);
    }

    fn is_valid(&self) -> bool {
        self.device_id.is_some()
            && self.realm.is_some()
            && self.pairing_url.is_some()
            && (self.pairing_token.is_some() || self.credentials_secret.is_some())
    }
}
