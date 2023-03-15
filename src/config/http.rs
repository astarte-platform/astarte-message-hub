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

use std::io::Write;
use std::net::SocketAddr;
use std::panic;
use std::path::Path;
use std::str::FromStr;

use crate::config;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Extension, Json, Router, Server};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Sender};

use crate::config::MessageHubOptions;

#[derive(Deserialize, Serialize)]
struct ConfigResponse {
    result: String,
    message: Option<String>,
}

#[derive(Clone)]
struct ConfigServerExtension {
    store_directory: String,
    configuration_ready_channel: Sender<()>,
}

pub struct HttpConfigProvider {
    shutdown_channel: Sender<()>,
}

#[derive(Deserialize)]
struct ConfigPayload {
    realm: String,
    device_id: String,
    credentials_secret: Option<String>,
    pairing_url: String,
    pairing_token: Option<String>,
}

impl Default for ConfigResponse {
    fn default() -> Self {
        ConfigResponse {
            result: "OK".to_string(),
            message: None,
        }
    }
}

impl HttpConfigProvider {
    /// HTTP API endpoint that allows to set The Message Hub configurations
    pub(self) async fn set_config(
        Extension(state): Extension<ConfigServerExtension>,
        Json(payload): Json<ConfigPayload>,
    ) -> impl IntoResponse {
        let message_hub_options = MessageHubOptions {
            realm: Some(payload.realm),
            device_id: Some(payload.device_id),
            credentials_secret: payload.credentials_secret,
            pairing_url: Some(payload.pairing_url),
            pairing_token: payload.pairing_token,
            interfaces_directory: "".to_string(),
            store_directory: "".to_string(),
            astarte_ignore_ssl: None,
        };

        let result =
            std::fs::File::create(Path::new(&state.store_directory).join(config::CONFIG_FILE_NAME));
        if let Err(_) = result {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ConfigResponse {
                    result: "KO".to_string(),
                    message: Some(format!(
                        "Unable to create file message-hub-config.toml in {}",
                        state.store_directory
                    )),
                }),
            );
        }

        let mut file = result.unwrap();

        let result = toml::to_string(&message_hub_options);
        if let Err(_) = result {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ConfigResponse {
                    result: "KO".to_string(),
                    message: Some("Error in config serialization".to_string()),
                }),
            );
        }

        let cfg = result.unwrap();

        if let Err(_) = write!(file, "{cfg}") {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ConfigResponse {
                    result: "KO".to_string(),
                    message: Some(format!(
                        "Unable to write in file {}/message-hub-config.toml",
                        state.store_directory
                    )),
                }),
            );
        }

        let _ = state.configuration_ready_channel.send(()).await;

        (StatusCode::OK, Json(ConfigResponse::default()))
    }

    /// HTTP API endpoint that respond on a request done on the root (used for test purposes)
    async fn root() -> impl IntoResponse {
        (StatusCode::OK, Json(ConfigResponse::default()))
    }

    /// Start a new HTTP API Server to allow a third party to feed the Message Hub configurations
    pub fn new(
        address: &str,
        store_directory: String,
        configuration_ready_channel: Sender<()>,
    ) -> HttpConfigProvider {
        let extension = ConfigServerExtension {
            store_directory,
            configuration_ready_channel,
        };
        let app = Router::new()
            .route("/", get(Self::root))
            .route("/config", post(Self::set_config))
            .layer(Extension(extension));

        let (tx, mut rx) = channel::<()>(1);
        let addr = SocketAddr::from_str(address).unwrap();

        tokio::spawn(async move {
            Server::bind(&addr)
                .serve(app.into_make_service())
                .with_graceful_shutdown(async { rx.recv().await.unwrap() })
                .await
                .unwrap_or_else(|e| panic!("Unable to start http service: {}", e));
        });

        HttpConfigProvider {
            shutdown_channel: tx,
        }
    }

    /// Stop the HTTP API Server
    pub async fn stop(&self) {
        let shutdown_channel = self.shutdown_channel.clone();
        let _ = shutdown_channel.send(()).await;
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::time::Duration;

    use tokio::sync::mpsc::channel;

    use crate::config::http::{
        ConfigPayload, ConfigResponse, ConfigServerExtension, HttpConfigProvider,
    };

    #[tokio::test]
    async fn server_test() {
        let (tx, mut rx) = channel(1);
        let server = HttpConfigProvider::new("127.0.0.1:8080", "./".to_string(), tx);

        let mut body = HashMap::new();
        body.insert("realm", "realm");
        body.insert("device_id", "device_id");
        body.insert("credentials_secret", "credentials_secret");
        body.insert("pairing_url", "pairing_url");

        let client = reqwest::Client::new();
        let resp = client
            .post("http://localhost:8080/config")
            .json(&body)
            .send()
            .await
            .unwrap();

        let status = resp.status().clone();
        let json: ConfigResponse = resp.json().await.unwrap();
        assert!(status.is_success());
        assert_eq!(json.result, "OK".to_string());
        assert!(rx.recv().await.is_some());
        server.stop().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let resp = reqwest::get("http://localhost:8080/").await;
        assert!(resp.is_err());
    }

    #[tokio::test]
    async fn bad_request_test() {
        let (tx, _) = channel(1);
        let server = HttpConfigProvider::new("127.0.0.1:8081", "./".to_string(), tx);

        let mut body = HashMap::new();
        body.insert("device_id", "device_id");
        body.insert("pairing_url", "pairing_url");

        let client = reqwest::Client::new();
        let resp = client
            .post("http://localhost:8081/config")
            .json(&body)
            .send()
            .await
            .unwrap();

        let status = resp.status().clone();
        assert!(!status.is_success());
        server.stop().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let resp = reqwest::get("http://localhost:8081/").await;
        assert!(resp.is_err());
    }
}
