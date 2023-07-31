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

//! Provides an HTTP API to set The Message Hub configurations

use std::io::Write;
use std::net::SocketAddr;
use std::panic;
use std::path::Path;
use std::str::FromStr;

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
    configuration_ready_channel: Sender<()>,
    toml_file: String,
}

/// Provides an HTTP API to set The Message Hub configurations
pub struct HttpConfigProvider {
    shutdown_channel: Sender<()>,
}

#[derive(Deserialize)]
struct ConfigPayload {
    realm: String,
    device_id: Option<String>,
    credentials_secret: Option<String>,
    pairing_url: String,
    pairing_token: Option<String>,
    grpc_socket_port: u16,
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
            realm: payload.realm,
            device_id: payload.device_id,
            credentials_secret: payload.credentials_secret,
            pairing_url: payload.pairing_url,
            pairing_token: payload.pairing_token,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: payload.grpc_socket_port,
            store_directory: MessageHubOptions::default_store_directory(),
        };

        if let Err(err) = message_hub_options.validate() {
            return (
                StatusCode::BAD_REQUEST,
                Json(ConfigResponse {
                    result: "KO".to_string(),
                    message: Some(format!("Invalid configuration: {}", err)),
                }),
            );
        }

        let result = std::fs::File::create(Path::new(&state.toml_file));
        if result.is_err() {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ConfigResponse {
                    result: "KO".to_string(),
                    message: Some(format!("Unable to create file {}", state.toml_file)),
                }),
            );
        }

        let mut file = result.unwrap();

        let result = toml::to_string(&message_hub_options);
        if result.is_err() {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ConfigResponse {
                    result: "KO".to_string(),
                    message: Some("Error in config serialization".to_string()),
                }),
            );
        }

        let cfg = result.unwrap();

        if write!(file, "{cfg}").is_err() {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ConfigResponse {
                    result: "KO".to_string(),
                    message: Some(format!("Unable to write in file {}", state.toml_file)),
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
        configuration_ready_channel: Sender<()>,
        toml_file: &str,
    ) -> HttpConfigProvider {
        let extension = ConfigServerExtension {
            configuration_ready_channel,
            toml_file: toml_file.to_string(),
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
    use super::*;

    use serial_test::serial;
    use std::collections::HashMap;
    use std::time::Duration;
    use tempfile::TempDir;

    use serde_json::{Map, Number, Value};

    use crate::config::file::CONFIG_FILE_NAMES;

    #[tokio::test]
    #[serial]
    async fn server_test() {
        let (tx, mut rx) = channel(1);

        let dir = TempDir::new().unwrap();
        let toml_file = dir
            .path()
            .join(CONFIG_FILE_NAMES[0])
            .to_string_lossy()
            .to_string();

        let server = HttpConfigProvider::new("127.0.0.1:8080", tx, &toml_file);

        let mut body = Map::new();
        body.insert("realm".to_string(), Value::String("realm".to_string()));
        body.insert(
            "device_id".to_string(),
            Value::String("device_id".to_string()),
        );
        body.insert(
            "credentials_secret".to_string(),
            Value::String("credentials_secret".to_string()),
        );
        body.insert(
            "pairing_url".to_string(),
            Value::String("pairing_url".to_string()),
        );
        body.insert(
            "grpc_socket_port".to_string(),
            Value::Number(Number::from(22_u16)),
        );

        let client = reqwest::Client::new();
        let resp = client
            .post("http://localhost:8080/config")
            .json(&body)
            .send()
            .await
            .unwrap();

        let status = resp.status();
        assert!(status.is_success());
        let json: ConfigResponse = resp.json().await.unwrap();
        assert_eq!(json.result, "OK".to_string());
        assert!(rx.recv().await.is_some());
        server.stop().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let resp = reqwest::get("http://localhost:8080/").await;
        assert!(resp.is_err());
    }

    #[tokio::test]
    #[serial]
    async fn bad_request_test() {
        let (tx, _) = channel(1);

        let dir = TempDir::new().unwrap();
        let toml_file = dir
            .path()
            .join(CONFIG_FILE_NAMES[0])
            .to_string_lossy()
            .to_string();

        let server = HttpConfigProvider::new("127.0.0.1:8081", tx, &toml_file);

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

        let status = resp.status();
        assert!(!status.is_success());
        server.stop().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let resp = reqwest::get("http://localhost:8081/").await;
        assert!(resp.is_err());
    }

    #[tokio::test]
    #[serial]
    async fn test_set_config_invalid_cfg() {
        let (tx, _) = channel(1);

        let dir = TempDir::new().unwrap();
        let toml_file = dir
            .path()
            .join(CONFIG_FILE_NAMES[0])
            .to_string_lossy()
            .to_string();

        let server = HttpConfigProvider::new("127.0.0.1:8080", tx, &toml_file);

        let mut body = Map::new();
        body.insert("realm".to_string(), Value::String("".to_string()));
        body.insert(
            "device_id".to_string(),
            Value::String("device_id".to_string()),
        );
        body.insert(
            "credentials_secret".to_string(),
            Value::String("credentials_secret".to_string()),
        );
        body.insert(
            "pairing_url".to_string(),
            Value::String("pairing_url".to_string()),
        );
        body.insert(
            "grpc_socket_port".to_string(),
            Value::Number(Number::from(22_u16)),
        );

        let client = reqwest::Client::new();
        let resp = client
            .post("http://localhost:8080/config")
            .json(&body)
            .send()
            .await
            .unwrap();

        let status = resp.status();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let json: ConfigResponse = resp.json().await.unwrap();
        assert_eq!(json.result, "KO".to_string());
        server.stop().await;
    }
}
