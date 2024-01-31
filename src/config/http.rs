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

//! Provides an HTTP API to set The Message Hub configurations

use std::io;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use log::debug;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::Notify;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::config::MessageHubOptions;
use crate::error::ConfigValidationError;

use super::DeviceSdkOptions;

/// HTTP server error
#[derive(thiserror::Error, Debug, displaydoc::Display)]
pub enum HttpError {
    /// couldn't bind the address {addr}
    Bind {
        /// address
        addr: String,
        /// backtrace error
        #[source]
        backtrace: io::Error,
    },
    /// couldn't start the HTTP server
    Serve(#[source] io::Error),
    /// server panicked
    Join(#[from] JoinError),
}

/// HTTP errors that will be mapped into an HTTP [Response]
#[derive(thiserror::Error, Debug, displaydoc::Display)]
pub enum ErrorResponse {
    /// invalid configuration
    InvalidConfig(#[from] ConfigValidationError),
    /// failed to serialize config
    Serialize(#[from] toml::ser::Error),
    /// write config file
    Write(#[from] io::Error),
    /// failed to send over channel
    Channel(#[from] SendError<()>),
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            ErrorResponse::InvalidConfig(err) => (
                StatusCode::BAD_REQUEST,
                format!("Invalid configuration: {}", err),
            ),
            ErrorResponse::Serialize(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error in config serialization, {}", err),
            ),
            ErrorResponse::Write(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Unable to write in toml file, {}", err),
            ),
            ErrorResponse::Channel(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Channel error, {}", err),
            ),
        };

        let t = (
            status,
            Json(ConfigResponse {
                result: "KO".to_string(),
                message: Some(msg),
            }),
        );

        t.into_response()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ConfigResponse {
    result: String,
    message: Option<String>,
}

impl Default for ConfigResponse {
    fn default() -> Self {
        ConfigResponse {
            result: "OK".to_string(),
            message: None,
        }
    }
}

#[derive(Debug, Clone)]
struct ConfigServer {
    configuration_ready_channel: Arc<Notify>,
    toml_file: Arc<String>,
}

impl ConfigServer {
    fn new(configuration_ready_channel: Arc<Notify>, toml_file: &str) -> Self {
        Self {
            configuration_ready_channel,
            toml_file: Arc::new(toml_file.into()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ConfigPayload {
    realm: String,
    device_id: Option<String>,
    credentials_secret: Option<String>,
    pairing_url: String,
    pairing_token: Option<String>,
    grpc_socket_port: u16,
}

/// Provides an HTTP API to set The Message Hub configurations
#[derive(Debug)]
pub struct HttpConfigProvider {
    handle: JoinHandle<Result<(), HttpError>>,
    stop: CancellationToken,
}

impl HttpConfigProvider {
    /// Start a new HTTP API Server to allow a third party to feed the Message Hub configurations
    pub async fn serve(
        address: &str,
        configuration_ready_channel: Arc<Notify>,
        toml_file: &str,
    ) -> Result<HttpConfigProvider, HttpError> {
        let cfg_server = ConfigServer::new(configuration_ready_channel, toml_file);

        let app = Router::new()
            .route("/", get(root))
            .route("/config", post(set_config))
            .with_state(cfg_server);

        let c_token = CancellationToken::new();
        let c_token_cl = c_token.clone();

        let listener = TcpListener::bind(address)
            .await
            .map_err(|e| HttpError::Bind {
                addr: address.to_string(),
                backtrace: e,
            })?;

        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    c_token_cl.cancelled().await;
                    debug!("cancelled, shutting down");
                })
                .await
                .map_err(HttpError::Serve)
        });

        Ok(HttpConfigProvider {
            handle,
            stop: c_token,
        })
    }

    /// Stop the HTTP API Server
    pub async fn stop(self) -> Result<(), HttpError> {
        self.stop.cancel();
        self.handle.await.map_err(HttpError::Join)?
    }
}

/// HTTP API endpoint that respond on a request done on the root (used for test purposes)
async fn root() -> (StatusCode, Json<ConfigResponse>) {
    (StatusCode::OK, Json(ConfigResponse::default()))
}

/// HTTP API endpoint that allows to set The Message Hub configurations
async fn set_config(
    State(state): State<ConfigServer>,
    Json(payload): Json<ConfigPayload>,
) -> Result<(StatusCode, Json<ConfigResponse>), ErrorResponse> {
    #[allow(deprecated)]
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
        astarte: DeviceSdkOptions { ignore_ssl: false },
    };

    message_hub_options.validate()?;

    let cfg = toml::to_string(&message_hub_options)?;

    tokio::fs::write(state.toml_file.as_ref(), cfg).await?;

    state.configuration_ready_channel.notify_one();

    Ok((StatusCode::OK, Json(ConfigResponse::default())))
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::time::Duration;

    use serde_json::{Map, Number, Value};
    use serial_test::serial;
    use tempfile::TempDir;

    use crate::config::file::CONFIG_FILE_NAMES;

    use super::*;

    #[tokio::test]
    #[serial]
    async fn server_test() {
        let notify = Arc::new(Notify::new());

        let dir = TempDir::new().unwrap();
        let toml_file = dir
            .path()
            .join(CONFIG_FILE_NAMES[0])
            .to_string_lossy()
            .to_string();

        let server = HttpConfigProvider::serve("127.0.0.1:8080", Arc::clone(&notify), &toml_file)
            .await
            .expect("failed to create server");

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
        notify.notified().await;
        server.stop().await.expect("failed to stop server");
        tokio::time::sleep(Duration::from_secs(2)).await;
        let resp = reqwest::get("http://localhost:8080/").await;
        assert!(resp.is_err());
    }

    #[tokio::test]
    #[serial]
    async fn bad_request_test() {
        let notify = Arc::new(Notify::new());

        let dir = TempDir::new().unwrap();
        let toml_file = dir
            .path()
            .join(CONFIG_FILE_NAMES[0])
            .to_string_lossy()
            .to_string();

        let server = HttpConfigProvider::serve("127.0.0.1:8081", Arc::clone(&notify), &toml_file)
            .await
            .expect("failed to create server");

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
        server.stop().await.expect("failed to stop server");
        tokio::time::sleep(Duration::from_secs(2)).await;
        let resp = reqwest::get("http://localhost:8081/").await;
        assert!(resp.is_err());
    }

    #[tokio::test]
    #[serial]
    async fn test_set_config_invalid_cfg() {
        let notify = Arc::new(Notify::new());

        let dir = TempDir::new().unwrap();
        let toml_file = dir
            .path()
            .join(CONFIG_FILE_NAMES[0])
            .to_string_lossy()
            .to_string();

        let server = HttpConfigProvider::serve("127.0.0.1:8080", Arc::clone(&notify), &toml_file)
            .await
            .expect("failed to create server");

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
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
        let json: ConfigResponse = resp.json().await.unwrap();
        assert_eq!(json.result, "KO".to_string());
        server.stop().await.expect("failed to stop server");
    }
}
