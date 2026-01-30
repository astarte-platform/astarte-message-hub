// This file is part of Astarte.
//
// Copyright 2022, 2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Provides an HTTP API to set The Message Hub configurations

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use eyre::Context;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

use crate::error::ConfigError;

use super::Config;

/// HTTP server error
#[derive(thiserror::Error, Debug)]
pub enum HttpError {
    /// couldn't bind the address {addr}
    #[error("couldn't bind the address {addr}")]
    Bind {
        /// address
        addr: SocketAddr,
        /// backtrace error
        source: io::Error,
    },
    /// server panicked
    #[error("server panicked")]
    Join(#[from] JoinError),
}

/// HTTP errors that will be mapped into an HTTP [Response]
#[derive(thiserror::Error, Debug, displaydoc::Display)]
pub enum ErrorResponse {
    /// invalid configuration
    InvalidConfig(#[from] ConfigError),
    /// failed to serialize config
    Serialize(#[from] toml::ser::Error),
    /// write config file
    Write(#[from] io::Error),
    /// failed to send over channel
    Channel,
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            ErrorResponse::InvalidConfig(err) => (
                StatusCode::BAD_REQUEST,
                format!("Invalid configuration: {err}"),
            ),
            ErrorResponse::Serialize(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error in config serialization, {err}"),
            ),
            ErrorResponse::Write(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Unable to write in toml file, {err}"),
            ),
            ErrorResponse::Channel => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Channel error".to_string(),
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

#[derive(Debug)]
struct ConfigServer {
    tx: mpsc::Sender<Config>,
    config_file: Option<PathBuf>,
}

impl ConfigServer {
    fn new(tx: mpsc::Sender<Config>, config_file: Option<PathBuf>) -> Self {
        Self { tx, config_file }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ConfigPayload {
    realm: String,
    device_id: Option<String>,
    credentials_secret: Option<String>,
    pairing_url: String,
    pairing_token: Option<String>,
    grpc_socket_host: Option<IpAddr>,
    grpc_socket_port: Option<u16>,
}

/// Start a new HTTP API Server to allow a third party to feed the Message Hub configurations
pub async fn serve(
    tasks: &mut JoinSet<eyre::Result<()>>,
    cancel: CancellationToken,
    address: &SocketAddr,
    tx: mpsc::Sender<Config>,
    config_file: Option<PathBuf>,
) -> Result<SocketAddr, HttpError> {
    let cfg_server = ConfigServer::new(tx, config_file);

    let app = Router::new()
        .route("/", get(root))
        .route("/config", post(set_config))
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(cfg_server));

    let listener = TcpListener::bind(address)
        .await
        .map_err(|e| HttpError::Bind {
            addr: *address,
            source: e,
        })?;

    let local_addr = listener.local_addr().map_err(|error| {
        error!(%error, "couldn't get binded address");

        HttpError::Bind {
            addr: *address,
            source: error,
        }
    })?;

    info!("HTTP dynamic config server listening on http://{local_addr}");

    tasks.spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                cancel.cancelled().await;

                info!("HTTP server exiting");
            })
            .await
            .wrap_err("couldn't run HTTP dynamic config server")
    });

    Ok(local_addr)
}

/// HTTP API endpoint that respond on a request done on the root (used for test purposes)
async fn root() -> (StatusCode, Json<ConfigResponse>) {
    (StatusCode::OK, Json(ConfigResponse::default()))
}

/// HTTP API endpoint that allows to set The Message Hub configurations
async fn set_config(
    State(state): State<Arc<ConfigServer>>,
    Json(payload): Json<ConfigPayload>,
) -> Result<(StatusCode, Json<ConfigResponse>), ErrorResponse> {
    let config = Config {
        realm: Some(payload.realm),
        device_id: payload.device_id,
        credentials_secret: payload.credentials_secret,
        pairing_url: Some(payload.pairing_url),
        pairing_token: payload.pairing_token,
        grpc_socket_host: payload.grpc_socket_host,
        grpc_socket_port: payload.grpc_socket_port,
        ..Default::default()
    };

    config.validate()?;

    if let Some(config_file) = &state.config_file {
        let cfg = toml::to_string(&config)?;

        if let Err(error) = tokio::fs::write(config_file, cfg).await {
            error!(%error, config = %config_file.display(), "coulnd't write configuration file");
        }
    }

    state
        .tx
        .send_timeout(config, Duration::from_secs(10))
        .await
        .map_err(|error| {
            error!(%error, "couldn't send configuration");

            ErrorResponse::Channel
        })?;

    Ok((StatusCode::OK, Json(ConfigResponse::default())))
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use serde_json::{Map, Number, Value};
    use tempfile::TempDir;

    use crate::config::CONFIG_FILE_NAME;

    use super::*;

    struct TestServer {
        tasks: JoinSet<eyre::Result<()>>,
        cancel_token: CancellationToken,
        config_file: PathBuf,
        rx: mpsc::Receiver<Config>,
        address: SocketAddr,
        _dir: TempDir,
    }

    impl TestServer {
        async fn serve() -> Self {
            let dir = TempDir::new().unwrap();

            let toml_file = dir.path().join(CONFIG_FILE_NAME);

            let mut tasks = JoinSet::new();
            let cancel_token = CancellationToken::new();
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            let address = serve(
                &mut tasks,
                cancel_token.clone(),
                &"127.0.0.1:0".parse().unwrap(),
                tx,
                Some(toml_file.clone()),
            )
            .await
            .expect("failed to create server");

            Self {
                tasks,
                cancel_token,
                config_file: toml_file,
                rx,
                address,
                _dir: dir,
            }
        }
    }

    #[rstest]
    #[timeout(Duration::from_secs(2))]
    #[tokio::test]
    async fn server_test() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let mut server = TestServer::serve().await;

        let exp = Config {
            realm: Some("realm".to_string()),
            device_id: Some("device_id".to_string()),
            pairing_url: Some("pairing_url".to_string()),
            credentials_secret: Some("credentials_secret".to_string()),
            ..Default::default()
        };

        let client = reqwest::Client::new();

        let resp = client
            .post(format!("http://{}/config", server.address))
            .json(&exp)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let json: ConfigResponse = resp.json().await.unwrap();
        assert_eq!(json.result, "OK".to_string());

        let config = server.rx.try_recv().unwrap();

        assert_eq!(config, exp);

        server.cancel_token.cancel();

        server.tasks.join_next().await.unwrap().unwrap().unwrap();

        let config: Config =
            toml::from_str(&tokio::fs::read_to_string(server.config_file).await.unwrap()).unwrap();

        assert_eq!(config, exp);
    }

    #[rstest]
    #[timeout(Duration::from_secs(2))]
    #[tokio::test]
    async fn bad_request_test() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let mut server = TestServer::serve().await;

        let mut body = HashMap::new();
        body.insert("device_id", "device_id");
        body.insert("pairing_url", "pairing_url");

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/config", server.address))
            .json(&body)
            .send()
            .await
            .unwrap();

        let status = resp.status();
        assert!(!status.is_success());

        server.cancel_token.cancel();

        server.tasks.join_next().await.unwrap().unwrap().unwrap();
    }

    #[rstest]
    #[timeout(Duration::from_secs(2))]
    #[tokio::test]
    async fn test_set_config_invalid_cfg() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let mut server = TestServer::serve().await;

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
            .post(format!("http://{}/config", server.address))
            .json(&body)
            .send()
            .await
            .unwrap();

        let status = resp.status();
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
        let json: ConfigResponse = resp.json().await.unwrap();
        assert_eq!(json.result, "KO".to_string());

        server.cancel_token.cancel();

        server.tasks.join_next().await.unwrap().unwrap().unwrap();
    }
}
