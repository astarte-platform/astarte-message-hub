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

//! Provides a Protobuf API to set The Message Hub configurations

use std::net::{AddrParseError, IpAddr, SocketAddr};
use std::num::TryFromIntError;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use astarte_message_hub_proto::ConfigMessage;
use astarte_message_hub_proto::message_hub_config_server::{
    MessageHubConfig, MessageHubConfigServer,
};
use eyre::Context;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Code, Request, Response, Status};
use tracing::{error, info};

use super::Config;

/// Protobuf server error
#[derive(thiserror::Error, Debug, displaydoc::Display)]
pub enum ProtobufConfigError {
    /// couldn't parse the socket address
    ParseSocketAddr(#[from] AddrParseError),
    /// server panicked
    Join(#[from] JoinError),
    /// failed to send over channel
    Channel(#[from] SendError<()>),
}

#[derive(Debug)]
struct AstarteMessageHubConfig {
    tx: tokio::sync::mpsc::Sender<Config>,
    /// Optional config file in the store directory to save the configuration to.
    config_file: Option<PathBuf>,
}

#[tonic::async_trait]
impl MessageHubConfig for AstarteMessageHubConfig {
    /// Protobuf API that allows to set The Message Hub configurations
    async fn set_config(&self, request: Request<ConfigMessage>) -> Result<Response<()>, Status> {
        let req = request.into_inner();

        let host = req
            .grpc_socket_host
            .map(|host| IpAddr::from_str(&host))
            .transpose()
            .map_err(|err| {
                Status::new(Code::InvalidArgument, format!("Invalid grpc host: {err}"))
            })?;

        // Protobuf version 3 only supports u32
        let port = req
            .grpc_socket_port
            .map(u16::try_from)
            .transpose()
            .map_err(|err: TryFromIntError| {
                Status::new(Code::InvalidArgument, format!("Invalid grpc port: {err}"))
            })?;

        let config = Config {
            realm: Some(req.realm),
            device_id: req.device_id,
            credentials_secret: req.credentials_secret,
            pairing_token: req.pairing_token,
            pairing_url: Some(req.pairing_url),
            grpc_socket_port: port,
            grpc_socket_host: host,
            ..Default::default()
        };

        config.validate().map_err(|err| {
            error!("invalid config: {err}");

            Status::invalid_argument(err.to_string())
        })?;

        if let Some(config_file) = &self.config_file {
            let cfg = toml::to_string(&config).map_err(|e| Status::internal(e.to_string()))?;

            if let Err(error) = tokio::fs::write(config_file, cfg).await {
                error!(
                    config_file = %config_file.display(),
                    %error,
                    "couldn't write configuration file"
                );
            }
        }

        self.tx
            .send_timeout(config, Duration::from_secs(10))
            .await
            .map_err(|error| {
                error!(%error, "couldn't send configuration file");

                Status::internal("couldn't send configuration file")
            })?;

        Ok(Response::new(()))
    }
}

/// Starts the dynamic configuration gRPC server
pub async fn serve(
    tasks: &mut JoinSet<eyre::Result<()>>,
    cancel: CancellationToken,
    address: SocketAddr,
    tx: mpsc::Sender<Config>,
    config_file: Option<PathBuf>,
) -> eyre::Result<SocketAddr> {
    let service = AstarteMessageHubConfig { tx, config_file };

    let listener = TcpIncoming::bind(address).wrap_err("couldn't bind gRPC server address")?;

    let local_addr = listener
        .local_addr()
        .wrap_err("couldn't get gRPC local address")?;

    info!("gRPC dynamic config server listening on http://{local_addr}");

    tasks.spawn(async move {
        Server::builder()
            .add_service(MessageHubConfigServer::new(service))
            .serve_with_incoming_shutdown(listener, async {
                cancel.cancelled().await;

                info!("gRPC dynamic config server exiting")
            })
            .await
            .wrap_err("couldn't serve gRPC dynamic configuration server")
    });

    Ok(local_addr)
}

#[cfg(test)]
mod test {
    use astarte_message_hub_proto::message_hub_config_client::MessageHubConfigClient;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;
    use tonic::transport::Endpoint;

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
                "127.0.0.1:0".parse().unwrap(),
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

    fn create_config() -> (TempDir, AstarteMessageHubConfig, mpsc::Receiver<Config>) {
        let dir = TempDir::new().unwrap();
        let config_file = dir.path().join(CONFIG_FILE_NAME);

        let (tx, rx) = mpsc::channel(1);

        let config_server = AstarteMessageHubConfig {
            tx,
            config_file: Some(config_file),
        };

        (dir, config_server, rx)
    }

    #[tokio::test]
    async fn set_config_test() {
        let (_dir, config_server, mut rx) = create_config();

        let msg = ConfigMessage {
            realm: "rpc_realm".to_string(),
            device_id: Some("rpc_device_id".to_string()),
            credentials_secret: None,
            pairing_url: "rpc_pairing_url".to_string(),
            pairing_token: Some("rpc_pairing_token".to_string()),
            grpc_socket_port: Some(42),
            grpc_socket_host: None,
        };

        config_server.set_config(Request::new(msg)).await.unwrap();

        let exp = Config {
            realm: Some("rpc_realm".to_string()),
            device_id: Some("rpc_device_id".to_string()),
            pairing_url: Some("rpc_pairing_url".to_string()),
            pairing_token: Some("rpc_pairing_token".to_string()),
            grpc_socket_port: Some(42),
            ..Default::default()
        };

        let config = rx.try_recv().unwrap();

        assert_eq!(config, exp);
    }

    #[tokio::test]
    async fn test_set_config_invalid_config() {
        let (_dir, config_server, _rx) = create_config();

        let msg = ConfigMessage {
            realm: "".to_string(),
            device_id: Some("rpc_device_id".to_string()),
            credentials_secret: None,
            pairing_url: "rpc_pairing_url".to_string(),
            pairing_token: Some("rpc_pairing_token".to_string()),
            grpc_socket_port: Some(42),
            grpc_socket_host: None,
        };

        config_server
            .set_config(Request::new(msg))
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn server_test() {
        let mut server = TestServer::serve().await;

        let channel = Endpoint::from_str(&format!("http://{}", server.address))
            .unwrap()
            .connect()
            .await
            .unwrap();

        let mut client = MessageHubConfigClient::new(channel);
        let msg = ConfigMessage {
            realm: "rpc_realm".to_string(),
            device_id: Some("rpc_device_id".to_string()),
            credentials_secret: None,
            pairing_url: "rpc_pairing_url".to_string(),
            pairing_token: Some("rpc_pairing_token".to_string()),
            grpc_socket_port: Some(42),
            grpc_socket_host: None,
        };

        client.set_config(msg).await.unwrap();

        let config = server.rx.try_recv().unwrap();

        let exp = Config {
            realm: Some("rpc_realm".to_string()),
            device_id: Some("rpc_device_id".to_string()),
            pairing_url: Some("rpc_pairing_url".to_string()),
            pairing_token: Some("rpc_pairing_token".to_string()),
            grpc_socket_port: Some(42),
            ..Default::default()
        };

        assert_eq!(config, exp);

        server.cancel_token.cancel();

        server.tasks.join_next().await.unwrap().unwrap().unwrap();

        let config: Config =
            toml::from_str(&tokio::fs::read_to_string(server.config_file).await.unwrap()).unwrap();

        assert_eq!(config, exp);
    }
}
