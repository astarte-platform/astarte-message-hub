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
use std::sync::Arc;

use astarte_message_hub_proto::ConfigMessage;
use astarte_message_hub_proto::message_hub_config_server::{
    MessageHubConfig, MessageHubConfigServer,
};
use log::error;
use tokio::sync::Notify;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Sender, channel};
use tokio::task::{JoinError, JoinHandle};
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use super::{Config, DeviceSdkOptions};

/// Protobuf server error
#[derive(thiserror::Error, Debug, displaydoc::Display)]
pub enum ProtobufConfigError {
    /// couldn't parse the socket address
    ParseSocketAddr(#[from] AddrParseError),
    /// couldn't start the protobuf server
    Start(#[from] tonic::transport::Error),
    /// server panicked
    Join(#[from] JoinError),
    /// failed to send over channel
    Channel(#[from] SendError<()>),
}

#[derive(Debug, Clone)]
struct AstarteMessageHubConfig {
    configuration_ready_channel: Arc<Notify>,
    config_file: PathBuf,
    store_dir: PathBuf,
}

/// Provides a Protobuf API to set The Message Hub configurations
#[derive(Debug)]
pub struct ProtobufConfigProvider {
    shutdown_channel: Sender<()>,
    handle: JoinHandle<Result<(), ProtobufConfigError>>,
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

        #[allow(deprecated)]
        let message_hub_options = Config {
            realm: Some(req.realm),
            device_id: req.device_id,
            credentials_secret: req.credentials_secret,
            pairing_token: req.pairing_token,
            pairing_url: Some(req.pairing_url),
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: port,
            grpc_socket_host: host,
            store_directory: Some(self.store_dir.clone()),
            astarte: DeviceSdkOptions::default(),
        };

        message_hub_options.validate().map_err(|err| {
            error!("invalid config: {err}");

            Status::invalid_argument(err.to_string())
        })?;

        let cfg =
            toml::to_string(&message_hub_options).map_err(|e| Status::internal(e.to_string()))?;

        tokio::fs::write(&self.config_file, cfg).await?;

        self.configuration_ready_channel.notify_one();

        Ok(Response::new(()))
    }
}

impl ProtobufConfigProvider {
    /// Start a new Protobuf API Server to allow a third party to feed the Message Hub
    /// configurations
    pub async fn new(
        address: SocketAddr,
        configuration_ready_channel: Arc<Notify>,
        config_file: PathBuf,
        store_dir: PathBuf,
    ) -> Result<ProtobufConfigProvider, ProtobufConfigError> {
        let service = AstarteMessageHubConfig {
            configuration_ready_channel,
            config_file,
            store_dir,
        };
        let (tx, mut rx) = channel::<()>(1);

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(MessageHubConfigServer::new(service))
                .serve_with_shutdown(address, async { rx.recv().await.unwrap() })
                .await
                .map_err(ProtobufConfigError::Start)
        });

        Ok(ProtobufConfigProvider {
            shutdown_channel: tx,
            handle,
        })
    }

    /// Stop the Protobuf API Server
    pub async fn stop(self) -> Result<(), ProtobufConfigError> {
        self.shutdown_channel.send(()).await?;
        self.handle.await.map_err(ProtobufConfigError::Join)?
    }
}

#[cfg(test)]
mod test {
    use astarte_message_hub_proto::message_hub_config_client::MessageHubConfigClient;
    use serial_test::serial;
    use tempfile::TempDir;
    use tonic::transport::Endpoint;

    use crate::config::CONFIG_FILE_NAME;

    use super::*;

    fn create_config() -> (TempDir, AstarteMessageHubConfig) {
        let dir = TempDir::new().unwrap();
        let config_file = dir.path().join(CONFIG_FILE_NAME);

        let notify = Arc::new(Notify::new());

        let config_server = AstarteMessageHubConfig {
            configuration_ready_channel: notify,
            config_file,
            store_dir: dir.path().to_owned(),
        };

        (dir, config_server)
    }

    #[tokio::test]
    #[serial]
    async fn set_config_test() {
        let (_dir, config_server) = create_config();

        let msg = ConfigMessage {
            realm: "rpc_realm".to_string(),
            device_id: Some("rpc_device_id".to_string()),
            credentials_secret: None,
            pairing_url: "rpc_pairing_url".to_string(),
            pairing_token: Some("rpc_pairing_token".to_string()),
            grpc_socket_port: Some(42),
            grpc_socket_host: None,
        };

        let result = config_server.set_config(Request::new(msg)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial]
    async fn test_set_config_invalid_config() {
        let (_dir, config_server) = create_config();

        let msg = ConfigMessage {
            realm: "".to_string(),
            device_id: Some("rpc_device_id".to_string()),
            credentials_secret: None,
            pairing_url: "rpc_pairing_url".to_string(),
            pairing_token: Some("rpc_pairing_token".to_string()),
            grpc_socket_port: Some(42),
            grpc_socket_host: None,
        };
        let result = config_server.set_config(Request::new(msg)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[serial]
    async fn server_test() {
        let dir = TempDir::new().unwrap();
        let config_file = dir.path().join(CONFIG_FILE_NAME);

        let notify = Arc::new(Notify::new());
        let server = ProtobufConfigProvider::new(
            "127.0.0.1:1400".parse().unwrap(),
            Arc::clone(&notify),
            config_file,
            dir.path().to_path_buf(),
        )
        .await
        .expect("failed to create a protobuf server");
        let channel = Endpoint::from_static("http://localhost:1400")
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
        let response = client.set_config(msg).await;
        assert!(response.is_ok());
        notify.notified().await;
        server.stop().await.expect("failed to stop the server");
        assert!(
            MessageHubConfigClient::connect("http://localhost:1400")
                .await
                .is_err()
        );
    }
}
