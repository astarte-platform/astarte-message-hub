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

//! Provides a Protobuf API to set The Message Hub configurations

use std::io::Write;
use std::num::TryFromIntError;
use std::path::Path;

use tokio::sync::mpsc::{channel, Sender};
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use crate::config::MessageHubOptions;
use crate::proto_message_hub;

#[derive(Debug)]
struct AstarteMessageHubConfig {
    configuration_ready_channel: Sender<()>,
    toml_file: String,
}

/// Provides a Protobuf API to set The Message Hub configurations
pub struct ProtobufConfigProvider {
    shutdown_channel: Sender<()>,
}

#[tonic::async_trait]
impl proto_message_hub::message_hub_config_server::MessageHubConfig for AstarteMessageHubConfig {
    /// Protobuf API that allows to set The Message Hub configurations
    async fn set_config(
        &self,
        request: Request<proto_message_hub::ConfigMessage>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        let req = request.into_inner();

        // Protobuf version 3 only supports u32
        let port: u16 = req
            .grpc_socket_port
            .try_into()
            .map_err(|err: TryFromIntError| {
                Status::new(Code::InvalidArgument, format!("Invalid grpc port: {}", err))
            })?;

        let message_hub_options = MessageHubOptions {
            realm: req.realm,
            device_id: req.device_id,
            credentials_secret: req.credentials_secret,
            pairing_url: req.pairing_url,
            pairing_token: req.pairing_token,
            interfaces_directory: None,
            astarte_ignore_ssl: false,
            grpc_socket_port: port,
            store_directory: MessageHubOptions::default_store_directory(),
        };

        if let Err(err) = message_hub_options.validate() {
            return Err(Status::new(
                Code::InvalidArgument,
                format!("Invalid configuration: {}.", err),
            ));
        }

        let mut file = std::fs::File::create(Path::new(&self.toml_file))?;

        let result = toml::to_string(&message_hub_options);
        if let Err(e) = result {
            return Err(Status::new(Code::InvalidArgument, e.to_string()));
        }

        let cfg = result.unwrap();

        write!(file, "{cfg}")?;
        let _ = self.configuration_ready_channel.send(()).await;

        Ok(Response::new(pbjson_types::Empty {}))
    }
}

impl ProtobufConfigProvider {
    /// Start a new Protobuf API Server to allow a third party to feed the Message Hub
    /// configurations
    pub async fn new(
        address: &str,
        configuration_ready_channel: Sender<()>,
        toml_file: &str,
    ) -> ProtobufConfigProvider {
        use crate::proto_message_hub::message_hub_config_server::MessageHubConfigServer;

        let addr = address.parse().unwrap();
        let service = AstarteMessageHubConfig {
            configuration_ready_channel,
            toml_file: toml_file.to_string(),
        };
        let (tx, mut rx) = channel::<()>(1);
        tokio::spawn(async move {
            Server::builder()
                .add_service(MessageHubConfigServer::new(service))
                .serve_with_shutdown(addr, async { rx.recv().await.unwrap() })
                .await
                .unwrap_or_else(|e| panic!("Unable to start protobuf service: {}", e));
        });
        ProtobufConfigProvider {
            shutdown_channel: tx,
        }
    }

    /// Stop the Protobuf API Server
    pub async fn stop(&self) {
        self.shutdown_channel.send(()).await.unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use serial_test::serial;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tonic::transport::Endpoint;

    use crate::config::file::CONFIG_FILE_NAMES;

    #[tokio::test]
    #[serial]
    async fn set_config_test() {
        use crate::proto_message_hub::message_hub_config_server::MessageHubConfig;
        use crate::proto_message_hub::ConfigMessage;

        let dir = TempDir::new().unwrap();
        let toml_file = dir
            .path()
            .join(CONFIG_FILE_NAMES[0])
            .to_string_lossy()
            .to_string();

        let (tx, _) = mpsc::channel(1);
        let config_server = AstarteMessageHubConfig {
            configuration_ready_channel: tx,
            toml_file,
        };
        let msg = ConfigMessage {
            realm: "rpc_realm".to_string(),
            device_id: Some("rpc_device_id".to_string()),
            credentials_secret: None,
            pairing_url: "rpc_pairing_url".to_string(),
            pairing_token: Some("rpc_pairing_token".to_string()),
            grpc_socket_port: 42,
        };
        let result = config_server.set_config(Request::new(msg)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial]
    async fn test_set_config_invalid_config() {
        use crate::proto_message_hub::message_hub_config_server::MessageHubConfig;
        use crate::proto_message_hub::ConfigMessage;

        let dir = TempDir::new().unwrap();
        let toml_file = dir
            .path()
            .join(CONFIG_FILE_NAMES[0])
            .to_string_lossy()
            .to_string();

        let (tx, _) = mpsc::channel(1);
        let config_server = AstarteMessageHubConfig {
            configuration_ready_channel: tx,
            toml_file,
        };
        let msg = ConfigMessage {
            realm: "".to_string(),
            device_id: Some("rpc_device_id".to_string()),
            credentials_secret: None,
            pairing_url: "rpc_pairing_url".to_string(),
            pairing_token: Some("rpc_pairing_token".to_string()),
            grpc_socket_port: 42,
        };
        let result = config_server.set_config(Request::new(msg)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[serial]
    async fn server_test() {
        use crate::proto_message_hub::message_hub_config_client::MessageHubConfigClient;
        use crate::proto_message_hub::ConfigMessage;

        let dir = TempDir::new().unwrap();
        let toml_file = dir
            .path()
            .join(CONFIG_FILE_NAMES[0])
            .to_string_lossy()
            .to_string();

        let (tx, mut rx) = mpsc::channel(1);
        let server = ProtobufConfigProvider::new("127.0.0.1:1400", tx, &toml_file).await;
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
            grpc_socket_port: 42,
        };
        let response = client.set_config(msg).await;
        assert!(response.is_ok());
        assert!(rx.recv().await.is_some());
        server.stop().await;
        assert!(MessageHubConfigClient::connect("http://localhost:1400")
            .await
            .is_err());
    }
}
