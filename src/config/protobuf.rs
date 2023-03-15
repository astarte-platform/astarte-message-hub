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
use std::path::Path;

use tokio::sync::mpsc::{channel, Sender};
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use crate::config::MessageHubOptions;
use crate::proto_message_hub::message_hub_config_server::{
    MessageHubConfig, MessageHubConfigServer,
};
use crate::proto_message_hub::ConfigMessage;

#[derive(Debug)]
struct AstarteMessageHubConfig {
    store_directory: String,
    configuration_ready_channel: Sender<()>,
}

pub struct ProtobufConfigProvider {
    store_directory: String,
    shutdown_channel: Sender<()>,
}

#[tonic::async_trait]
impl MessageHubConfig for AstarteMessageHubConfig {
    /// Protobuf API that allows to set The Message Hub configurations
    async fn set_config(
        &self,
        request: Request<ConfigMessage>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        let req = request.into_inner();
        let message_hub_options = MessageHubOptions {
            realm: Some(req.realm),
            device_id: Some(req.device_id),
            credentials_secret: req.credentials_secret,
            pairing_url: Some(req.pairing_url),
            pairing_token: req.pairing_token,
            interfaces_directory: "".to_string(),
            store_directory: "".to_string(),
            astarte_ignore_ssl: None,
        };

        let mut file = std::fs::File::create(
            Path::new(&self.store_directory).join("message-hub-config.toml"),
        )?;

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
        store_directory: String,
        configuration_ready_channel: Sender<()>,
    ) -> ProtobufConfigProvider {
        let addr = address.parse().unwrap();
        let service = AstarteMessageHubConfig {
            store_directory: store_directory.clone(),
            configuration_ready_channel,
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
            store_directory,
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
    use tokio::sync::mpsc;
    use tonic::transport::Endpoint;
    use tonic::Request;

    use crate::config::protobuf::{AstarteMessageHubConfig, ProtobufConfigProvider};
    use crate::proto_message_hub::message_hub_config_client::MessageHubConfigClient;
    use crate::proto_message_hub::message_hub_config_server::MessageHubConfig;
    use crate::proto_message_hub::ConfigMessage;

    #[tokio::test]
    async fn set_config_test() {
        let (tx, _) = mpsc::channel(1);
        let config_server = AstarteMessageHubConfig {
            store_directory: "./".to_string(),
            configuration_ready_channel: tx,
        };
        let msg = ConfigMessage {
            realm: "rpc_realm".to_string(),
            device_id: "rpc_device_id".to_string(),
            credentials_secret: None,
            pairing_url: "rpc_pairing_url".to_string(),
            pairing_token: Some("rpc_pairing_token".to_string()),
        };
        let result = config_server.set_config(Request::new(msg)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn server_test() {
        pub mod proto_message_hub {
            tonic::include_proto!("astarteplatform.msghub");
        }
        let (tx, mut rx) = mpsc::channel(1);
        let server = ProtobufConfigProvider::new("127.0.0.1:1400", "./".to_string(), tx).await;
        let channel = Endpoint::from_static("http://localhost:1400")
            .connect()
            .await
            .unwrap();
        let mut client = MessageHubConfigClient::new(channel);
        let msg = ConfigMessage {
            realm: "rpc_realm".to_string(),
            device_id: "rpc_device_id".to_string(),
            credentials_secret: None,
            pairing_url: "rpc_pairing_url".to_string(),
            pairing_token: Some("rpc_pairing_token".to_string()),
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
