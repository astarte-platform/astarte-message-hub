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
//! Contains the implementation for the Astarte message hub.

use std::collections::HashMap;
use std::sync::Arc;

use log::info;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::data::astarte::{AstartePublisher, AstarteRunner, AstarteSubscriber};
use crate::proto_message_hub;
use crate::types::InterfaceJson;

/// Main struct for the Astarte message hub.
pub struct AstarteMessageHub<T: Clone + AstarteRunner + AstartePublisher + AstarteSubscriber> {
    /// The nodes connected to the message hub.
    nodes: Arc<RwLock<HashMap<Uuid, AstarteNode>>>,
    /// The Astarte handler used to communicate with Astarte.
    astarte_handler: T,
}

/// A single node that can be connected to the Astarte message hub.
pub struct AstarteNode {
    /// Identifier for the node
    pub id: Uuid,
    /// A vector of interfaces for this node.
    pub introspection: Vec<InterfaceJson>,
}

impl AstarteNode {
    /// Instantiate a new node.
    pub fn new(uuid: Uuid, introspection: Vec<Vec<u8>>) -> Self {
        AstarteNode {
            id: uuid,
            introspection: introspection.into_iter().map(InterfaceJson).collect(),
        }
    }
}

impl<T: 'static> AstarteMessageHub<T>
where
    T: Clone + AstarteRunner + AstartePublisher + AstarteSubscriber,
{
    /// Instantiate a new Astarte message hub.
    ///
    /// The `astarte_handler` should satisfy the required traits for an Astarte handler.
    /// See the [AstarteHandler](crate::data::astarte_handler::AstarteHandler) for a ready-to-use Astarte
    /// handler.
    pub fn new(astarte_handler: T) -> Self {
        let mut astarte_handler_cpy = astarte_handler.clone();
        tokio::task::spawn(async move {
            loop {
                astarte_handler_cpy.run().await;
            }
        });

        AstarteMessageHub {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            astarte_handler,
        }
    }
}

#[tonic::async_trait]
impl<T: Clone + AstarteRunner + AstartePublisher + AstarteSubscriber + 'static>
    proto_message_hub::message_hub_server::MessageHub for AstarteMessageHub<T>
{
    type AttachStream = ReceiverStream<Result<proto_message_hub::AstarteMessage, Status>>;

    /// Attach a node to the Message hub. If the node was successfully attached,
    /// the method returns a gRPC stream into which the events received
    /// from Astarte(based on the declared Introspection) will be redirected.
    ///
    /// ```no_run
    /// use astarte_message_hub::proto_message_hub::message_hub_client::MessageHubClient;
    /// use astarte_message_hub::proto_message_hub::Node;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), tonic::Status> {
    ///     let mut message_hub_client = MessageHubClient::connect("http://[::1]:10000").await.unwrap();
    ///
    ///     let interface_json = std::fs::read("/tmp/org.astarteplatform.rust.examples.DeviceDatastream.json")
    ///         .unwrap();
    ///
    ///     let node = Node {
    ///             uuid: "a2d4769f-0338-4f7f-b71d-9f81b41ae13f".to_string(),
    ///             interface_jsons: vec![interface_json],
    ///     };
    ///
    ///     let mut stream = message_hub_client
    ///         .attach(tonic::Request::new(node))
    ///         .await?
    ///         .into_inner();
    ///
    ///     loop {
    ///         if let Some(astarte_message) = stream.message().await?{
    ///             println!("AstarteMessage = {:?}", astarte_message);
    ///         }
    ///     }
    /// }
    /// ```
    async fn attach(
        &self,
        request: Request<proto_message_hub::Node>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        info!("Node Attach Request => {:?}", request);
        let node = request.into_inner();

        let id = Uuid::parse_str(&node.uuid).map_err(|err| {
            Status::invalid_argument(format!(
                "Unable to parse UUID value, err {:?}",
                err.to_string()
            ))
        })?;

        let astarte_node = AstarteNode::new(id, node.interface_jsons);
        let subscribe_result = self.astarte_handler.subscribe(&astarte_node).await;

        if let Ok(rx) = subscribe_result {
            let mut nodes = self.nodes.write().await;
            nodes.insert(astarte_node.id.to_owned(), astarte_node);
            Ok(Response::new(ReceiverStream::new(rx)))
        } else {
            Err(Status::aborted(format!(
                "Unable to subscribe, err: {:?}",
                subscribe_result.err()
            )))
        }
    }

    /// Send a message to Astarte for a node attached to the Astarte Message Hub.
    ///
    /// ```no_run
    /// use astarte_message_hub::proto_message_hub::message_hub_client::MessageHubClient;
    /// use astarte_message_hub::proto_message_hub::Node;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), tonic::Status> {
    /// use astarte_message_hub::proto_message_hub::astarte_message::Payload;
    /// use astarte_message_hub::proto_message_hub::AstarteMessage;
    ///
    ///     let mut message_hub_client = MessageHubClient::connect("http://[::1]:10000").await.unwrap();
    ///
    ///     let interface_json = std::fs::read("/tmp/org.astarteplatform.rust.examples.DeviceDatastream.json")
    ///         .unwrap();
    ///
    ///     let node = Node {
    ///             uuid: "a2d4769f-0338-4f7f-b71d-9f81b41ae13f".to_string(),
    ///             interface_jsons: vec![interface_json],
    ///     };
    ///
    ///     let stream = message_hub_client
    ///         .attach(tonic::Request::new(node))
    ///         .await?
    ///         .into_inner();
    ///
    ///     let astarte_message = AstarteMessage {
    ///         interface_name: "org.astarteplatform.esp32.examples.DeviceDatastream".to_string(),
    ///         path: "uptimeSeconds".to_string(),
    ///         timestamp: None,
    ///         payload: Some(Payload::AstarteData(100.into()))
    ///     };
    ///
    ///     let  _ = message_hub_client.send(astarte_message).await;
    ///
    ///     Ok(())
    ///
    /// }
    async fn send(
        &self,
        request: Request<proto_message_hub::AstarteMessage>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        info!("Node Send Request => {:?}", request);

        let astarte_message = request.into_inner();

        if let Err(err) = self.astarte_handler.publish(&astarte_message).await {
            let err_msg = format!("Unable to publish astarte message, err: {:?}", err);
            Err(Status::internal(err_msg))
        } else {
            Ok(Response::new(pbjson_types::Empty {}))
        }
    }

    /// Remove an existing Node from Astarte Message Hub.
    async fn detach(
        &self,
        request: Request<proto_message_hub::Node>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        info!("Node Detach Request => {:?}", request);
        let node = request.into_inner();

        let id = Uuid::parse_str(&node.uuid).map_err(|err| {
            let err_msg = format!("Unable to parse UUID value, err {:?}", err);
            Status::invalid_argument(err_msg)
        })?;

        let mut nodes = self.nodes.write().await;

        if let Some(astarte_node) = nodes.remove(&id) {
            if let Err(err) = self.astarte_handler.unsubscribe(&astarte_node).await {
                let err_msg = format!("Unable to unsubscribe, err: {err:?}");
                Err(Status::internal(err_msg))
            } else {
                Ok(Response::new(pbjson_types::Empty {}))
            }
        } else {
            Err(Status::internal("Unable to find AstarteNode"))
        }
    }
}

#[cfg(test)]
mod test {
    use super::AstarteMessageHub;

    use std::io::Error;
    use std::io::ErrorKind;

    use async_trait::async_trait;
    use mockall::mock;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;
    use tonic::{Request, Status};

    use crate::astarte_message_hub::AstarteNode;
    use crate::data::astarte::{AstartePublisher, AstarteRunner, AstarteSubscriber};
    use crate::error::AstarteMessageHubError;
    use crate::proto_message_hub;

    mock! {
        AstarteHandler { }

        impl Clone for AstarteHandler {
            fn clone(&self) -> Self;
        }

        #[async_trait]
        impl AstarteRunner for AstarteHandler {
            async fn run(&mut self);
        }

        #[async_trait]
        impl AstartePublisher for AstarteHandler {
            async fn publish(
                &self,
                data: &proto_message_hub::AstarteMessage
            ) -> Result<(), AstarteMessageHubError>;
        }

        #[async_trait]
        impl AstarteSubscriber for AstarteHandler {
            async fn subscribe(
                &self,
                astarte_node: &AstarteNode,
            ) -> Result<Receiver<Result<proto_message_hub::AstarteMessage, Status>>, AstarteMessageHubError>;

            async fn unsubscribe(&self, astarte_node: &AstarteNode) -> Result<(), AstarteMessageHubError>;
        }
    }

    const SERV_OBJ_IFACE: &str = r#"
        {
            "interface_name": "com.test.object",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "server",
            "aggregation": "object",
            "mappings": [
                {
                    "endpoint": "/button",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/uptimeSeconds",
                    "type": "integer",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;

    const SERV_PROPS_IFACE: &str = r#"
        {
            "interface_name": "org.astarte-platform.test.test",
            "version_major": 12,
            "version_minor": 1,
            "type": "properties",
            "ownership": "server",
            "mappings": [
                {
                    "endpoint": "/button",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/uptimeSeconds",
                    "type": "integer",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;

    #[tokio::test]
    async fn attach_success_node() {
        use crate::proto_message_hub::message_hub_server::MessageHub;
        use crate::proto_message_hub::Node;

        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte.expect_subscribe().returning(|_| {
            let (_, rx) = mpsc::channel(2);
            Ok(rx)
        });
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let astarte_message: AstarteMessageHub<MockAstarteHandler> =
            AstarteMessageHub::new(mock_astarte);

        let interfaces = vec![
            SERV_PROPS_IFACE.to_string().into_bytes(),
            SERV_OBJ_IFACE.to_string().into_bytes(),
        ];

        let node_introspection = Node {
            uuid: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
            interface_jsons: interfaces,
        };

        let req_node = Request::new(node_introspection);
        let attach_result = astarte_message.attach(req_node).await;

        assert!(attach_result.is_ok())
    }

    #[tokio::test]
    async fn attach_reject_invalid_uuid_node() {
        use crate::proto_message_hub::message_hub_server::MessageHub;
        use crate::proto_message_hub::Node;

        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let astarte_message: AstarteMessageHub<MockAstarteHandler> =
            AstarteMessageHub::new(mock_astarte);

        let node_introspection = Node {
            uuid: "a1".to_owned(),
            interface_jsons: vec![],
        };

        let req_node = Request::new(node_introspection);
        let attach_result = astarte_message.attach(req_node).await;

        assert!(attach_result.is_err());
        let err: Status = attach_result.err().unwrap();
        assert_eq!("Unable to parse UUID value, err \"invalid length: expected length 32 for simple format, found 2\"", err.message())
    }

    #[tokio::test]
    async fn attach_reject_node() {
        use crate::proto_message_hub::message_hub_server::MessageHub;
        use crate::proto_message_hub::Node;

        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte.expect_subscribe().returning(|_| {
            Err(AstarteMessageHubError::AstarteInvalidData(
                "interface not found".to_string(),
            ))
        });
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let astarte_message: AstarteMessageHub<MockAstarteHandler> =
            AstarteMessageHub::new(mock_astarte);

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let node_introspection = Node {
            uuid: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
            interface_jsons: interfaces,
        };

        let req_node = Request::new(node_introspection);
        let attach_result = astarte_message.attach(req_node).await;

        assert!(attach_result.is_err());
        let err: Status = attach_result.err().unwrap();
        assert_eq!(
            "Unable to subscribe, err: Some(AstarteInvalidData(\"interface not found\"))",
            err.message()
        )
    }

    #[tokio::test]
    async fn send_message_success() {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::message_hub_server::MessageHub;

        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte.expect_publish().returning(|_| Ok(()));
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let astarte_message_hub: AstarteMessageHub<MockAstarteHandler> =
            AstarteMessageHub::new(mock_astarte);

        let interface_name = "io.demo.Values".to_owned();

        let astarte_message = proto_message_hub::AstarteMessage {
            interface_name,
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(5.into())),
            timestamp: None,
        };

        let req_astarte_message = Request::new(astarte_message);
        let send_result = astarte_message_hub.send(req_astarte_message).await;

        assert!(send_result.is_ok())
    }

    #[tokio::test]
    async fn send_message_reject() {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::message_hub_server::MessageHub;

        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte.expect_publish().returning(|_| {
            Err(AstarteMessageHubError::IOError(Error::new(
                ErrorKind::InvalidData,
                "interface not found",
            )))
        });
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let astarte_message_hub: AstarteMessageHub<MockAstarteHandler> =
            AstarteMessageHub::new(mock_astarte);

        let interface_name = "io.demo.Values".to_owned();

        let value: i32 = 5;
        let astarte_message = proto_message_hub::AstarteMessage {
            interface_name,
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(value.into())),
            timestamp: None,
        };

        let req_astarte_message = Request::new(astarte_message);
        let send_result = astarte_message_hub.send(req_astarte_message).await;

        assert!(send_result.is_err());
        let err: Status = send_result.err().unwrap();
        assert_eq!("Unable to publish astarte message, err: IOError(Custom { kind: InvalidData, error: \"interface not found\" })", err.message())
    }

    #[tokio::test]
    async fn detach_node_success() {
        use crate::proto_message_hub::message_hub_server::MessageHub;
        use crate::proto_message_hub::Node;

        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        mock_astarte.expect_subscribe().returning(|_| {
            let (_, rx) = mpsc::channel(2);
            Ok(rx)
        });
        mock_astarte.expect_unsubscribe().returning(|_| Ok(()));
        let astarte_message: AstarteMessageHub<MockAstarteHandler> =
            AstarteMessageHub::new(mock_astarte);

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let node = Node {
            uuid: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
            interface_jsons: interfaces,
        };

        let req_node_attach = Request::new(node.clone());
        assert!(astarte_message.attach(req_node_attach).await.is_ok());
        let req_node_detach = Request::new(node);
        let detach_result = astarte_message.detach(req_node_detach).await;

        assert!(detach_result.is_ok())
    }

    #[tokio::test]
    async fn detach_failed_invalid_uuid_node() {
        use crate::proto_message_hub::message_hub_server::MessageHub;
        use crate::proto_message_hub::Node;

        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let astarte_message: AstarteMessageHub<MockAstarteHandler> =
            AstarteMessageHub::new(mock_astarte);

        let node_introspection = Node {
            uuid: "a1".to_owned(),
            interface_jsons: vec![],
        };

        let req_node = Request::new(node_introspection);
        let detach_result = astarte_message.detach(req_node).await;

        assert!(detach_result.is_err());
        let err: Status = detach_result.err().unwrap();
        assert_eq!(
            "Unable to parse UUID value, err Error(SimpleLength { len: 2 })",
            err.message()
        )
    }

    #[tokio::test]
    async fn detach_node_not_found() {
        use crate::proto_message_hub::message_hub_server::MessageHub;

        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte.expect_unsubscribe().returning(|_| {
            Err(AstarteMessageHubError::AstarteInvalidData(
                "invalid node".to_string(),
            ))
        });
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let astarte_message: AstarteMessageHub<MockAstarteHandler> =
            AstarteMessageHub::new(mock_astarte);

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let node = proto_message_hub::Node {
            uuid: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
            interface_jsons: interfaces,
        };

        let req_node = Request::new(node);
        let detach_result = astarte_message.detach(req_node).await;

        assert!(detach_result.is_err());
        let err: Status = detach_result.err().unwrap();
        assert_eq!("Unable to find AstarteNode", err.message())
    }

    #[tokio::test]
    async fn detach_node_unsubscribe_failed() {
        use crate::proto_message_hub::message_hub_server::MessageHub;
        use crate::proto_message_hub::Node;

        let mut mock_astarte = MockAstarteHandler::new();

        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        mock_astarte.expect_subscribe().returning(|_| {
            let (_, rx) = mpsc::channel(2);
            Ok(rx)
        });
        mock_astarte.expect_unsubscribe().returning(|_| {
            Err(AstarteMessageHubError::AstarteInvalidData(
                "invalid node".to_string(),
            ))
        });

        let astarte_message: AstarteMessageHub<MockAstarteHandler> =
            AstarteMessageHub::new(mock_astarte);

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let node = Node {
            uuid: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
            interface_jsons: interfaces,
        };

        let req_node_attach = Request::new(node.clone());
        assert!(astarte_message.attach(req_node_attach).await.is_ok());
        let req_node_detach = Request::new(node);
        let detach_result = astarte_message.detach(req_node_detach).await;

        assert!(detach_result.is_err());
        let err: Status = detach_result.err().unwrap();
        assert_eq!(
            "Unable to unsubscribe, err: AstarteInvalidData(\"invalid node\")",
            err.message()
        )
    }
}
