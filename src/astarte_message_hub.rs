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

use std::collections::HashMap;
use std::sync::Arc;

use log::info;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::data::astarte::{AstartePublisher, AstarteSubscriber};
use crate::proto_message_hub::message_hub_server::MessageHub;
use crate::proto_message_hub::{AstarteMessage, Node};
use crate::types::InterfaceJson;

pub struct AstarteMessageHub<T: AstartePublisher + AstarteSubscriber> {
    nodes: Arc<RwLock<HashMap<Uuid, AstarteNode>>>,
    astarte_handler: T,
}

pub struct AstarteNode {
    pub id: Uuid,
    pub introspection: Vec<InterfaceJson>,
}

impl AstarteNode {
    pub fn new(uuid: Uuid, introspection: Vec<Vec<u8>>) -> Self {
        AstarteNode {
            id: uuid,
            introspection: introspection.into_iter().map(InterfaceJson).collect(),
        }
    }
}

impl<T> AstarteMessageHub<T>
where
    T: AstartePublisher + AstarteSubscriber,
{
    pub fn new(astarte_handler: T) -> Self {
        AstarteMessageHub {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            astarte_handler,
        }
    }
}

#[tonic::async_trait]
impl<T: AstartePublisher + AstarteSubscriber + 'static> MessageHub for AstarteMessageHub<T> {
    type AttachStream = ReceiverStream<Result<AstarteMessage, Status>>;

    async fn attach(&self, request: Request<Node>) -> Result<Response<Self::AttachStream>, Status> {
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

    async fn send(
        &self,
        request: Request<AstarteMessage>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        info!("Node Send Request => {:?}", request);

        let astarte_message = request.into_inner();

        if let Err(err) = self.astarte_handler.publish(&astarte_message).await {
            Err(Status::internal(format!(
                "Unable to publish astarte message, err: {:?}",
                err
            )))
        } else {
            Ok(Response::new(pbjson_types::Empty {}))
        }
    }

    async fn detach(
        &self,
        _request: Request<Node>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::io::Error;

    use async_trait::async_trait;
    use mockall::mock;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;
    use tonic::{Request, Status};

    use crate::astarte_message_hub::AstarteNode;
    use crate::data::astarte::{AstartePublisher, AstarteSubscriber};
    use crate::error::AstarteMessageHubError;
    use crate::error::AstarteMessageHubError::AstarteInvalidData;
    use crate::proto_message_hub::AstarteMessage;
    use crate::AstarteMessageHub;

    mock! {
        Astarte { }

        #[async_trait]
        impl AstartePublisher for Astarte {
            async fn publish(&self, data: &AstarteMessage) -> Result<(), AstarteMessageHubError>;
        }

        #[async_trait]
        impl AstarteSubscriber for Astarte {
            async fn subscribe(&self, astarte_node: &AstarteNode,
            ) -> Result<Receiver<Result<AstarteMessage, Status>>, AstarteMessageHubError>;

            async fn unsubscribe(&self, astarte_node: &AstarteNode) -> Result<(), Error>;
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

        let mut mock_astarte = MockAstarte::new();
        mock_astarte.expect_subscribe().returning(|_| {
            let (_, rx) = mpsc::channel(2);
            Ok(rx)
        });

        let astarte_message: AstarteMessageHub<MockAstarte> = AstarteMessageHub::new(mock_astarte);

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

        let mock_astarte = MockAstarte::new();
        let astarte_message: AstarteMessageHub<MockAstarte> = AstarteMessageHub::new(mock_astarte);

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

        let mut mock_astarte = MockAstarte::new();
        mock_astarte
            .expect_subscribe()
            .returning(|_| Err(AstarteInvalidData("interface not found".to_string())));

        let astarte_message: AstarteMessageHub<MockAstarte> = AstarteMessageHub::new(mock_astarte);

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

        let mut mock_astarte = MockAstarte::new();
        mock_astarte.expect_publish().returning(|_| Ok(()));

        let astarte_message_hub: AstarteMessageHub<MockAstarte> =
            AstarteMessageHub::new(mock_astarte);

        let interface_name = "io.demo.Values".to_owned();

        let astarte_message = AstarteMessage {
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
        use crate::error::AstarteMessageHubError::IoError;
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::message_hub_server::MessageHub;
        use std::io::ErrorKind::InvalidData;

        let mut mock_astarte = MockAstarte::new();
        mock_astarte
            .expect_publish()
            .returning(|_| Err(IoError(Error::new(InvalidData, "interface not found"))));

        let astarte_message_hub: AstarteMessageHub<MockAstarte> =
            AstarteMessageHub::new(mock_astarte);

        let interface_name = "io.demo.Values".to_owned();

        let value: i32 = 5;
        let astarte_message = AstarteMessage {
            interface_name,
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(value.into())),
            timestamp: None,
        };

        let req_astarte_message = Request::new(astarte_message);
        let send_result = astarte_message_hub.send(req_astarte_message).await;

        assert!(send_result.is_err());
        let err: Status = send_result.err().unwrap();
        assert_eq!("Unable to publish astarte message, err: IoError(Custom { kind: InvalidData, error: \"interface not found\" })", err.message())
    }
}
