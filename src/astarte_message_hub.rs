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

use crate::data::astarte::{AstartePublisher, AstarteSubscriber};
use uuid::Uuid;

use crate::proto_message_hub::message_hub_server::MessageHub;
use crate::proto_message_hub::{AstarteMessage, Interface, Node};

pub struct AstarteMessageHub<T: AstartePublisher + AstarteSubscriber> {
    nodes: Arc<RwLock<HashMap<Uuid, AstarteNode>>>,
    astarte_handler: T,
}

pub struct AstarteNode {
    pub id: Uuid,
    pub introspection: Vec<Interface>,
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

        let astarte_node = AstarteNode {
            id,
            introspection: node.introspection,
        };

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
        _request: Request<AstarteMessage>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        todo!()
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
    use async_trait::async_trait;
    use mockall::mock;
    use std::io::{Error, ErrorKind};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;
    use tonic::{Request, Response, Status};

    use crate::astarte_message_hub::AstarteNode;
    use crate::data::astarte::{AstartePublisher, AstarteSubscriber};
    use crate::proto_message_hub::AstarteMessage;
    use crate::AstarteMessageHub;

    mock! {
        Astarte { }

        #[async_trait]
        impl AstartePublisher for Astarte {
            async fn publish(&self, data: AstarteMessage) -> Result<(), Error>;
        }

        #[async_trait]
        impl AstarteSubscriber for Astarte {
            async fn subscribe(&self, astarte_node: &AstarteNode,
            ) -> Result<Receiver<Result<AstarteMessage, Status>>, Error>;

            async fn unsubscribe(&self, astarte_node: &AstarteNode) -> Result<(), Error>;
        }
    }

    #[tokio::test]
    async fn attach_success_node() {
        use crate::proto_message_hub::message_hub_server::MessageHub;
        use crate::proto_message_hub::Interface;
        use crate::proto_message_hub::Node;

        let mut mock_astarte = MockAstarte::new();
        mock_astarte.expect_subscribe().returning(|_| {
            let (_, rx) = mpsc::channel(2);
            Ok(rx)
        });
        let astarte_message: AstarteMessageHub<MockAstarte> = AstarteMessageHub::new(mock_astarte);

        let interfaces = vec![
            Interface {
                name: "io.demo.ServerProperties".to_owned(),
                minor: 0,
                major: 2,
            },
            Interface {
                name: "org.astarteplatform.esp32.DeviceDatastream".to_owned(),
                minor: 0,
                major: 2,
            },
        ];

        let node_introspection = Node {
            uuid: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
            introspection: interfaces,
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
            introspection: vec![],
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
        use crate::proto_message_hub::Interface;
        use crate::proto_message_hub::Node;

        let mut mock_astarte = MockAstarte::new();
        mock_astarte
            .expect_subscribe()
            .returning(|_| Err(Error::new(ErrorKind::InvalidInput, "interface not found")));

        let astarte_message: AstarteMessageHub<MockAstarte> = AstarteMessageHub::new(mock_astarte);

        let interfaces = vec![Interface {
            name: "io.demo.ServerProperties".to_owned(),
            minor: 0,
            major: 2,
        }];

        let node_introspection = Node {
            uuid: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
            introspection: interfaces,
        };

        let req_node = Request::new(node_introspection);
        let attach_result = astarte_message.attach(req_node).await;

        assert!(attach_result.is_err());
        let err: Status = attach_result.err().unwrap();
        assert_eq!("Unable to subscribe, err: Some(Custom { kind: InvalidInput, error: \"interface not found\" })", err.message())
    }
}
