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

use hyper::{http, Body, Uri};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use astarte_message_hub_proto::types::InterfaceJson;
use astarte_message_hub_proto::AstarteMessage;
use log::{debug, error, info, trace};
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::BoxBody;
use tonic::metadata::errors::InvalidMetadataValueBytes;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use tower::{Layer, Service};
use uuid::Uuid;

use crate::astarte::{AstartePublisher, AstarteSubscriber};

/// Main struct for the Astarte message hub.
pub struct AstarteMessageHub<T: Clone + AstartePublisher + AstarteSubscriber> {
    /// The nodes connected to the message hub.
    nodes: Arc<RwLock<HashMap<Uuid, AstarteNode>>>,
    /// The Astarte handler used to communicate with Astarte.
    astarte_handler: T,
}

/// A single node that can be connected to the Astarte message hub.
#[derive(Clone)]
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
    T: Clone + AstartePublisher + AstarteSubscriber,
{
    /// Instantiate a new Astarte message hub.
    ///
    /// The `astarte_handler` should satisfy the required traits for an Astarte handler.
    pub fn new(astarte_handler: T) -> Self {
        AstarteMessageHub {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            astarte_handler,
        }
    }

    /// Create a [tower] layer to intercept the Node ID in the gRPC requests
    pub fn make_interceptor_layer(&self) -> NodeIdInterceptorLayer {
        NodeIdInterceptorLayer::new(Arc::clone(&self.nodes))
    }
}

/// Errors related to the interception of the NodeId into gRPC request
#[derive(thiserror::Error, Debug, displaydoc::Display)]
pub enum InterceptorError {
    /// Node ID not specified in the gRPC request
    AbsentId,
    /// Invalid Uuid bytes value for Node ID, {0}
    Uuid(#[from] uuid::Error),
    /// Invalid Uuid str value for Node ID, {0}
    ParseStr(#[from] tonic::metadata::errors::ToStrError),
    /// The Node ID does not exist
    IdNotFound,
    /// Couldn't decode metadata binary value
    MetadataBytes(#[from] InvalidMetadataValueBytes),
    /// Invalid URI
    InvalidUri(#[from] http::uri::InvalidUri),
    /// Http error
    Http(#[from] http::Error),
}

impl From<InterceptorError> for Status {
    fn from(value: InterceptorError) -> Self {
        match value {
            InterceptorError::AbsentId | InterceptorError::IdNotFound => {
                Status::unauthenticated("node not authenticated")
            }
            err => Status::invalid_argument(format!("invalid argument: {err}")),
        }
    }
}

pub(crate) fn is_attach(uri: &Uri) -> bool {
    uri.path() == "/astarteplatform.msghub.MessageHub/Attach"
}

#[derive(Clone, Default)]
pub struct NodeIdInterceptorLayer {
    msg_hub_nodes: Arc<RwLock<HashMap<Uuid, AstarteNode>>>,
}

impl NodeIdInterceptorLayer {
    fn new(msg_hub_nodes: Arc<RwLock<HashMap<Uuid, AstarteNode>>>) -> NodeIdInterceptorLayer {
        Self { msg_hub_nodes }
    }
}

impl<S> Layer<S> for NodeIdInterceptorLayer {
    type Service = NodeIdInterceptor<S>;

    fn layer(&self, service: S) -> Self::Service {
        NodeIdInterceptor {
            inner: service,
            msg_hub_nodes: self.msg_hub_nodes.clone(),
        }
    }
}

#[derive(Clone)]
pub struct NodeIdInterceptor<S> {
    inner: S,
    msg_hub_nodes: Arc<RwLock<HashMap<Uuid, AstarteNode>>>,
}

impl<S> NodeIdInterceptor<S> {
    /// Check if the node ID is present in the request and if a node actually exists with that ID.
    ///
    /// This associated function does not contain a reference to `Self` since `NodeIdInterceptor`
    /// cannot implement the `Sync` trait (as services in tonic cannot be Sync). To circumvent this
    /// problem, we pass a reference to the message hub nodes in the input to perform the necessary
    /// checks.
    async fn check_node_id(
        nodes: &Arc<RwLock<HashMap<Uuid, AstarteNode>>>,
        req: hyper::Request<Body>,
    ) -> Result<hyper::Request<Body>, InterceptorError> {
        debug!("checking the NodeId in the request metadata");
        // use tonic::Metadata to access the metadata and avoid to manually perform a base64
        // conversion of the uuid.
        let (mut parts, body) = req.into_parts();
        let metadata = MetadataMap::from_headers(parts.headers);

        // retrieve the node id from the metadata
        let opt = node_id_from_bin(&metadata)?;
        let node_uuid = match opt {
            Some(node_uuid) => node_uuid,
            None => node_id_from_ascii(&metadata)?.ok_or(InterceptorError::AbsentId)?,
        };

        trace!("NodeId: {node_uuid}");

        if !nodes.read().await.contains_key(&node_uuid) {
            debug!("NodeId not found");
            return Err(InterceptorError::IdNotFound);
        }

        debug!("NodeId checked");

        // reconstruct the http::Request
        parts.headers = metadata.into_headers();
        parts.extensions.insert(NodeId(node_uuid));
        let req = http::Request::from_parts(parts, body);

        Ok(req)
    }
}

/// Node Id
#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub struct NodeId(Uuid);

impl Display for NodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Borrow<Uuid> for NodeId {
    fn borrow(&self) -> &Uuid {
        &self.0
    }
}

impl Deref for NodeId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn node_id_from_bin(metadata: &MetadataMap) -> Result<Option<Uuid>, InterceptorError> {
    let Some(node_id) = metadata.get_bin("node-id-bin") else {
        return Ok(None);
    };

    let uuid_bytes = node_id.to_bytes()?;

    Uuid::from_slice(&uuid_bytes)
        .map(Some)
        .map_err(InterceptorError::Uuid)
}

fn node_id_from_ascii(metadata: &MetadataMap) -> Result<Option<Uuid>, InterceptorError> {
    let Some(node_id) = metadata.get("node-id") else {
        return Ok(None);
    };

    let uuid_str = node_id.to_str()?;

    Uuid::from_str(uuid_str)
        .map(Some)
        .map_err(InterceptorError::Uuid)
}

impl<S> Service<hyper::Request<Body>> for NodeIdInterceptor<S>
where
    S: Service<hyper::Request<Body>, Response = hyper::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(S::Error::into)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
        let clone = self.clone();
        let mut service = std::mem::replace(self, clone);

        Box::pin(async move {
            // check that the Node ID is present inside the metadata on all gRPC requests apart from the
            // attach ones
            if is_attach(req.uri()) {
                return service.inner.call(req).await;
            }

            let checked_req = match Self::check_node_id(&service.msg_hub_nodes, req).await {
                Ok(req) => req,
                // return a response with the status code related to the given error
                Err(err) => {
                    return Ok(Status::from(err).to_http());
                }
            };

            service.inner.call(checked_req).await
        })
    }
}

/// Retrieve information from a [`Request`]
pub trait RequestExt {
    fn get_node_id(&self) -> Result<&NodeId, Status>;
}

impl<R> RequestExt for Request<R> {
    fn get_node_id(&self) -> Result<&NodeId, Status> {
        if let Some(node_id) = self.extensions().get::<NodeId>() {
            trace!("node id {node_id} checked");
            return Ok(node_id);
        }

        error!("missing node id");
        Err(Status::failed_precondition("missing node id"))
    }
}

#[tonic::async_trait]
impl<T: Clone + AstartePublisher + AstarteSubscriber + 'static>
    astarte_message_hub_proto::message_hub_server::MessageHub for AstarteMessageHub<T>
{
    type AttachStream = ReceiverStream<Result<AstarteMessage, Status>>;

    /// Attach a node to the Message hub. If the node was successfully attached,
    /// the method returns a gRPC stream into which the events received
    /// from Astarte(based on the declared Introspection) will be redirected.
    ///
    /// ```no_run
    /// use astarte_message_hub_proto::message_hub_client::MessageHubClient;
    /// use astarte_message_hub_proto::Node;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), tonic::Status> {
    ///     let mut message_hub_client = MessageHubClient::connect("http://[::1]:10000").await.unwrap();
    ///
    ///     let interface_json = tokio::fs::read("/tmp/org.astarteplatform.rust.examples.DeviceDatastream.json").await
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
        request: Request<astarte_message_hub_proto::Node>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        info!("Node Attach Request");
        let node = request.into_inner();

        let id = Uuid::parse_str(&node.uuid).map_err(|err| {
            Status::invalid_argument(format!(
                "Unable to parse UUID value, err {:?}",
                err.to_string()
            ))
        })?;

        debug!("Attaching node {id}");

        let astarte_node = AstarteNode::new(id, node.interface_jsons);

        let rx = match self.astarte_handler.subscribe(&astarte_node).await {
            Ok(rx) => rx,
            Err(err) => {
                return Err(Status::aborted(format!(
                    "Unable to subscribe, err: {:?}",
                    err
                )));
            }
        };

        let mut nodes = self.nodes.write().await;
        nodes.insert(astarte_node.id.to_owned(), astarte_node);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// Send a message to Astarte for a node attached to the Astarte Message Hub.
    ///
    /// ```no_run
    /// use astarte_message_hub_proto::message_hub_client::MessageHubClient;
    /// use astarte_message_hub_proto::Node;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), tonic::Status> {
    /// use astarte_message_hub_proto::astarte_message::Payload;
    /// use astarte_message_hub_proto::AstarteMessage;
    ///
    ///     let mut message_hub_client = MessageHubClient::connect("http://[::1]:10000").await.unwrap();
    ///
    ///     let interface_json = tokio::fs::read("/tmp/org.astarteplatform.rust.examples.DeviceDatastream.json").await
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
    ///     let _ = message_hub_client.send(astarte_message).await;
    ///
    ///     Ok(())
    ///
    /// }
    async fn send(
        &self,
        request: Request<AstarteMessage>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        // verify that the request contains a Node Id in the metadata
        let node_id = request.get_node_id()?;
        debug!("Node {node_id} Send Request");

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
        request: Request<astarte_message_hub_proto::Node>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        let node_id = request.get_node_id()?;
        info!("Node {node_id} Detach Request => {:?}", request);

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
    use std::collections::HashMap;
    use std::io::Error;
    use std::io::ErrorKind;
    use std::ops::Deref;
    use std::sync::Arc;

    use astarte_message_hub_proto::astarte_message::Payload;
    use astarte_message_hub_proto::message_hub_server::MessageHub;
    use astarte_message_hub_proto::AstarteMessage;
    use astarte_message_hub_proto::Node;
    use async_trait::async_trait;
    use hyper::{Body, Response, StatusCode};
    use mockall::mock;
    use tokio::sync::mpsc::Receiver;
    use tokio::sync::{mpsc, RwLock};
    use tonic::body::BoxBody;
    use tonic::metadata::{MetadataMap, MetadataValue};
    use tonic::{Code, Request, Status};
    use tower::{Service, ServiceBuilder};
    use uuid::Uuid;

    use crate::astarte::{AstartePublisher, AstarteSubscriber};
    use crate::error::AstarteMessageHubError;
    use crate::server::{
        node_id_from_ascii, node_id_from_bin, AstarteNode, InterceptorError, NodeId,
        NodeIdInterceptorLayer,
    };

    use super::AstarteMessageHub;

    const TEST_UUID: uuid::Uuid = uuid::uuid!("d1e7a6e9-cf99-4694-8fb6-997934be079c");

    mock! {
        AstarteHandler { }

        impl Clone for AstarteHandler {
            fn clone(&self) -> Self;
        }

        #[async_trait]
        impl AstartePublisher for AstarteHandler {
            async fn publish(
                &self,
                data: &AstarteMessage
            ) -> Result<(), AstarteMessageHubError>;
        }

        #[async_trait]
        impl AstarteSubscriber for AstarteHandler {
            async fn subscribe(
                &self,
                astarte_node: &AstarteNode,
            ) -> Result<Receiver<Result<AstarteMessage, Status>>, AstarteMessageHubError>;

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
            "Unable to subscribe, err: AstarteInvalidData(\"interface not found\")",
            err.message()
        )
    }

    #[tokio::test]
    async fn send_message_success() {
        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte.expect_publish().returning(|_| Ok(()));
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let astarte_message_hub: AstarteMessageHub<MockAstarteHandler> =
            AstarteMessageHub::new(mock_astarte);

        let interface_name = "io.demo.Values".to_owned();

        let astarte_message = AstarteMessage {
            interface_name,
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(5.into())),
            timestamp: None,
        };

        let mut req_astarte_message = Request::new(astarte_message);
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_astarte_message
            .extensions_mut()
            .insert(NodeId(TEST_UUID));

        let send_result = astarte_message_hub.send(req_astarte_message).await;

        assert!(send_result.is_ok())
    }

    #[tokio::test]
    async fn send_message_reject() {
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

        let astarte_message = AstarteMessage {
            interface_name,
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(value.into())),
            timestamp: None,
        };

        let mut req_astarte_message = Request::new(astarte_message);
        req_astarte_message
            .extensions_mut()
            .insert(NodeId(TEST_UUID));
        let send_result = astarte_message_hub.send(req_astarte_message).await;

        assert!(send_result.is_err());
        let err: Status = send_result.err().unwrap();
        assert_eq!("Unable to publish astarte message, err: IOError(Custom { kind: InvalidData, error: \"interface not found\" })", err.message())
    }

    #[tokio::test]
    async fn detach_node_success() {
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

        let mut req_node_attach = Request::new(node.clone());
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_node_attach.extensions_mut().insert(NodeId(TEST_UUID));
        assert!(astarte_message.attach(req_node_attach).await.is_ok());
        let mut req_node_detach = Request::new(node);
        req_node_detach.extensions_mut().insert(NodeId(TEST_UUID));
        let detach_result = astarte_message.detach(req_node_detach).await;

        assert!(detach_result.is_ok())
    }

    #[tokio::test]
    async fn detach_failed_invalid_uuid_node() {
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

        let mut req_node = Request::new(node_introspection);
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_node.extensions_mut().insert(NodeId(TEST_UUID));

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

        let node = Node {
            uuid: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
            interface_jsons: interfaces,
        };

        let mut req_node = Request::new(node);
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_node.extensions_mut().insert(NodeId(TEST_UUID));

        let detach_result = astarte_message.detach(req_node).await;

        assert!(detach_result.is_err());
        let err: Status = detach_result.err().unwrap();
        assert_eq!("Unable to find AstarteNode", err.message())
    }

    #[tokio::test]
    async fn detach_node_unsubscribe_failed() {
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

        let astarte_message = AstarteMessageHub::new(mock_astarte);

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let node = Node {
            uuid: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
            interface_jsons: interfaces,
        };

        let mut req_node_attach = Request::new(node.clone());
        req_node_attach.extensions_mut().insert(NodeId(TEST_UUID));
        assert!(astarte_message.attach(req_node_attach).await.is_ok());

        let mut req_node_detach = Request::new(node);
        req_node_detach.extensions_mut().insert(NodeId(TEST_UUID));
        let detach_result = astarte_message.detach(req_node_detach).await;

        assert!(detach_result.is_err());
        let err: Status = detach_result.err().unwrap();
        assert_eq!(
            "Unable to unsubscribe, err: AstarteInvalidData(\"invalid node\")",
            err.message()
        )
    }

    #[test]
    fn test_node_id() {
        let node_id = NodeId(TEST_UUID);

        // test Deref
        assert_eq!(node_id.deref(), &TEST_UUID);

        // test Display
        let display = format!("{node_id}");
        assert_eq!(display, TEST_UUID.to_string());
    }

    #[test]
    fn test_node_id_from_bin() {
        let mut metadata_map = MetadataMap::new();

        // empty node-id-bin field
        let res = node_id_from_bin(&metadata_map).unwrap();
        assert!(res.is_none());

        // insert a wrong uuid
        let metadata_val = MetadataValue::from_bytes(b"wrong-uuid");
        metadata_map.insert_bin("node-id-bin", metadata_val);

        let res = node_id_from_bin(&metadata_map);
        assert!(res.is_err());

        // correct
        let metadata_val = MetadataValue::from_bytes(TEST_UUID.as_bytes());
        metadata_map.insert_bin("node-id-bin", metadata_val);
        let res = node_id_from_bin(&metadata_map).unwrap();
        assert!(res.is_some());
    }

    #[test]
    fn test_node_id_from_ascii() {
        let mut metadata_map = MetadataMap::new();

        // empty node-id field
        let res = node_id_from_ascii(&metadata_map).unwrap();
        assert!(res.is_none());

        // insert a wrong uuid
        let metadata_val = MetadataValue::from_static("wrong-uuid");
        metadata_map.insert("node-id", metadata_val);
        let res = node_id_from_ascii(&metadata_map);
        assert!(res.is_err());

        // correct
        let metadata_val = MetadataValue::from_static("d1e7a6e9-cf99-4694-8fb6-997934be079c");
        metadata_map.insert("node-id", metadata_val);
        let res = node_id_from_ascii(&metadata_map).unwrap();
        assert!(res.is_some());
    }

    fn create_http_req(uri: &str) -> hyper::Request<Body> {
        hyper::Request::builder()
            .uri(uri)
            .body(Body::default())
            .expect("failed to create http req")
    }

    fn create_http_req_with_metadata_node_id(uri: &str) -> hyper::Request<Body> {
        let req = hyper::Request::builder()
            .uri(uri)
            .body(Body::default())
            .expect("failed to create http req");

        let (mut parts, body) = req.into_parts();
        let mut metadata = MetadataMap::from_headers(parts.headers);
        let metadata_val = MetadataValue::from_bytes(TEST_UUID.as_bytes());
        metadata.insert_bin("node-id-bin", metadata_val);
        parts.headers = metadata.into_headers();

        hyper::Request::from_parts(parts, body)
    }

    fn create_node_interceptor_service(
        hm: HashMap<Uuid, AstarteNode>,
    ) -> impl Service<hyper::Request<Body>, Response = hyper::Response<BoxBody>, Error = InterceptorError>
    {
        let msg_hub_nodes = Arc::new(RwLock::new(hm));

        ServiceBuilder::new()
            .layer(NodeIdInterceptorLayer::new(msg_hub_nodes))
            .service_fn(|_req: hyper::Request<Body>| {
                futures::future::ready(
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(BoxBody::default())
                        .map_err(InterceptorError::Http),
                )
            })
    }

    #[tokio::test]
    async fn test_node_id_interceptor() {
        // don't check node id if the request is an Attach
        let req =
            create_http_req("http://localhost:50051/astarteplatform.msghub.MessageHub/Attach");

        let mut service = create_node_interceptor_service(HashMap::default());

        let res = service.call(req).await;

        assert!(res.is_ok());

        // check node id for non-attach requests
        // CASE: node id metadata not present
        let req = create_http_req("http://localhost:50051/");

        let res = service.call(req).await;
        let status = res
            .ok()
            .unwrap()
            .headers_mut()
            .remove("grpc-status")
            .unwrap();
        let status = Code::from_bytes(status.as_bytes());

        assert_eq!(status, Code::Unauthenticated);

        // CASE: node-id-bin present but not inserted in the Message Hub Node lists
        let req = create_http_req_with_metadata_node_id("http://localhost:50051/");

        let res = service.call(req).await;
        let status = res
            .ok()
            .unwrap()
            .headers_mut()
            .remove("grpc-status")
            .unwrap();
        let status = Code::from_bytes(status.as_bytes());

        assert_eq!(status, Code::Unauthenticated);

        // CASE: node-id-bin present but not inserted in the Message Hub Node lists
        let req = create_http_req_with_metadata_node_id("http://localhost:50051/");

        let msg_hub_nodes = HashMap::from([(TEST_UUID, AstarteNode::new(TEST_UUID, vec![]))]);

        let mut service = create_node_interceptor_service(msg_hub_nodes);

        let res = service.call(req).await;

        assert!(res.is_ok());
    }
}
