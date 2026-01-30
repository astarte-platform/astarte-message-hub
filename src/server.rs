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

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use astarte_interfaces::Interface;
use astarte_message_hub_proto::message_hub_server::MessageHub;
use astarte_message_hub_proto::{
    AstarteMessage, AstartePropertyIndividual, InterfaceName, InterfacesJson, InterfacesName,
    MessageHubEvent, Node, PropertyFilter, PropertyIdentifier, StoredProperties,
};
use axum::http::{self, Uri};
use itertools::Itertools;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataMap;
use tonic::metadata::errors::InvalidMetadataValueBytes;
use tonic::{Request, Response, Status};
use tower::{Layer, Service};
use tracing::{debug, error, info, trace};
use uuid::Uuid;

use crate::astarte::handler::DeviceError;
use crate::astarte::{AstartePublisher, AstarteSubscriber, PropAccessExt};
use crate::cache::Introspection;
use crate::error::AstarteMessageHubError;
use crate::messages::{convert_data_into_proto, map_stored_properties_to_proto};

type Resp<T> = Result<Response<T>, AstarteMessageHubError>;

/// Main struct for the Astarte message hub.
pub struct AstarteMessageHub<T: Clone + AstartePublisher + AstarteSubscriber + PropAccessExt> {
    /// The nodes connected to the message hub.
    nodes: Arc<RwLock<HashMap<Uuid, AstarteNode>>>,
    /// The Astarte handler used to communicate with Astarte.
    astarte_handler: T,
    introspection: Introspection,
}

impl<T> AstarteMessageHub<T>
where
    T: 'static + Clone + AstartePublisher + AstarteSubscriber + PropAccessExt,
{
    /// Instantiate a new Astarte message hub.
    ///
    /// The `astarte_handler` should satisfy the required traits for an Astarte handler.
    pub fn new(astarte_handler: T, introspection: Introspection) -> Self {
        AstarteMessageHub {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            astarte_handler,
            introspection,
        }
    }

    /// Create a [tower] layer to intercept the Node ID in the gRPC requests
    pub fn make_interceptor_layer(&self) -> NodeIdInterceptorLayer {
        NodeIdInterceptorLayer::new(Arc::clone(&self.nodes))
    }

    async fn attach_node(
        &self,
        node_id: Uuid,
        req: Request<Node>,
    ) -> Resp<ReceiverStream<Result<MessageHubEvent, Status>>> {
        debug!("Node Attach Request");
        let node = req.into_inner();

        let interfaces_json = InterfacesJson::from_iter(node.interfaces_json);
        let astarte_node = AstarteNode::from_json(node_id, &interfaces_json)?;

        info!("Node attached {astarte_node:?}");

        let sub = self.astarte_handler.subscribe(&astarte_node).await?;

        self.introspection.store_many(&sub.added_interfaces).await;

        let mut nodes = self.nodes.write().await;
        nodes.insert(astarte_node.id, astarte_node);

        Ok(Response::new(ReceiverStream::new(sub.receiver)))
    }

    async fn detach_node(&self, id: &Uuid) -> Resp<()> {
        info!("Node {id} Detach Request");

        // TODO we should receive this currently hardcoded `false` flag from the grpc method
        let removed = self.astarte_handler.unsubscribe(id, false).await?;
        self.introspection.remove_many(&removed).await;

        let mut nodes = self.nodes.write().await;
        nodes.remove(id);

        Ok(Response::new(()))
    }

    async fn add_interfaces_node(
        &self,
        node_id: &Uuid,
        interfaces_json: &InterfacesJson,
    ) -> Resp<()> {
        let to_add = interfaces_json
            .interfaces_json
            .iter()
            .map(|i| {
                Interface::from_str(i).map(|iface| (iface.interface_name().to_string(), iface))
            })
            .try_collect()?;

        let interfaces = self
            .astarte_handler
            .extend_interfaces(node_id, to_add)
            .await?;

        self.introspection.store_many(interfaces.iter()).await;
        Ok(Response::new(()))
    }

    async fn remove_interfaces_node(
        &self,
        node_id: &Uuid,
        interfaces_name: InterfacesName,
    ) -> Resp<()> {
        let to_remove = HashSet::from_iter(interfaces_name.names);

        let removed = self
            .astarte_handler
            .remove_interfaces(node_id, to_remove)
            .await?;

        self.introspection.remove_many(&removed).await;

        Ok(Response::new(()))
    }

    async fn get_node_properties(
        &self,
        node_id: NodeId,
        interface_name: InterfaceName,
    ) -> Resp<StoredProperties> {
        let props = self
            .astarte_handler
            .interface_props(node_id, &interface_name.name)
            .await?;

        Ok(Response::new(map_stored_properties_to_proto(props)))
    }

    async fn get_node_all_properties(
        &self,
        node_id: NodeId,
        filter: PropertyFilter,
    ) -> Resp<StoredProperties> {
        // retrieve all props if no filter is specified
        let Some(owenrship) = filter.ownership else {
            debug!("get all properties");
            let props = self.astarte_handler.all_props(node_id).await?;
            let props = map_stored_properties_to_proto(props);

            return Ok(Response::new(props));
        };

        let props = match owenrship.try_into() {
            Ok(astarte_message_hub_proto::Ownership::Device) => {
                debug!("get device properties");
                self.astarte_handler.device_props(node_id).await?
            }
            Ok(astarte_message_hub_proto::Ownership::Server) => {
                debug!("get server properties");
                self.astarte_handler.server_props(node_id).await?
            }
            Err(err) => {
                error!("Invalid ownership enum value: {err}");
                return Err(AstarteMessageHubError::InvalidProtoOwnership(err));
            }
        };

        let props = map_stored_properties_to_proto(props);

        Ok(Response::new(props))
    }

    async fn get_node_property(
        &self,
        node_id: NodeId,
        prop_req: PropertyIdentifier,
    ) -> Resp<AstartePropertyIndividual> {
        // retrieve the property value
        let data = self
            .astarte_handler
            .property(node_id, &prop_req.interface_name, &prop_req.path)
            .await?
            .map(convert_data_into_proto);

        Ok(Response::new(AstartePropertyIndividual { data }))
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
    InvalidUri(#[from] axum::http::uri::InvalidUri),
    /// Http error
    Http(#[from] axum::http::Error),
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
            nodes: self.msg_hub_nodes.clone(),
        }
    }
}

#[derive(Clone)]
pub struct NodeIdInterceptor<S> {
    inner: S,
    nodes: Arc<RwLock<HashMap<Uuid, AstarteNode>>>,
}

impl<S> NodeIdInterceptor<S> {
    /// Check if the node ID is present in the request and if a node actually exists with that ID.
    ///
    /// This associated function does not contain a reference to `Self` since `NodeIdInterceptor`
    /// cannot implement the `Sync` trait (as services in tonic cannot be Sync). To circumvent this
    /// problem, we pass a reference to the message hub nodes in the input to perform the necessary
    /// checks.
    async fn check_node_id<ReqBody>(
        nodes: &RwLock<HashMap<Uuid, AstarteNode>>,
        req: http::Request<ReqBody>,
    ) -> Result<http::Request<ReqBody>, InterceptorError> {
        // check if the request is an Attach rpc
        let is_attach = is_attach(req.uri());

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

        // check that the Server contains a node with the Node ID present inside the metadata.
        // this check is performed on all gRPC requests apart from the Attach ones.
        if !is_attach && !nodes.read().await.contains_key(&node_uuid) {
            debug!("NodeId not found");
            return Err(InterceptorError::IdNotFound);
        }

        trace!("NodeId checked");

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

impl From<NodeId> for Uuid {
    fn from(value: NodeId) -> Self {
        value.0
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

impl<S, ReqBody, ResBody> Service<http::request::Request<ReqBody>> for NodeIdInterceptor<S>
where
    S: Service<http::request::Request<ReqBody>, Response = http::response::Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let nodes = Arc::clone(&self.nodes);
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let checked_req = match Self::check_node_id(&nodes, req).await {
                Ok(req) => req,
                // return a response with the status code related to the given error
                Err(err) => {
                    return Ok(Status::from(err).into_http());
                }
            };

            inner.call(checked_req).await
        })
    }
}

#[derive(thiserror::Error, Debug, displaydoc::Display)]
/// Request missing node id
pub(crate) struct NodeIdError;

impl From<NodeIdError> for Status {
    fn from(value: NodeIdError) -> Self {
        Status::failed_precondition(value.to_string())
    }
}

/// Retrieve information from a [`Request`]
pub(crate) trait RequestExt {
    fn get_node_id(&self) -> Result<&NodeId, NodeIdError>;
}

impl<R> RequestExt for Request<R> {
    fn get_node_id(&self) -> Result<&NodeId, NodeIdError> {
        if let Some(node_id) = self.extensions().get::<NodeId>() {
            trace!("node id {node_id} checked");
            return Ok(node_id);
        }

        error!("missing node id");
        Err(NodeIdError)
    }
}

#[tonic::async_trait]
impl<T> MessageHub for AstarteMessageHub<T>
where
    T: Clone + AstartePublisher + AstarteSubscriber + PropAccessExt + 'static,
{
    type AttachStream = ReceiverStream<Result<MessageHubEvent, Status>>;

    /// Attach a node to the Message hub. If the node was successfully attached,
    /// the method returns a gRPC stream into which the events received
    /// from Astarte(based on the declared Introspection) will be redirected.
    ///
    /// ```no_run
    /// use astarte_message_hub_proto::message_hub_client::MessageHubClient;
    /// use astarte_message_hub_proto::{AstarteData, AstarteDatastreamIndividual};
    /// use astarte_message_hub_proto::astarte_data::AstarteData as ProtoData;
    /// use astarte_message_hub_proto::Node;
    /// use tonic::transport::channel::Endpoint;
    /// use tonic::metadata::MetadataValue;
    /// use uuid::Uuid;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), tonic::Status> {
    ///     let uuid = Uuid::new_v4();
    ///
    ///     // adding the interceptor layer will include the Node ID inside the metadata
    ///     let channel = Endpoint::from_static("http://[::1]:50051")
    ///         .connect()
    ///         .await
    ///         .unwrap();
    ///
    ///     // adding the interceptor layer will include the Node ID inside the metadata
    ///     let mut client =
    ///         MessageHubClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
    ///             req.metadata_mut()
    ///                 .insert_bin("node-id-bin", MetadataValue::from_bytes(uuid.as_ref()));
    ///             Ok(req)
    ///         });
    ///
    ///     let interface = tokio::fs::read_to_string("/tmp/org.astarteplatform.rust.examples.DeviceDatastream.json").await
    ///         .unwrap();
    ///
    ///     let node = Node::from_interfaces([&interface]).unwrap();
    ///
    ///     let mut stream = client
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
    async fn attach(&self, request: Request<Node>) -> Result<Response<Self::AttachStream>, Status> {
        // retrieve the node id from the request metadata
        let node_id = request.get_node_id()?;
        debug!("Node {node_id} Send Request");

        self.attach_node(node_id.0, request)
            .await
            .map_err(Status::from)
    }

    /// Send a message to Astarte for a node attached to the Astarte Message Hub.
    ///
    /// ```no_run
    /// use astarte_message_hub_proto::Node;
    /// use astarte_message_hub_proto::astarte_message::Payload;
    /// use astarte_message_hub_proto::message_hub_client::MessageHubClient;
    /// use astarte_message_hub_proto::{AstarteData, AstarteDatastreamIndividual};
    /// use astarte_message_hub_proto::astarte_data::AstarteData as ProtoData;
    /// use astarte_message_hub_proto::AstarteMessage;
    /// use astarte_message_hub_proto::prost_types::Timestamp;
    /// use tonic::transport::channel::Endpoint;
    /// use tonic::metadata::MetadataValue;
    /// use uuid::Uuid;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), tonic::Status> {
    ///     let uuid = Uuid::new_v4();
    ///
    ///     // adding the interceptor layer will include the Node ID inside the metadata
    ///     let channel = Endpoint::from_static("http://[::1]:50051")
    ///         .connect()
    ///         .await
    ///         .unwrap();
    ///
    ///     // adding the interceptor layer will include the Node ID inside the metadata
    ///     let mut client =
    ///         MessageHubClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
    ///             req.metadata_mut()
    ///                 .insert_bin("node-id-bin", MetadataValue::from_bytes(uuid.as_ref()));
    ///             Ok(req)
    ///         });
    ///
    ///     let interface = tokio::fs::read_to_string("/tmp/org.astarteplatform.rust.examples.DeviceDatastream.json").await
    ///         .unwrap();
    ///
    ///     let node = Node::from_interfaces([&interface]).unwrap();
    ///
    ///     let stream = client
    ///         .attach(tonic::Request::new(node))
    ///         .await?
    ///         .into_inner();
    ///
    ///     let astarte_message = AstarteMessage {
    ///         interface_name: "org.astarteplatform.esp32.examples.DeviceDatastream".to_string(),
    ///         path: "uptimeSeconds".to_string(),
    ///         payload: Some(Payload::DatastreamIndividual(AstarteDatastreamIndividual {
    ///             data: Some(AstarteData { astarte_data: Some(ProtoData::Integer(5) )}),
    ///             timestamp: Some(Timestamp{ seconds: 1769788312, nanos: 0 }),
    ///         }))
    ///     };
    ///
    ///     let _ = client.send(astarte_message).await;
    ///
    ///     Ok(())
    ///
    /// }
    async fn send(&self, request: Request<AstarteMessage>) -> Result<Response<()>, Status> {
        let node_id = request.get_node_id()?;
        debug!("Node {node_id} Send Request");

        let astarte_message = request.into_inner();

        if let Err(err) = self.astarte_handler.publish(&astarte_message).await {
            let err_msg = format!("Unable to publish astarte message, err: {err:?}");
            error!("{err_msg}");
            Err(Status::internal(err_msg))
        } else {
            Ok(Response::new(()))
        }
    }

    /// Remove an existing Node from Astarte Message Hub.
    async fn detach(&self, request: Request<()>) -> Result<Response<()>, Status> {
        let id = request.get_node_id()?;

        self.detach_node(id).await.map_err(Status::from)
    }

    async fn add_interfaces(
        &self,
        request: Request<InterfacesJson>,
    ) -> Result<Response<()>, Status> {
        // retrieve the node id
        let node_id = request.get_node_id()?;

        info!("Node {node_id} Add Interfaces Request");

        let interfaces = request.get_ref();

        self.add_interfaces_node(node_id.as_ref(), interfaces)
            .await
            .map_err(Status::from)
    }

    async fn remove_interfaces(
        &self,
        request: Request<InterfacesName>,
    ) -> Result<Response<()>, Status> {
        // retrieve the node id
        let node_id = *request.get_node_id()?;

        info!("Node {node_id} Remove Interfaces Request");
        let interfaces = request.into_inner();

        self.remove_interfaces_node(node_id.as_ref(), interfaces)
            .await
            .map_err(Status::from)
    }

    async fn get_properties(
        &self,
        request: Request<InterfaceName>,
    ) -> Result<Response<StoredProperties>, Status> {
        // retrieve the node id
        let node_id = *request.get_node_id()?;

        info!("Node {node_id} Get Properties Request");
        let prop_req = request.into_inner();

        self.get_node_properties(node_id, prop_req)
            .await
            .map_err(Status::from)
    }

    async fn get_all_properties(
        &self,
        request: Request<PropertyFilter>,
    ) -> Result<Response<StoredProperties>, Status> {
        // retrieve the node id
        let node_id = *request.get_node_id()?;

        info!("Node {node_id} Get All Properties Request");
        let filter = request.into_inner();

        self.get_node_all_properties(node_id, filter)
            .await
            .map_err(Status::from)
    }

    async fn get_property(
        &self,
        request: Request<PropertyIdentifier>,
    ) -> Result<Response<AstartePropertyIndividual>, Status> {
        // retrieve the node id
        let node_id = *request.get_node_id()?;

        info!("Node {node_id} Get Property Request");
        let prop_req = request.into_inner();

        self.get_node_property(node_id, prop_req)
            .await
            .map_err(Status::from)
    }
}

/// A single node that can be connected to the Astarte message hub.
#[derive(Debug, Clone)]
pub struct AstarteNode {
    /// Identifier for the node
    pub id: Uuid,
    /// A vector of interfaces for this node.
    pub introspection: HashMap<String, Interface>,
}

impl AstarteNode {
    /// Instantiate a new node.
    pub fn new(uuid: Uuid, introspection: HashMap<String, Interface>) -> Self {
        AstarteNode {
            id: uuid,
            introspection,
        }
    }

    pub fn from_json(uuid: Uuid, interfaces_json: &InterfacesJson) -> Result<Self, DeviceError> {
        let introspection = interfaces_json
            .interfaces_json
            .iter()
            .map(|i| {
                Interface::from_str(i)
                    .map_err(DeviceError::Interface)
                    .map(|i| (i.interface_name().to_string(), i))
            })
            .try_collect()?;

        Ok(Self::new(uuid, introspection))
    }
}

#[cfg(test)]
mod test {
    use astarte_device_sdk::AstarteData as AstarteDataSdk;
    use astarte_device_sdk::store::StoredProp;
    use astarte_interfaces::error::Error as InterfaceError;
    use astarte_interfaces::schema::Ownership;
    use astarte_message_hub_proto::astarte_data::AstarteData as ProtoData;
    use astarte_message_hub_proto::astarte_message::Payload;
    use astarte_message_hub_proto::{AstarteData, AstarteDatastreamIndividual};
    use async_trait::async_trait;
    use mockall::mock;
    use std::collections::HashMap;
    use std::io;
    use std::io::ErrorKind;
    use std::ops::Deref;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::RwLock;
    use tokio::sync::mpsc;
    use tonic::body::Body;
    use tonic::metadata::{MetadataMap, MetadataValue};
    use tonic::{Code, Request, Status};
    use tower::{Service, ServiceBuilder};
    use uuid::Uuid;

    use crate::astarte::Subscription;
    use crate::astarte::{AstartePublisher, AstarteSubscriber};
    use crate::error::AstarteMessageHubError;
    use crate::server::{
        AstarteNode, InterceptorError, NodeId, NodeIdInterceptorLayer, node_id_from_ascii,
        node_id_from_bin,
    };

    use super::*;

    const TEST_UUID: Uuid = uuid::uuid!("d1e7a6e9-cf99-4694-8fb6-997934be079c");

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
            ) -> Result<Subscription, AstarteMessageHubError>;

            async fn unsubscribe(&self, id: &Uuid, cleanup_interfaces: bool) -> Result<Vec<String>, AstarteMessageHubError>;

            async fn extend_interfaces(&self, node_id: &Uuid, to_add: HashMap<String, Interface>) -> Result<Vec<Interface>, AstarteMessageHubError>;

            async fn remove_interfaces(&self, node_id: &Uuid, to_remove: HashSet<String>) -> Result<Vec<String>, AstarteMessageHubError>;
        }

        impl PropAccessExt for AstarteHandler {
            async fn property(
                &self,
                node_id: NodeId,
                interface: &str,
                path: &str,
            ) -> Result<Option<AstarteDataSdk>, AstarteMessageHubError>;

            async fn interface_props(
                &self,
                node_id: NodeId,
                interface: &str,
            ) -> Result<Vec<StoredProp>, AstarteMessageHubError>;

            async fn all_props(&self, node_id: NodeId) -> Result<Vec<StoredProp>, AstarteMessageHubError>;

            async fn device_props(
                &self,
                node_id: NodeId,
            ) -> Result<Vec<StoredProp>, AstarteMessageHubError>;

            async fn server_props(
                &self,
                node_id: NodeId,
            ) -> Result<Vec<StoredProp>, AstarteMessageHubError>;
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
                    "endpoint": "/test/button",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/test/uptimeSeconds",
                    "type": "integer",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;

    const SERV_PROPS_IFACE: &str = r#"
        {
            "interface_name": "org.astarte-platform.Test",
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

    async fn mock_msg_hub(
        mock_astarte: MockAstarteHandler,
    ) -> (AstarteMessageHub<MockAstarteHandler>, TempDir) {
        let tmp = TempDir::new().unwrap();

        let introspection = Introspection::create(tmp.path().join("interfaces"))
            .await
            .unwrap();

        (AstarteMessageHub::new(mock_astarte, introspection), tmp)
    }

    fn add_metadata_node_id<T>(uuid: Uuid, req: &mut Request<T>) {
        req.metadata_mut()
            .insert_bin("node-id-bin", MetadataValue::from_bytes(uuid.as_bytes()));
        req.extensions_mut().insert(NodeId(uuid));
    }

    async fn attach(
        node_id: Uuid,
        msg_hub: &AstarteMessageHub<MockAstarteHandler>,
    ) -> Result<tonic::Response<ReceiverStream<Result<MessageHubEvent, Status>>>, Status> {
        let interfaces = vec![SERV_PROPS_IFACE.to_string(), SERV_OBJ_IFACE.to_string()];
        let node = Node::new(interfaces);

        let mut req_node = Request::new(node);

        // add the node id into the metadata request
        add_metadata_node_id(node_id, &mut req_node);

        msg_hub.attach(req_node).await
    }

    #[tokio::test]
    async fn attach_success_node() {
        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte.expect_subscribe().returning(|node| {
            let (_, rx) = mpsc::channel(2);
            Ok(Subscription {
                added_interfaces: node.introspection.values().cloned().collect(),
                receiver: rx,
            })
        });
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let (msg_hub, _dir) = mock_msg_hub(mock_astarte).await;
        let attach_result = attach(TEST_UUID, &msg_hub).await;

        assert!(
            attach_result.is_ok(),
            "error {:?}",
            attach_result.unwrap_err()
        );
    }

    #[tokio::test]
    async fn send_message_hub_event() {
        let (tx, rx) = mpsc::channel(2);

        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte.expect_subscribe().return_once(|node| {
            Ok(Subscription {
                added_interfaces: node.introspection.values().cloned().collect(),
                receiver: rx,
            })
        });
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let (msg_hub, _dir) = mock_msg_hub(mock_astarte).await;
        let attach_result = attach(TEST_UUID, &msg_hub).await;

        // send a custom error to the Node
        let msghub_event = MessageHubEvent::from_error(AstarteMessageHubError::Astarte(
            astarte_device_sdk::Error::Interface(InterfaceError::DuplicateMapping {
                endpoint: "test".to_string(),
                duplicate: "test".to_string(),
            }),
        ));
        if let Err(err) = tx.send(Ok(msghub_event.clone())).await {
            panic!("send error: {err:?}");
        }

        let mut receiver = attach_result.unwrap().into_inner().into_inner();
        let event = receiver.recv().await.unwrap().unwrap();
        assert_eq!(event, msghub_event);
    }

    #[tokio::test]
    async fn attach_reject_invalid_uuid_node() {
        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let (msg_hub, _dir) = mock_msg_hub(mock_astarte).await;

        let node = Node::new(vec![]);

        // avoid inserting the node id
        let req_node = Request::new(node);
        let attach_result = msg_hub.attach(req_node).await;

        assert!(attach_result.is_err());
        let err: Status = attach_result.err().unwrap();
        assert_eq!(err.code(), Code::FailedPrecondition)
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

        let (msg_hub, _dir) = mock_msg_hub(mock_astarte).await;

        let interfaces = [SERV_PROPS_IFACE];

        let node = Node::from_interfaces(interfaces).unwrap();

        let req_node = Request::new(node);
        let attach_result = msg_hub.attach(req_node).await;

        assert!(attach_result.is_err());
        let err: Status = attach_result.err().unwrap();
        assert_eq!(err.code(), Code::FailedPrecondition)
    }

    #[tokio::test]
    async fn send_message_success() {
        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte.expect_publish().returning(|_| Ok(()));
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let (msg_hub, _dir) = mock_msg_hub(mock_astarte).await;

        let interface_name = "io.demo.Values".to_owned();
        let astarte_data = AstarteData {
            astarte_data: Some(ProtoData::Integer(5)),
        };
        let payload = Payload::DatastreamIndividual(AstarteDatastreamIndividual {
            data: Some(astarte_data.clone()),
            timestamp: None,
        });

        let astarte_message = AstarteMessage {
            interface_name,
            path: "/test".to_string(),
            payload: Some(payload),
        };

        let mut req_astarte_message = Request::new(astarte_message);
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_astarte_message
            .extensions_mut()
            .insert(NodeId(TEST_UUID));

        let send_result = msg_hub.send(req_astarte_message).await;

        assert!(send_result.is_ok())
    }

    #[tokio::test]
    async fn send_message_reject() {
        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte.expect_publish().returning(|_| {
            Err(AstarteMessageHubError::Io(io::Error::new(
                ErrorKind::InvalidData,
                "interface not found",
            )))
        });
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let (msg_hub, _dir) = mock_msg_hub(mock_astarte).await;

        let interface_name = "io.demo.Values".to_owned();
        let astarte_data = AstarteData {
            astarte_data: Some(ProtoData::Integer(5)),
        };
        let payload = Payload::DatastreamIndividual(AstarteDatastreamIndividual {
            data: Some(astarte_data.clone()),
            timestamp: None,
        });

        let astarte_message = AstarteMessage {
            interface_name,
            path: "/test".to_string(),
            payload: Some(payload),
        };

        let mut req_astarte_message = Request::new(astarte_message);
        req_astarte_message
            .extensions_mut()
            .insert(NodeId(TEST_UUID));
        let send_result = msg_hub.send(req_astarte_message).await;

        assert!(send_result.is_err());
        let err: Status = send_result.err().unwrap();
        assert_eq!(err.code(), Code::Internal)
    }

    #[tokio::test]
    async fn detach_node_success() {
        let mut mock_astarte = MockAstarteHandler::new();

        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        mock_astarte.expect_subscribe().returning(|node| {
            let (_, rx) = mpsc::channel(2);
            Ok(Subscription {
                added_interfaces: node.introspection.values().cloned().collect(),
                receiver: rx,
            })
        });
        mock_astarte
            .expect_unsubscribe()
            .returning(|_, _| Ok(vec!["org.astarte-platform.test.test".to_string()]));

        let (msg_hub, _dir) = mock_msg_hub(mock_astarte).await;

        assert!(attach(TEST_UUID, &msg_hub).await.is_ok());

        let mut req_node_detach = Request::new(());
        add_metadata_node_id(TEST_UUID, &mut req_node_detach);
        let detach_result = msg_hub.detach(req_node_detach).await;

        assert!(detach_result.is_ok())
    }

    #[tokio::test]
    async fn detach_node_not_found() {
        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte.expect_unsubscribe().returning(|_, _| {
            Err(AstarteMessageHubError::AstarteInvalidData(
                "invalid node".to_string(),
            ))
        });
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        let (msg_hub, _dir) = mock_msg_hub(mock_astarte).await;

        let mut req_node = Request::new(());

        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_node.extensions_mut().insert(NodeId(TEST_UUID));

        let detach_result = msg_hub.detach(req_node).await;

        assert!(detach_result.is_err());
        let err: Status = detach_result.err().unwrap();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn detach_node_unsubscribe_failed() {
        let mut mock_astarte = MockAstarteHandler::new();

        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        mock_astarte.expect_subscribe().returning(|node| {
            let (_, rx) = mpsc::channel(2);
            Ok(Subscription {
                added_interfaces: node.introspection.values().cloned().collect(),
                receiver: rx,
            })
        });
        mock_astarte.expect_unsubscribe().returning(|_, _| {
            Err(AstarteMessageHubError::AstarteInvalidData(
                "invalid node".to_string(),
            ))
        });

        let (msg_hub, _dir) = mock_msg_hub(mock_astarte).await;

        let interfaces = vec![SERV_PROPS_IFACE.to_string()];
        let node = Node::new(interfaces);

        let mut req_node_attach = Request::new(node);
        req_node_attach.extensions_mut().insert(NodeId(TEST_UUID));
        assert!(msg_hub.attach(req_node_attach).await.is_ok());

        let mut req_node_detach = Request::new(());
        req_node_detach.extensions_mut().insert(NodeId(TEST_UUID));
        let detach_result = msg_hub.detach(req_node_detach).await;

        assert!(detach_result.is_err());
        let err: Status = detach_result.err().unwrap();
        assert_eq!(err.code(), Code::InvalidArgument)
    }

    #[tokio::test]
    async fn test_get_property() {
        let mut mock_astarte = MockAstarteHandler::new();
        mock_astarte
            .expect_clone()
            .returning(MockAstarteHandler::new);

        // expect property not found
        mock_astarte
            .expect_property()
            .once()
            .returning(|_, _, _| Ok(None));

        // expect property
        mock_astarte
            .expect_property()
            .once()
            .returning(|_, _, _| Ok(Some(AstarteDataSdk::Boolean(true))));

        let (msg_hub, _dir) = mock_msg_hub(mock_astarte).await;

        let interface_name = "io.demo.Values".to_owned();

        let prop_identifier = PropertyIdentifier {
            interface_name,
            path: "/1/test".to_string(),
        };

        // get an unset property
        let mut req_astarte_message = Request::new(prop_identifier.clone());
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_astarte_message
            .extensions_mut()
            .insert(NodeId(TEST_UUID));

        let res = msg_hub.get_property(req_astarte_message).await;
        assert!(res.is_ok());

        let prop_res = res.unwrap().into_inner();
        assert!(matches!(prop_res, AstartePropertyIndividual { data: None }));

        // get a property value
        let mut req_astarte_message = Request::new(prop_identifier);
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_astarte_message
            .extensions_mut()
            .insert(NodeId(TEST_UUID));

        let res = msg_hub.get_property(req_astarte_message).await;
        let prop_value_res = res.unwrap().into_inner().data.unwrap();
        let exp = astarte_message_hub_proto::AstarteData {
            astarte_data: Some(astarte_message_hub_proto::astarte_data::AstarteData::Boolean(true)),
        };
        assert_eq!(prop_value_res, exp);
    }

    #[tokio::test]
    async fn test_get_all_properties() {
        let mut mock_astarte = MockAstarteHandler::new();

        let p1_dev = StoredProp {
            interface: "io.demo.Values1".to_string(),
            path: "/1/test1".to_string(),
            value: AstarteDataSdk::Integer(1),
            interface_major: 0,
            ownership: Ownership::Device,
        };
        let p2_dev = StoredProp {
            interface: "io.demo.Values1".to_string(),
            path: "/1/test2".to_string(),
            value: AstarteDataSdk::Boolean(true),
            interface_major: 0,
            ownership: Ownership::Device,
        };
        let p1_serv = StoredProp {
            interface: "io.demo.Values2".to_string(),
            path: "/2/test1".to_string(),
            value: AstarteDataSdk::Boolean(true),
            interface_major: 0,
            ownership: Ownership::Server,
        };

        let all_props = vec![p1_dev.clone(), p2_dev.clone(), p1_serv.clone()];
        let dev_props = vec![p1_dev, p2_dev];
        let serv_props = vec![p1_serv];

        // expect all properties when no prop value is available
        mock_astarte
            .expect_all_props()
            .once()
            .returning(|_| Ok(vec![]));

        // expect all properties
        mock_astarte
            .expect_all_props()
            .once()
            .return_once(|_| Ok(all_props));

        // expect all device properties
        let dev_props_cl = dev_props.clone();
        mock_astarte
            .expect_device_props()
            .once()
            .return_once(|_| Ok(dev_props_cl));

        // expect all server properties
        let serv_props_cl = serv_props.clone();
        mock_astarte
            .expect_server_props()
            .once()
            .return_once(|_| Ok(serv_props_cl));

        let (msg_hub, _dir) = mock_msg_hub(mock_astarte).await;

        // get all properties when no values are available
        let prop_filter = PropertyFilter { ownership: None };

        let mut req_astarte_message = Request::new(prop_filter);
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_astarte_message
            .extensions_mut()
            .insert(NodeId(TEST_UUID));

        let res = msg_hub.get_all_properties(req_astarte_message).await;
        let prop_res = res.unwrap().into_inner().properties;
        assert!(prop_res.is_empty());

        // get all properties when values are available
        let mut req_astarte_message = Request::new(prop_filter);
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_astarte_message
            .extensions_mut()
            .insert(NodeId(TEST_UUID));

        let res = msg_hub.get_all_properties(req_astarte_message).await;

        assert_eq!(res.unwrap().into_inner().properties.len(), 3);

        // get all device properties
        let prop_filter = PropertyFilter {
            ownership: Some(Ownership::Device as i32),
        };

        let mut req_astarte_message = Request::new(prop_filter);
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_astarte_message
            .extensions_mut()
            .insert(NodeId(TEST_UUID));

        let res = msg_hub.get_all_properties(req_astarte_message).await;

        let len = res
            .unwrap()
            .into_inner()
            .properties
            .iter()
            .filter(|prop| prop.interface_name == "io.demo.Values1")
            .collect_vec()
            .len();

        assert_eq!(len, 2);

        // get all server properties
        let prop_filter = PropertyFilter {
            ownership: Some(Ownership::Server as i32),
        };

        let mut req_astarte_message = Request::new(prop_filter);
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_astarte_message
            .extensions_mut()
            .insert(NodeId(TEST_UUID));

        let res = msg_hub.get_all_properties(req_astarte_message).await;

        let len = res
            .unwrap()
            .into_inner()
            .properties
            .iter()
            .filter(|prop| prop.interface_name == "io.demo.Values2")
            .collect_vec()
            .len();

        assert_eq!(len, 1);
    }

    #[tokio::test]
    async fn test_get_properties() {
        let mut mock_astarte = MockAstarteHandler::new();

        let prop1 = StoredProp {
            interface: "io.demo.Values1".to_string(),
            path: "/1/test1".to_string(),
            value: AstarteDataSdk::Integer(1),
            interface_major: 0,
            ownership: Ownership::Device,
        };
        let prop2 = StoredProp {
            interface: "io.demo.Values1".to_string(),
            path: "/1/test2".to_string(),
            value: AstarteDataSdk::Boolean(true),
            interface_major: 0,
            ownership: Ownership::Device,
        };

        let props = vec![prop1, prop2];

        // expect Internal status code when get_properties is called with an unknown interface
        mock_astarte
            .expect_interface_props()
            .once()
            .return_once(|_, _| {
                Err(AstarteMessageHubError::Astarte(
                    astarte_device_sdk::Error::InterfaceNotFound {
                        name: "io.demo.Values3".to_string(),
                    },
                ))
            });

        // expect get_properties
        mock_astarte
            .expect_interface_props()
            .once()
            .return_once(|_, _| Ok(props));

        let (msg_hub, _dir) = mock_msg_hub(mock_astarte).await;

        // get_properties with unknown interface
        let iface_name = InterfaceName {
            name: "unknown".to_string(),
        };

        let mut req_astarte_message = Request::new(iface_name);
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_astarte_message
            .extensions_mut()
            .insert(NodeId(TEST_UUID));

        let res = msg_hub.get_properties(req_astarte_message).await;

        assert_eq!(res.err().unwrap().code(), Code::Internal);

        // get properties
        let iface_name = InterfaceName {
            name: "io.demo.Values1".to_string(),
        };

        let mut req_astarte_message = Request::new(iface_name);
        // it is necessary to add the NodeId extensions otherwise sending is not allowed
        req_astarte_message
            .extensions_mut()
            .insert(NodeId(TEST_UUID));

        let res = msg_hub.get_properties(req_astarte_message).await;

        let props = res.unwrap().into_inner().properties;

        assert_eq!(props.len(), 2);
    }

    #[test]
    fn failed_invalid_interface() {
        let interfaces = InterfacesJson::from_iter(["INVALID".to_string()]);

        let astarte_node = AstarteNode::from_json(TEST_UUID, &interfaces);

        assert!(astarte_node.is_err())
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

    fn create_http_req(uri: &str) -> http::Request<Body> {
        http::Request::builder()
            .uri(uri)
            .body(Body::default())
            .expect("failed to create http req")
    }

    fn create_http_req_with_metadata_node_id(uri: &str) -> http::Request<Body> {
        let req = http::Request::builder()
            .uri(uri)
            .body(Body::default())
            .expect("failed to create http req");

        let (mut parts, body) = req.into_parts();
        let mut metadata = MetadataMap::from_headers(parts.headers);
        let metadata_val = MetadataValue::from_bytes(TEST_UUID.as_bytes());
        metadata.insert_bin("node-id-bin", metadata_val);
        parts.headers = metadata.into_headers();

        http::Request::from_parts(parts, body)
    }

    fn create_node_interceptor_service(
        hm: HashMap<Uuid, AstarteNode>,
    ) -> impl Service<http::Request<Body>, Response = http::Response<Body>, Error = InterceptorError>
    {
        let msg_hub_nodes = Arc::new(RwLock::new(hm));

        ServiceBuilder::new()
            .layer(NodeIdInterceptorLayer::new(msg_hub_nodes))
            .service_fn(|_req: http::Request<Body>| {
                futures::future::ready(
                    http::Response::builder()
                        .status(http::StatusCode::OK)
                        .body(Body::default())
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

        let msg_hub_nodes =
            HashMap::from([(TEST_UUID, AstarteNode::new(TEST_UUID, HashMap::default()))]);

        let mut service = create_node_interceptor_service(msg_hub_nodes);

        let res = service.call(req).await;

        assert!(res.is_ok());
    }
}
