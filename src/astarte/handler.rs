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

//! Contains an implementation of an Astarte handler.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use astarte_device_sdk::{
    client::RecvError,
    interface::error::InterfaceError,
    properties::PropAccess,
    store::SqliteStore,
    transport::grpc::convert::{map_values_to_astarte_type, MessageHubProtoError},
    types::AstarteType,
    DeviceEvent, Error as AstarteError, Interface, Value,
};
use astarte_message_hub_proto::{
    astarte_data_type::Data, astarte_message::Payload, AstarteDataTypeIndividual, AstarteMessage,
    MessageHubEvent,
};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use log::{debug, error, info, trace};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::RwLock;
use tonic::{Code, Status};
use uuid::Uuid;

use super::sdk::{Client, DeviceClient, DynamicIntrospection};
use super::{AstartePublisher, AstarteSubscriber, EventSender, Subscription};
use crate::error::AstarteMessageHubError;
use crate::server::AstarteNode;

type SubscribersMap = Arc<RwLock<HashMap<Uuid, Subscriber>>>;

/// Error while sending or receiving data from Astarte.
#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum DeviceError {
    /// received error from Astarte
    Astarte(#[source] AstarteError),
    /// couldn't convert the astarte event into a proto message
    Convert(#[from] MessageHubProtoError),
    /// subscriber already disconnected
    Disconnected,
    /// invalid interface json in node introspection
    Interface(#[from] InterfaceError),
}

impl From<&DeviceError> for Code {
    fn from(value: &DeviceError) -> Self {
        match value {
            DeviceError::Astarte(_) => Code::Aborted,
            DeviceError::Disconnected => Code::Internal,
            DeviceError::Convert(_) | DeviceError::Interface(_) => Code::InvalidArgument,
        }
    }
}

/// Initialize the [`DevicePublisher`] and [`DeviceSubscriber`]
pub fn init_pub_sub(client: DeviceClient<SqliteStore>) -> (DevicePublisher, DeviceSubscriber) {
    let subscribers = SubscribersMap::default();

    (
        DevicePublisher::new(client.clone(), subscribers.clone()),
        DeviceSubscriber::new(client, subscribers),
    )
}

/// Receiver for the [`DeviceEvent`].
///
/// It will forward the events to the registered subscriber.
pub struct DeviceSubscriber {
    client: DeviceClient<SqliteStore>,
    subscribers: SubscribersMap,
}

impl DeviceSubscriber {
    /// Create a new client.
    pub fn new(client: DeviceClient<SqliteStore>, subscribers: SubscribersMap) -> Self {
        Self {
            client,
            subscribers,
        }
    }

    /// Handler responsible for receiving and managing [`DeviceEvent`].
    pub async fn forward_events(&mut self) -> Result<(), DeviceError> {
        loop {
            match self.client.recv().await {
                Err(RecvError::Disconnected) => return Err(DeviceError::Disconnected),
                event => {
                    if let Err(err) = self.on_event(event).await {
                        error!("error on event receive: {err}");
                    }
                }
            }
        }
    }

    async fn on_event(&mut self, event: Result<DeviceEvent, RecvError>) -> Result<(), DeviceError> {
        let subscribers = self.subscribers.read().await;

        // determine if sending a proto message hub error or a correct message to the message hub nodes
        match event {
            Ok(event) => {
                let iface_name = event.interface.clone();

                let msghub_event = MessageHubEvent::from(AstarteMessage::from(event));

                trace!("message hub event: {msghub_event:?}");

                // filter only the subscribers having iface_name in their introspection
                let subscribers = subscribers
                    .values()
                    .filter(|s| s.introspection.contains(&iface_name));

                self.send_to_subscribers(msghub_event, subscribers).await
            }
            Err(RecvError::MappingNotFound { interface, mapping }) => {
                error!("mapping {mapping} not found");

                let iface_name = Interface::from_str(&interface)?
                    .interface_name()
                    .to_string();

                let msghub_event = MessageHubEvent::from_error(&RecvError::MappingNotFound {
                    interface: interface.clone(),
                    mapping,
                });

                trace!("message hub event: {msghub_event:?}");

                // filter only the subscribers having iface_name in their introspection
                let subscribers = subscribers
                    .values()
                    .filter(|s| s.introspection.contains(&iface_name));

                self.send_to_subscribers(msghub_event, subscribers).await
            }
            Err(err) => {
                error!("receive error: {err}");

                let msghub_event = MessageHubEvent::from_error(&err);

                trace!("message hub event: {msghub_event:?}");

                // send to all subscribers
                self.send_to_subscribers(msghub_event, subscribers.values())
                    .await
            }
        }
    }

    /// Send a message to a subset of subscribers.
    ///
    /// The closure passed in input to the method discriminates which subscribers must receive a message.
    async fn send_to_subscribers<'a>(
        &'a self,
        msghub_event: MessageHubEvent,
        subscribers: impl Iterator<Item = &'a Subscriber>,
    ) -> Result<(), DeviceError> {
        for subscriber in subscribers {
            subscriber
                .sender
                // This is needed to satisfy the tonic trait
                .send(Ok(msghub_event.clone()))
                .await
                .map_err(|_| DeviceError::Disconnected)?;
        }

        Ok(())
    }
}

/// A subscriber for the Astarte handler.
#[derive(Debug, Clone)]
pub struct Subscriber {
    introspection: HashSet<String>,
    sender: Sender<Result<MessageHubEvent, Status>>,
}

/// An Astarte Device SDK based implementation of an Astarte handler.
///
/// Uses the [`DeviceClient`] to provide subscribe and publish functionality.
#[derive(Clone)]
pub struct DevicePublisher {
    client: DeviceClient<SqliteStore>,
    subscribers: SubscribersMap,
}

impl DevicePublisher {
    /// Constructs a new handler from the [AstarteDeviceSdk](astarte_device_sdk::AstarteDeviceSdk)
    fn new(client: DeviceClient<SqliteStore>, subscribers: SubscribersMap) -> Self {
        DevicePublisher {
            client,
            subscribers,
        }
    }

    /// Publish an [`AstarteDataTypeIndividual`] on specific interface and path.
    async fn publish_astarte_individual(
        &self,
        data: AstarteDataTypeIndividual,
        interface_name: &str,
        path: &str,
        timestamp: Option<pbjson_types::Timestamp>,
    ) -> Result<(), AstarteMessageHubError> {
        let astarte_type: AstarteType = data
            .individual_data
            .ok_or_else(|| {
                AstarteMessageHubError::AstarteInvalidData("Invalid individual data".to_string())
            })?
            .try_into()?;

        if let Some(timestamp) = timestamp {
            let timestamp = timestamp
                .try_into()
                .map_err(AstarteMessageHubError::Timestamp)?;
            self.client
                .send_with_timestamp(interface_name, path, astarte_type, timestamp)
                .await
        } else {
            self.client.send(interface_name, path, astarte_type).await
        }
        .map_err(AstarteMessageHubError::Astarte)
    }

    /// Publish an [`AstarteDataTypeObject`](astarte_message_hub_proto::AstarteDataTypeObject) on specific interface and path.
    async fn publish_astarte_object(
        &self,
        object_data: astarte_message_hub_proto::AstarteDataTypeObject,
        interface_name: &str,
        path: &str,
        timestamp: Option<pbjson_types::Timestamp>,
    ) -> Result<(), AstarteMessageHubError> {
        let aggr = map_values_to_astarte_type(object_data)?;

        if let Some(timestamp) = timestamp {
            let timestamp = timestamp
                .try_into()
                .map_err(AstarteMessageHubError::Timestamp)?;
            self.client
                .send_object_with_timestamp(interface_name, path, aggr, timestamp)
                .await
        } else {
            self.client.send_object(interface_name, path, aggr).await
        }
        .map_err(AstarteMessageHubError::Astarte)
    }

    /// Send all the properties in the attached node introspection.
    async fn send_server_props(
        &self,
        node: &AstarteNode,
        sender: &Sender<Result<MessageHubEvent, Status>>,
    ) -> Result<(), AstarteMessageHubError> {
        debug!("sending properties");

        let iter = self
            .client
            .server_props()
            .await?
            .into_iter()
            .filter_map(|prop| {
                node.introspection.contains_key(&prop.interface).then(|| {
                    let event = DeviceEvent {
                        interface: prop.interface,
                        path: prop.path,
                        data: Value::Individual(prop.value),
                    };

                    MessageHubEvent::from(AstarteMessage::from(event))
                })
            });

        futures::stream::iter(iter)
            .then(|msg| async { sender.send(Ok(msg)).await })
            .inspect_err(|_| error!("failed to send prop, device disconnected"))
            .try_collect::<()>()
            .await
            .map_err(|_| DeviceError::Disconnected)?;

        Ok(())
    }
}

#[async_trait]
impl AstarteSubscriber for DevicePublisher {
    async fn subscribe(
        &self,
        node: &AstarteNode,
    ) -> Result<(Subscription, EventSender), AstarteMessageHubError> {
        let introspection = node
            .introspection
            .values()
            .map(|i| i.interface_name().to_string())
            .collect();

        let added_names = self
            .client
            .extend_interfaces_vec(node.introspection.values().cloned().collect())
            .await?;

        let added_interfaces = added_names
            .iter()
            .filter_map(|i| node.introspection.get(i).cloned())
            .collect();

        let (sender, receiver) = channel(32);

        self.send_server_props(node, &sender).await?;

        self.subscribers.write().await.insert(
            node.id,
            Subscriber {
                introspection,
                sender: sender.clone(),
            },
        );

        Ok((
            Subscription {
                added_interfaces,
                receiver,
            },
            sender,
        ))
    }

    /// Unsubscribe an existing Node and its introspection from Astarte Message Hub.
    ///
    /// All the interfaces in this node introspection that are not in the introspection of any other
    /// node will be removed and the names will be returned.
    async fn unsubscribe(&self, id: &Uuid) -> Result<Vec<String>, AstarteMessageHubError> {
        let mut subscribers = self.subscribers.write().await;

        let removed = {
            let (id, node) = subscribers
                .get_key_value(id)
                .ok_or_else(|| AstarteMessageHubError::NodeId(*id))?;

            let to_remove = node
                .introspection
                .iter()
                .filter(|&interface| {
                    !subscribers
                        .iter()
                        // Check if any other subscriber use the same interface
                        .any(|(s_id, s_node)| {
                            s_id != id && s_node.introspection.contains(interface)
                        })
                })
                .cloned()
                .collect_vec();

            debug!("interfaces to remove {to_remove:?}");

            self.client.remove_interfaces(to_remove).await?
        };

        subscribers.remove(id);

        Ok(removed)
    }

    /// Extend the existing Node interfaces and its introspection.
    async fn extend_interfaces(
        &self,
        node_id: &Uuid,
        to_add: HashMap<String, Interface>,
    ) -> Result<Vec<Interface>, AstarteMessageHubError> {
        let mut rw_subscribers = self.subscribers.write().await;

        let Some(subscriber) = rw_subscribers.get_mut(node_id) else {
            error!("invalid node id {node_id}");
            return Err(AstarteMessageHubError::NodeId(*node_id));
        };

        trace!(
            "extending introspection with the following interfaces: {:?}",
            to_add.keys()
        );
        let added_names = self
            .client
            .extend_interfaces_vec(to_add.values().cloned().collect())
            .await?;
        info!("Node interfaces extended");

        let added_interfaces = added_names
            .iter()
            .filter_map(|i| to_add.get(i).cloned())
            .collect();

        subscriber.introspection.extend(to_add.into_keys());

        Ok(added_interfaces)
    }

    /// Remove one or more Node interfaces from its introspection.
    async fn remove_interfaces(
        &self,
        node_id: &Uuid,
        to_remove: HashSet<String>,
    ) -> Result<Vec<String>, AstarteMessageHubError> {
        let mut rw_subscribers = self.subscribers.write().await;

        if !rw_subscribers.contains_key(node_id) {
            error!("invalid node id {node_id}");
            return Err(AstarteMessageHubError::NodeId(*node_id));
        };

        // check that each of the interfaces to remove is not present in other nodes:
        // - if at least another node uses that interface, the interface should not be removed from the introspection but only from that node (in the subscriber map)
        // - if none of the other nodes use that interface, it can also be removed from the introspection
        let to_remove_from_intr = to_remove
            .iter()
            .filter(|&iface| {
                !rw_subscribers
                    .iter()
                    // Check if any other subscriber use the same interface
                    .any(|(s_id, s_node)| s_id != node_id && s_node.introspection.contains(iface))
            })
            .cloned()
            .collect_vec();

        trace!("removing the following interfaces from the introspection: {to_remove_from_intr:?}");
        let removed_interfaces = self
            .client
            .remove_interfaces_vec(to_remove_from_intr)
            .await?;
        debug!("interfaces removed");

        let sub = rw_subscribers
            .get_mut(node_id)
            .ok_or(AstarteMessageHubError::NodeId(*node_id))?;

        // remove the specified interfaces from the current node
        for to_remove in to_remove.iter() {
            sub.introspection.remove(to_remove);
        }
        debug!("introspection updated after removing interfaces");

        Ok(removed_interfaces)
    }
}

#[async_trait]
impl AstartePublisher for DevicePublisher {
    async fn publish(
        &self,
        astarte_message: &AstarteMessage,
    ) -> Result<(), AstarteMessageHubError> {
        let astarte_message_payload = astarte_message.clone().payload.ok_or_else(|| {
            AstarteMessageHubError::AstarteInvalidData("Invalid payload".to_string())
        })?;

        match astarte_message_payload {
            Payload::AstarteUnset(_) => self
                .client
                .unset(&astarte_message.interface_name, &astarte_message.path)
                .await
                .map_err(AstarteMessageHubError::Astarte),
            Payload::AstarteData(astarte_data) => {
                let astarte_data = astarte_data.data.ok_or_else(|| {
                    AstarteMessageHubError::AstarteInvalidData(
                        "Invalid Astarte data type".to_string(),
                    )
                })?;

                match astarte_data {
                    Data::AstarteIndividual(data) => {
                        self.publish_astarte_individual(
                            data,
                            &astarte_message.interface_name,
                            &astarte_message.path,
                            astarte_message.timestamp.clone(),
                        )
                        .await
                    }
                    Data::AstarteObject(object_data) => {
                        self.publish_astarte_object(
                            object_data,
                            &astarte_message.interface_name,
                            &astarte_message.path,
                            astarte_message.timestamp.clone(),
                        )
                        .await
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::error::Error;
    use std::str::FromStr;

    use astarte_device_sdk::error::Error as AstarteSdkError;
    use astarte_device_sdk::transport::grpc::Grpc;
    use astarte_device_sdk::Value;
    use astarte_device_sdk_mock::MockDeviceConnection as DeviceConnection;
    use astarte_message_hub_proto::astarte_data_type_individual::IndividualData;
    use astarte_message_hub_proto::{AstarteUnset, InterfacesJson, MessageHubError};
    use chrono::Utc;
    use pbjson_types::Timestamp;

    const SERV_PROPS_IFACE: &str = r#"
        {
            "interface_name": "org.astarte-platform.test.test",
            "version_major": 1,
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
                    "endpoint": "/request/button",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/request/uptimeSeconds",
                    "type": "integer",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;

    fn create_astarte_message(
        iname: impl Into<String>,
        path: impl Into<String>,
        payload: Option<Payload>,
        timestamp: Option<Timestamp>,
    ) -> AstarteMessage {
        AstarteMessage {
            interface_name: iname.into(),
            path: path.into(),
            payload,
            timestamp,
        }
    }

    #[tokio::test]
    async fn subscribe_success() {
        let interface = Interface::from_str(SERV_PROPS_IFACE).unwrap();
        let interfaces = vec![interface.clone()];

        let mut client = DeviceClient::<SqliteStore>::default();

        let mut seq = mockall::Sequence::new();

        client
            .expect_extend_interfaces_vec()
            .once()
            .in_sequence(&mut seq)
            .withf(move |i| *i == interfaces)
            .returning(move |_| Ok(vec![interface.interface_name().to_string()]));

        client
            .expect_server_props()
            .once()
            .in_sequence(&mut seq)
            .returning(|| Ok(Vec::new()));

        let interfaces = InterfacesJson::from_iter(vec![SERV_PROPS_IFACE.to_string()]);

        let astarte_node = AstarteNode::from_json(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            &interfaces,
        )
        .unwrap();

        let astarte_handler = DevicePublisher::new(client, SubscribersMap::default());

        let result = astarte_handler.subscribe(&astarte_node).await;
        assert!(
            result.is_ok(),
            "error {}",
            result.unwrap_err().source().unwrap().source().unwrap()
        );
    }

    #[tokio::test]
    async fn poll_success() {
        let prop_interface = astarte_device_sdk::Interface::from_str(SERV_PROPS_IFACE).unwrap();
        let expected_interface_name = prop_interface.interface_name();
        let interface_string = expected_interface_name.to_string();
        let path = "test";
        let value: i32 = 5;

        let mut client = DeviceClient::<SqliteStore>::default();
        let mut connection = DeviceConnection::<SqliteStore, Grpc<SqliteStore>>::default();

        let interfaces = InterfacesJson::from_iter(vec![SERV_PROPS_IFACE.to_string()]);

        let astarte_node = AstarteNode::from_json(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            &interfaces,
        )
        .unwrap();

        connection.expect_handle_events().returning(|| Ok(()));

        let mut seq = mockall::Sequence::new();

        let i_cl = prop_interface.clone();
        let v = vec![i_cl.clone()];
        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::default();

                let name = i_cl.interface_name().to_string();
                client
                    .expect_extend_interfaces_vec()
                    .once()
                    .in_sequence(&mut seq)
                    .with(mockall::predicate::eq(v.clone()))
                    .returning(move |_| Ok(vec![name.clone()]));

                client
                    .expect_server_props()
                    .once()
                    .in_sequence(&mut seq)
                    .returning(|| Ok(Vec::new()));

                client
            });

        let mut seq = mockall::Sequence::new();

        client
            .expect_recv()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                Ok(DeviceEvent {
                    interface: interface_string.clone(),
                    path: path.to_string(),
                    data: Value::Individual(value.into()),
                })
            });
        client
            .expect_recv()
            .once()
            .in_sequence(&mut seq)
            .returning(|| Err(RecvError::Disconnected));

        let (publisher, mut subscriber) = init_pub_sub(client);

        let handle = tokio::spawn(async move { subscriber.forward_events().await });

        let subscribe_result = publisher.subscribe(&astarte_node).await;
        assert!(subscribe_result.is_ok());

        let (mut subscription, _) = subscribe_result.unwrap();

        let astarte_message = subscription
            .receiver
            .recv()
            .await
            .unwrap()
            .unwrap()
            .take_message()
            .unwrap();

        assert_eq!(expected_interface_name, astarte_message.interface_name);
        assert_eq!(path, astarte_message.path);

        let individual_data = astarte_message
            .take_data()
            .and_then(|data| data.take_individual())
            .and_then(|data| data.individual_data)
            .and_then(|data| data.into())
            .unwrap();

        assert_eq!(IndividualData::AstarteInteger(value), individual_data);

        let err = handle.await.unwrap().unwrap_err();

        assert!(matches!(err, DeviceError::Disconnected));
    }

    async fn recv_proto_error(sub: &mut Subscription) -> Option<MessageHubError> {
        let res =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), sub.receiver.recv()).await;

        match res {
            Ok(e) => e.unwrap().unwrap().take_error(),
            Err(elapsed) => {
                debug!("timeout occurred: {elapsed}");
                None
            }
        }
    }

    #[tokio::test]
    async fn poll_failed_with_proto_error() {
        let prop_interface = astarte_device_sdk::Interface::from_str(SERV_PROPS_IFACE).unwrap();

        let mut client = DeviceClient::<SqliteStore>::default();
        let mut connection = DeviceConnection::<SqliteStore, Grpc<SqliteStore>>::default();

        let interfaces = InterfacesJson::from_iter(vec![SERV_PROPS_IFACE.to_string()]);

        // we define two nodes with the same introspection
        let astarte_node_1 = AstarteNode::from_json(
            "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            &interfaces,
        )
        .unwrap();

        let astarte_node_2 = AstarteNode::from_json(
            "550e8400-e29b-41d4-a716-446655440002".parse().unwrap(),
            &interfaces,
        )
        .unwrap();

        // we define a third node with a different introspection compared to the others
        let empty_interfaces = InterfacesJson::from_iter(vec![]);

        let astarte_node_3 = AstarteNode::from_json(
            "550e8400-e29b-41d4-a716-446655440003".parse().unwrap(),
            &empty_interfaces,
        )
        .unwrap();

        connection.expect_handle_events().returning(|| Ok(()));

        let mut seq = mockall::Sequence::new();

        let i_cl = prop_interface.clone();
        let v = vec![i_cl.clone()];
        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::default();

                let name = i_cl.interface_name().to_string();
                let name_cl1 = name.clone();
                let name_cl2 = name.clone();

                // subscription of the two nodes
                client
                    .expect_extend_interfaces_vec()
                    .once()
                    .in_sequence(&mut seq)
                    .with(mockall::predicate::eq(v.clone()))
                    .returning(move |_| Ok(vec![name_cl1.clone()]));

                client
                    .expect_server_props()
                    .once()
                    .in_sequence(&mut seq)
                    .returning(|| Ok(vec![]));

                client
                    .expect_extend_interfaces_vec()
                    .once()
                    .in_sequence(&mut seq)
                    .with(mockall::predicate::eq(v.clone()))
                    .returning(move |_| Ok(vec![name_cl2.clone()]));

                client
                    .expect_server_props()
                    .once()
                    .in_sequence(&mut seq)
                    .returning(|| Ok(vec![]));

                // subscription of the third node
                client
                    .expect_extend_interfaces_vec()
                    .once()
                    .in_sequence(&mut seq)
                    .with(mockall::predicate::eq(vec![]))
                    .returning(move |_| Ok(vec![]));

                client
                    .expect_server_props()
                    .once()
                    .in_sequence(&mut seq)
                    .returning(|| Ok(vec![]));

                client
            });

        let mut seq = mockall::Sequence::new();

        // we must check that the RecvError is received by all the nodes having SERV_PROPS_IFACE in
        // their introspection
        client
            .expect_recv()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                Err(RecvError::MappingNotFound {
                    interface: SERV_PROPS_IFACE.to_string(),
                    mapping: "test".to_string(),
                })
            });

        let (publisher, mut subscriber) = init_pub_sub(client);

        tokio::spawn(async move { subscriber.forward_events().await });

        let subscribe_1_result = publisher.subscribe(&astarte_node_1).await;
        let subscribe_2_result = publisher.subscribe(&astarte_node_2).await;
        let subscribe_3_result = publisher.subscribe(&astarte_node_3).await;
        assert!(subscribe_1_result.is_ok());
        assert!(subscribe_2_result.is_ok());
        assert!(subscribe_3_result.is_ok());

        let (mut subscription_1, _) = subscribe_1_result.unwrap();
        let (mut subscription_2, _) = subscribe_2_result.unwrap();
        let (mut subscription_3, _) = subscribe_3_result.unwrap();

        let message_hub_err = recv_proto_error(&mut subscription_1).await.unwrap();

        assert!(message_hub_err
            .description
            .contains("couldn't find mapping test in interface"));

        // check that also the second node received this message
        let message_hub_err = recv_proto_error(&mut subscription_2).await.unwrap();

        assert!(message_hub_err
            .description
            .contains("couldn't find mapping test in interface"));

        // the third node should not receive the error since it has different interfaces in its
        // introspection
        let message_hub_err = recv_proto_error(&mut subscription_3).await;

        assert!(message_hub_err.is_none());
    }

    #[tokio::test]
    async fn poll_failed_with_proto_error_broadcast() {
        let prop_interface = astarte_device_sdk::Interface::from_str(SERV_PROPS_IFACE).unwrap();

        let mut client = DeviceClient::<SqliteStore>::default();
        let mut connection = DeviceConnection::<SqliteStore, Grpc<SqliteStore>>::default();

        // we define two nodes with the same introspection and test that a RecvError (except for a
        // MappingNotFound one) is broadcast to all the subscribed nodes, even those with a
        // different introspection
        let interfaces = InterfacesJson::from_iter(vec![SERV_PROPS_IFACE.to_string()]);
        let astarte_node_1 = AstarteNode::from_json(
            "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            &interfaces,
        )
        .unwrap();

        let empty_interfaces = InterfacesJson::from_iter(vec![]);
        let astarte_node_2 = AstarteNode::from_json(
            "550e8400-e29b-41d4-a716-446655440002".parse().unwrap(),
            &empty_interfaces,
        )
        .unwrap();

        connection.expect_handle_events().returning(|| Ok(()));

        let mut seq = mockall::Sequence::new();

        let i_cl = prop_interface.clone();
        let v = vec![i_cl.clone()];
        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::default();

                let name = i_cl.interface_name().to_string();

                // subscribe the first node
                client
                    .expect_extend_interfaces_vec()
                    .once()
                    .in_sequence(&mut seq)
                    .with(mockall::predicate::eq(v.clone()))
                    .returning(move |_| Ok(vec![name.clone()]));

                client
                    .expect_server_props()
                    .once()
                    .in_sequence(&mut seq)
                    .returning(|| Ok(vec![]));

                // subscription of the second node
                client
                    .expect_extend_interfaces_vec()
                    .once()
                    .in_sequence(&mut seq)
                    .with(mockall::predicate::eq(vec![]))
                    .returning(move |_| Ok(vec![]));

                client
                    .expect_server_props()
                    .once()
                    .in_sequence(&mut seq)
                    .returning(|| Ok(vec![]));

                client
            });

        let mut seq = mockall::Sequence::new();

        // we must check that the RecvError is received by all the nodes having SERV_PROPS_IFACE in
        // their introspection
        client
            .expect_recv()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                Err(RecvError::InterfaceNotFound {
                    name: "wrong interface name".to_string(),
                })
            });

        let (publisher, mut subscriber) = init_pub_sub(client);

        tokio::spawn(async move { subscriber.forward_events().await });

        let subscribe_1_result = publisher.subscribe(&astarte_node_1).await;
        let subscribe_2_result = publisher.subscribe(&astarte_node_2).await;
        assert!(subscribe_1_result.is_ok());
        assert!(subscribe_2_result.is_ok());

        let (mut subscription_1, _) = subscribe_1_result.unwrap();
        let (mut subscription_2, _) = subscribe_2_result.unwrap();

        // check that all the nodes received the error message
        let message_hub_err = recv_proto_error(&mut subscription_1).await.unwrap();
        assert!(message_hub_err
            .description
            .contains("couldn't find interface"));

        let message_hub_err = recv_proto_error(&mut subscription_2).await.unwrap();
        assert!(message_hub_err
            .description
            .contains("couldn't find interface"));
    }

    #[tokio::test]
    async fn poll_failed_with_astarte_error() {
        let mut client = DeviceClient::<SqliteStore>::default();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(DeviceClient::<SqliteStore>::default);

        // Simulate disconnect
        client
            .expect_recv()
            .once()
            .in_sequence(&mut seq)
            .returning(|| Err(RecvError::Disconnected));

        let (_publisher, mut subscriber) = init_pub_sub(client);

        let err = subscriber.forward_events().await.unwrap_err();
        assert!(matches!(err, DeviceError::Disconnected));
    }

    #[tokio::test]
    async fn publish_failed_with_invalid_payload() {
        let mut client = DeviceClient::<SqliteStore>::new();

        let expected_interface_name = "io.demo.Properties";

        let astarte_message = create_astarte_message(expected_interface_name, "/test", None, None);

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(DeviceClient::<SqliteStore>::default);

        let (publisher, _subscriber) = init_pub_sub(client);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_err());

        let result_err = result.err().unwrap();
        assert!(matches!(
            result_err,
            AstarteMessageHubError::AstarteInvalidData(_),
        ));
    }

    #[tokio::test]
    async fn publish_failed_with_invalid_astarte_data() {
        let mut client = DeviceClient::<SqliteStore>::default();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(DeviceClient::<SqliteStore>::default);

        let astarte_message = create_astarte_message("io.demo.Properties", "/test", None, None);

        let (publisher, _) = init_pub_sub(client);

        let result = publisher.publish(&astarte_message).await;

        let result_err = result.unwrap_err();
        assert!(matches!(
            result_err,
            AstarteMessageHubError::AstarteInvalidData(_),
        ));
    }

    #[tokio::test]
    async fn publish_individual_success() {
        let mut client = DeviceClient::<SqliteStore>::default();

        let mut seq = mockall::Sequence::new();

        let expected_interface_name = "io.demo.Properties";

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteData(5.into())),
            None,
        );

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::default();

                client
                    .expect_send()
                    .once()
                    .in_sequence(&mut seq)
                    .withf(move |interface_name: &str, _: &str, _: &AstarteType| {
                        interface_name == expected_interface_name
                    })
                    .returning(|_: &str, _: &str, _: AstarteType| Ok(()));
                client
            });

        let (publisher, _subscriber) = init_pub_sub(client);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_individual_with_timestamp_success() {
        let expected_interface_name = "io.demo.Properties";

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteData(5.into())),
            Some(Utc::now().into()),
        );

        let mut client = DeviceClient::<SqliteStore>::default();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::default();

                client
                    .expect_send_with_timestamp()
                    .once()
                    .in_sequence(&mut seq)
                    .withf(
                        move |interface_name: &str,
                              _: &str,
                              _: &AstarteType,
                              _: &chrono::DateTime<Utc>| {
                            interface_name == expected_interface_name
                        },
                    )
                    .returning(|_: &str, _: &str, _: AstarteType, _: chrono::DateTime<Utc>| Ok(()));

                client
            });

        let (publisher, _subscriber) = init_pub_sub(client);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_individual_failed() {
        let expected_interface_name = "io.demo.Properties";

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteData(5.into())),
            None,
        );

        let mut client = DeviceClient::<SqliteStore>::new();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::new();

                client
                    .expect_send()
                    .once()
                    .in_sequence(&mut seq)
                    .withf(move |interface_name: &str, _: &str, _: &AstarteType| {
                        interface_name == expected_interface_name
                    })
                    .returning(|_: &str, _: &str, _: AstarteType| {
                        Err(AstarteSdkError::MappingNotFound {
                            interface: expected_interface_name.to_string(),
                            mapping: String::new(),
                        })
                    });

                client
            });

        let (publisher, _subscriber) = init_pub_sub(client);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_err());

        let result_err = result.unwrap_err();
        assert!(matches!(
            result_err,
            AstarteMessageHubError::Astarte(AstarteSdkError::MappingNotFound {
                interface: _,
                mapping: _,
            }),
        ));
    }

    #[tokio::test]
    async fn publish_individual_with_timestamp_failed() {
        let expected_interface_name = "io.demo.Properties";

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteData(5.into())),
            Some(Utc::now().into()),
        );

        let mut client = DeviceClient::<SqliteStore>::default();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::default();

                client
                    .expect_send_with_timestamp()
                    .once()
                    .in_sequence(&mut seq)
                    .withf(
                        move |interface_name: &str,
                              _: &str,
                              _: &AstarteType,
                              _: &chrono::DateTime<Utc>| {
                            interface_name == expected_interface_name
                        },
                    )
                    .returning(
                        |_: &str, _: &str, _: AstarteType, _: chrono::DateTime<Utc>| {
                            Err(AstarteSdkError::MappingNotFound {
                                interface: expected_interface_name.to_string(),
                                mapping: String::new(),
                            })
                        },
                    );

                client
            });

        let (publisher, _subscriber) = init_pub_sub(client);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_err());

        let result_err = result.err().unwrap();
        assert!(matches!(
            result_err,
            AstarteMessageHubError::Astarte(AstarteSdkError::MappingNotFound {
                interface: _,
                mapping: _,
            }),
        ));
    }

    #[tokio::test]
    async fn publish_object_success() {
        let expected_interface_name = "io.demo.Object";

        let expected_i32 = 5;
        let expected_f64 = 5.12;
        let mut map_val: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        map_val.insert("i32".to_owned(), expected_i32.into());
        map_val.insert("f64".to_owned(), expected_f64.into());

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteData(map_val.into())),
            None,
        );

        let mut client = DeviceClient::<SqliteStore>::default();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::new();

                client
                    .expect_send_object()
                    .once()
                    .in_sequence(&mut seq)
                    .withf(
                        move |interface_name: &str, _: &str, _: &HashMap<String, AstarteType>| {
                            interface_name == expected_interface_name
                        },
                    )
                    .returning(|_: &str, _: &str, _: HashMap<String, AstarteType>| Ok(()));

                client
            });

        let (publisher, _subscriber) = init_pub_sub(client);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_object_with_timestamp_success() {
        let expected_interface_name = "io.demo.Object";

        let expected_i32 = 5;
        let expected_f64 = 5.12;
        let mut map_val: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        map_val.insert("i32".to_owned(), expected_i32.into());
        map_val.insert("f64".to_owned(), expected_f64.into());

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteData(map_val.into())),
            Some(Utc::now().into()),
        );

        let mut client = DeviceClient::<SqliteStore>::default();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::default();

                client
                    .expect_send_object_with_timestamp()
                    .once()
                    .in_sequence(&mut seq)
                    .withf(
                        move |interface_name: &str,
                              _: &str,
                              _: &HashMap<String, AstarteType>,
                              _: &chrono::DateTime<Utc>| {
                            interface_name == expected_interface_name
                        },
                    )
                    .returning(
                        |_: &str,
                         _: &str,
                         _: HashMap<String, AstarteType>,
                         _: chrono::DateTime<Utc>| Ok(()),
                    );

                client
            });

        let (publisher, _subscriber) = init_pub_sub(client);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_object_failed() {
        let expected_interface_name = "io.demo.Object";

        let expected_i32 = 5;
        let expected_f64 = 5.12;
        let mut map_val: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        map_val.insert("i32".to_owned(), expected_i32.into());
        map_val.insert("f64".to_owned(), expected_f64.into());

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteData(map_val.into())),
            None,
        );

        let mut client = DeviceClient::<SqliteStore>::new();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::new();

                client
                    .expect_send_object()
                    .once()
                    .in_sequence(&mut seq)
                    .withf(
                        move |interface_name: &str, _: &str, _: &HashMap<String, AstarteType>| {
                            interface_name == expected_interface_name
                        },
                    )
                    .returning(|_: &str, _: &str, _: HashMap<String, AstarteType>| {
                        Err(AstarteSdkError::MappingNotFound {
                            interface: expected_interface_name.to_string(),
                            mapping: String::new(),
                        })
                    });

                client
            });

        let (publisher, _subscriber) = init_pub_sub(client);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_err());

        let result_err = result.err().unwrap();
        assert!(matches!(
            result_err,
            AstarteMessageHubError::Astarte(AstarteSdkError::MappingNotFound {
                interface: _,
                mapping: _,
            }),
        ));
    }

    #[tokio::test]
    async fn publish_object_with_timestamp_failed() {
        let expected_interface_name = "io.demo.Object";

        let expected_i32 = 5;
        let expected_f64 = 5.12;
        let mut map_val: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        map_val.insert("i32".to_owned(), expected_i32.into());
        map_val.insert("f64".to_owned(), expected_f64.into());

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteData(map_val.into())),
            Some(Utc::now().into()),
        );

        let mut client = DeviceClient::<SqliteStore>::default();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::default();

                client
                    .expect_send_object_with_timestamp()
                    .once()
                    .in_sequence(&mut seq)
                    .withf(
                        move |interface_name: &str,
                              _: &str,
                              _: &HashMap<String, AstarteType>,
                              _: &chrono::DateTime<Utc>| {
                            interface_name == expected_interface_name
                        },
                    )
                    .returning(
                        |_: &str,
                         _: &str,
                         _: HashMap<String, AstarteType>,
                         _: chrono::DateTime<Utc>| {
                            Err(AstarteSdkError::MappingNotFound {
                                interface: expected_interface_name.to_string(),
                                mapping: String::new(),
                            })
                        },
                    );

                client
            });

        let (publisher, _subscriber) = init_pub_sub(client);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_err());

        let result_err = result.err().unwrap();
        assert!(matches!(
            result_err,
            AstarteMessageHubError::Astarte(AstarteSdkError::MappingNotFound {
                interface: _,
                mapping: _,
            }),
        ));
    }

    #[tokio::test]
    async fn publish_unset_success() {
        let expected_interface_name = "io.demo.Object";

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteUnset(AstarteUnset {})),
            None,
        );

        let mut client = DeviceClient::<SqliteStore>::default();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::new();

                client
                    .expect_unset()
                    .once()
                    .in_sequence(&mut seq)
                    .withf(move |interface_name: &str, _: &str| {
                        interface_name == expected_interface_name
                    })
                    .returning(|_: &str, _: &str| Ok(()));

                client
            });

        let (publisher, _subscriber) = init_pub_sub(client);
        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_unset_failed() {
        let expected_interface_name = "io.demo.Object";

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteUnset(AstarteUnset {})),
            None,
        );

        let mut client = DeviceClient::<SqliteStore>::default();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::default();

                client
                    .expect_unset()
                    .withf(move |interface_name: &str, _: &str| {
                        interface_name == expected_interface_name
                    })
                    .once()
                    .in_sequence(&mut seq)
                    .returning(|_: &str, _: &str| {
                        Err(AstarteSdkError::MappingNotFound {
                            interface: expected_interface_name.to_string(),
                            mapping: String::new(),
                        })
                    });

                client
            });

        let (publisher, _subscriber) = init_pub_sub(client);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_err());

        let result_err = result.err().unwrap();
        assert!(matches!(
            result_err,
            AstarteMessageHubError::Astarte(AstarteSdkError::MappingNotFound {
                interface: _,
                mapping: _,
            }),
        ));
    }

    #[tokio::test]
    async fn detach_node_success() {
        let interfaces = InterfacesJson::from_iter(vec![
            SERV_PROPS_IFACE.to_string(),
            SERV_OBJ_IFACE.to_string(),
        ]);

        let mut client = DeviceClient::<SqliteStore>::default();
        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::default();

                client
                    .expect_extend_interfaces_vec()
                    .once()
                    .in_sequence(&mut seq)
                    .returning(|_| {
                        Ok(vec![
                            "org.astarte-platform.test.test".to_string(),
                            "com.test.object".to_string(),
                        ])
                    });

                client
                    .expect_server_props()
                    .once()
                    .in_sequence(&mut seq)
                    .returning(|| Ok(Vec::new()));

                client
                    .expect_remove_interfaces()
                    .once()
                    .in_sequence(&mut seq)
                    .returning(|_: Vec<String>| {
                        Ok(vec![
                            "org.astarte-platform.test.test".to_string(),
                            "com.test.object".to_string(),
                        ])
                    });

                client
            });

        let astarte_node = AstarteNode::from_json(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            &interfaces,
        )
        .unwrap();

        let (publisher, _subscriber) = init_pub_sub(client);

        let result = publisher.subscribe(&astarte_node).await;
        assert!(result.is_ok());

        let detach_result = publisher.unsubscribe(&astarte_node.id).await;
        assert!(detach_result.is_ok())
    }

    #[tokio::test]
    async fn detach_node_unsubscribe_failed() {
        let interfaces = InterfacesJson::from_iter(vec![SERV_PROPS_IFACE.to_string()]);

        let mut client = DeviceClient::<SqliteStore>::new();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(DeviceClient::<SqliteStore>::default);

        let astarte_node = AstarteNode::from_json(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            &interfaces,
        )
        .unwrap();

        let (publisher, _subscriber) = init_pub_sub(client);

        let detach_result = publisher.unsubscribe(&astarte_node.id).await;

        assert!(detach_result.is_err());
        let err = detach_result.unwrap_err();
        assert!(
            matches!(err, AstarteMessageHubError::NodeId(_)),
            "actual {err:?}",
        )
    }
}
