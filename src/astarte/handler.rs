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

use std::collections::HashMap;
use std::sync::Arc;

use astarte_device_sdk::{
    store::SqliteStore,
    transport::grpc::convert::{map_values_to_astarte_type, MessageHubProtoError},
    types::AstarteType,
    DeviceEvent, Error as AstarteError,
};
use astarte_message_hub_proto::{
    astarte_data_type::Data, astarte_message::Payload, AstarteDataTypeIndividual, AstarteMessage,
};
use async_trait::async_trait;
use log::error;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tonic::Status;
use uuid::Uuid;

use super::sdk::{Client, DeviceClient, DynamicIntrospection};
use super::{AstartePublisher, AstarteSubscriber};
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
                Err(AstarteError::Disconnected) => return Err(DeviceError::Disconnected),
                event => {
                    if let Err(err) = self.on_event(event).await {
                        error!("error on event receive: {err}");
                    }
                }
            }
        }
    }

    async fn on_event(
        &mut self,
        event: Result<DeviceEvent, AstarteError>,
    ) -> Result<(), DeviceError> {
        let event = event.map_err(DeviceError::Astarte)?;

        let itf_name = event.interface.clone();

        let msg = AstarteMessage::from(event);

        let sub = self.subscribers.read().await;
        let subscribers = sub.iter().filter_map(|(_, subscriber)| {
            subscriber
                .introspection
                .iter()
                .any(|interface| interface.interface_name() == itf_name)
                .then_some(subscriber)
        });

        for subscriber in subscribers {
            subscriber
                .sender
                // This is needed to satisfy the tonic trait
                .send(Ok(msg.clone()))
                .await
                .map_err(|_| DeviceError::Disconnected)?;
        }

        Ok(())
    }
}

/// A subscriber for the Astarte handler.
#[derive(Clone)]
pub struct Subscriber {
    introspection: Vec<astarte_device_sdk::Interface>,
    sender: Sender<Result<AstarteMessage, Status>>,
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
        .map_err(AstarteMessageHubError::AstarteError)
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
        .map_err(AstarteMessageHubError::AstarteError)
    }
}

#[async_trait]
impl AstarteSubscriber for DevicePublisher {
    async fn subscribe(
        &self,
        astarte_node: &AstarteNode,
    ) -> Result<Receiver<Result<AstarteMessage, Status>>, AstarteMessageHubError> {
        use astarte_device_sdk::Interface;

        let (tx, rx) = channel(32);

        let mut astarte_interfaces: Vec<Interface> = vec![];

        for interface in astarte_node.introspection.iter() {
            let astarte_interface: Interface = interface.clone().try_into()?;
            self.client.add_interface(astarte_interface.clone()).await?;

            astarte_interfaces.push(astarte_interface);
        }

        self.subscribers.write().await.insert(
            astarte_node.id,
            Subscriber {
                introspection: astarte_interfaces,
                sender: tx,
            },
        );

        Ok(rx)
    }

    /// Unsubscribe an existing Node and its introspection from Astarte Message Hub.
    ///
    /// All the interfaces in this node introspection that are not in the introspection of any other node will be removed.
    async fn unsubscribe(&self, astarte_node: &AstarteNode) -> Result<(), AstarteMessageHubError> {
        let interfaces_to_remove = {
            let subscribers_guard = self.subscribers.read().await;
            astarte_node
                .introspection
                .iter()
                .filter_map(|interface| interface.clone().try_into().ok())
                .filter(|interface| {
                    subscribers_guard
                        .iter()
                        .filter(|(id, _)| astarte_node.id.ne(id))
                        .find_map(|(_, subscriber)| {
                            subscriber.introspection.contains(interface).then_some(())
                        })
                        .is_none()
                })
                .collect::<Vec<astarte_device_sdk::Interface>>()
        };

        for interface in interfaces_to_remove.iter() {
            self.client
                .remove_interface(interface.interface_name())
                .await?;
        }

        if self
            .subscribers
            .write()
            .await
            .remove(&astarte_node.id)
            .is_some()
        {
            Ok(())
        } else {
            Err(AstarteMessageHubError::AstarteInvalidData(
                "Unable to find AstarteNode".to_string(),
            ))
        }
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
                .map_err(AstarteMessageHubError::AstarteError),
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

    use astarte_device_sdk::transport::grpc::Grpc;
    use astarte_device_sdk::Value;
    use astarte_device_sdk_mock::MockDeviceConnection as DeviceConnection;
    use std::error::Error;
    use std::str::FromStr;

    use astarte_device_sdk::error::Error as AstarteSdkError;
    use astarte_message_hub_proto::astarte_data_type_individual::IndividualData;
    use astarte_message_hub_proto::AstarteUnset;
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
        let mut device_sdk = DeviceClient::<SqliteStore>::default();

        device_sdk
            .expect_add_interface()
            .withf(|i| i.interface_name() == "org.astarte-platform.test.test")
            .returning(|_| Ok(true));

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let astarte_handler = DevicePublisher::new(device_sdk, Default::default());

        let result = astarte_handler.subscribe(&astarte_node).await;
        assert!(
            result.is_ok(),
            "error {}",
            result.unwrap_err().source().unwrap().source().unwrap()
        );
    }

    #[tokio::test]
    async fn subscribe_failed_invalid_interface() {
        let device_sdk = DeviceClient::<SqliteStore>::default();
        let interfaces = vec!["INVALID".to_string().into_bytes()];

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let astarte_handler = DevicePublisher::new(device_sdk, Default::default());

        let result = astarte_handler.subscribe(&astarte_node).await;
        assert!(result.is_err());

        assert!(matches!(
            result.err().unwrap(),
            AstarteMessageHubError::AstarteError(AstarteSdkError::Interface(_))
        ))
    }

    #[tokio::test]
    async fn poll_success() {
        let prop_interface = astarte_device_sdk::Interface::from_str(SERV_PROPS_IFACE).unwrap();
        let expected_interface_name = prop_interface.interface_name();
        let interface_string = expected_interface_name.to_string();
        let path = "test";
        let value: i32 = 5;

        let mut client = DeviceClient::<SqliteStore>::default();
        let mut connection = DeviceConnection::<SqliteStore, Grpc>::default();

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        connection.expect_handle_events().returning(|| Ok(()));

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::default();

                client
                    .expect_add_interface()
                    .once()
                    .in_sequence(&mut seq)
                    .withf(|i| i.interface_name() == "org.astarte-platform.test.test")
                    .returning(|_| Ok(true));

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
            .returning(|| Err(AstarteError::Disconnected));

        let (publisher, mut subscriber) = init_pub_sub(client);

        let handle = tokio::spawn(async move { subscriber.forward_events().await });

        let subscribe_result = publisher.subscribe(&astarte_node).await;
        assert!(subscribe_result.is_ok());

        let mut rx = subscribe_result.unwrap();

        let astarte_message = rx.recv().await.unwrap().unwrap();

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
            .returning(|| Err(AstarteError::Disconnected));

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
            AstarteMessageHubError::AstarteError(AstarteSdkError::MappingNotFound {
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
            AstarteMessageHubError::AstarteError(AstarteSdkError::MappingNotFound {
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
            AstarteMessageHubError::AstarteError(AstarteSdkError::MappingNotFound {
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
            AstarteMessageHubError::AstarteError(AstarteSdkError::MappingNotFound {
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
            AstarteMessageHubError::AstarteError(AstarteSdkError::MappingNotFound {
                interface: _,
                mapping: _,
            }),
        ));
    }

    #[tokio::test]
    async fn detach_node_success() {
        let interfaces = vec![
            SERV_PROPS_IFACE.to_string().into_bytes(),
            SERV_OBJ_IFACE.to_string().into_bytes(),
        ];

        let mut client = DeviceClient::<SqliteStore>::default();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::default();

                client
                    .expect_add_interface()
                    .times(2)
                    .in_sequence(&mut seq)
                    .returning(|_| Ok(true));
                client
                    .expect_remove_interface()
                    .times(2)
                    .in_sequence(&mut seq)
                    .returning(|_| Ok(true));

                client
            });

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let (publisher, _subscriber) = init_pub_sub(client);

        let result = publisher.subscribe(&astarte_node).await;
        assert!(result.is_ok());

        let detach_result = publisher.unsubscribe(&astarte_node).await;
        assert!(detach_result.is_ok())
    }

    #[tokio::test]
    async fn detach_node_unsubscribe_failed() {
        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let mut client = DeviceClient::<SqliteStore>::new();

        let mut seq = mockall::Sequence::new();

        client
            .expect_clone()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                let mut client = DeviceClient::<SqliteStore>::new();

                client
                    .expect_remove_interface()
                    .once()
                    .in_sequence(&mut seq)
                    .returning(|_| Ok(false));

                client
            });

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let (publisher, _subscriber) = init_pub_sub(client);

        let detach_result = publisher.unsubscribe(&astarte_node).await;

        assert!(detach_result.is_err());
        assert!(matches!(
            detach_result.unwrap_err(),
            AstarteMessageHubError::AstarteInvalidData(_)
        ))
    }
}
