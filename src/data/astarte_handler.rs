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

use astarte_device_sdk::transport::grpc::convert::MessageHubProtoError;
use astarte_device_sdk::types::AstarteType;
#[cfg(not(test))]
use astarte_device_sdk::Client;
use astarte_device_sdk::{AstarteDeviceDataEvent, Error as AstarteError, EventReceiver};
use astarte_message_hub_proto::astarte_data_type::Data;
use astarte_message_hub_proto::astarte_message::Payload;
use astarte_message_hub_proto::{AstarteDataTypeIndividual, AstarteMessage};
use async_trait::async_trait;
use log::error;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tonic::Status;
use uuid::Uuid;

use crate::astarte_message_hub::AstarteNode;
use crate::data::astarte::{AstartePublisher, AstarteSubscriber};
#[cfg(test)]
use crate::data::mock_astarte_sdk::Client;
use crate::error::AstarteMessageHubError;

type SubscribersMap = Arc<RwLock<HashMap<Uuid, Subscriber>>>;

/// Initialize the [`DevicePublisher`] and [`DeviceSubscriber`]
pub fn init_pub_sub<T>(device: T, rx: EventReceiver) -> (DevicePublisher<T>, DeviceSubscriber)
where
    T: Client + Send + Sync,
{
    let subscribers: SubscribersMap = Arc::new(Default::default());

    (
        DevicePublisher::new(device, subscribers.clone()),
        DeviceSubscriber::new(rx, subscribers),
    )
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum DeviceError {
    /// received error from Astarte
    Astarte(#[source] AstarteError),
    /// couldn't convert the astarte event into a proto message
    Convert(#[from] MessageHubProtoError),
    /// subscriber already disconnected
    Subscriber,
}

/// Receiver for the [`AstarteDeviceDataEvent`].
///
/// It will forward the events to the registered subscriber.
pub struct DeviceSubscriber {
    rx: EventReceiver,
    subscribers: SubscribersMap,
}

impl DeviceSubscriber {
    /// Create a new subscriber.
    fn new(rx: EventReceiver, subscribers: SubscribersMap) -> Self {
        Self { rx, subscribers }
    }

    /// Handler responsible for receiving and managing [AstarteDeviceDataEvent](astarte_device_sdk::AstarteDeviceDataEvent)s
    pub async fn forward_events(&mut self) -> Result<(), DeviceError> {
        while let Some(event) = self.rx.recv().await {
            if let Err(err) = self.on_event(event).await {
                error!("error on event receive: {err}");
            }
        }

        Err(DeviceError::Subscriber)
    }

    async fn on_event(
        &mut self,
        event: Result<AstarteDeviceDataEvent, AstarteError>,
    ) -> Result<(), DeviceError> {
        let event = event.map_err(DeviceError::Astarte)?;

        let itf_name = event.interface.clone();

        let msg = AstarteMessage::try_from(event)?;

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
                .map_err(|_| DeviceError::Subscriber)?;
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
/// Uses the [`Astarte Device SDK`](astarte_device_sdk::AstarteDeviceSdk) to provide subscribe and publish functionality.
#[derive(Clone)]
pub struct DevicePublisher<T> {
    device_sdk: T,
    subscribers: SubscribersMap,
}

impl<T> DevicePublisher<T>
where
    T: Client + Send + Sync,
{
    /// Constructs a new handler from the [AstarteDeviceSdk](astarte_device_sdk::AstarteDeviceSdk)
    #[allow(dead_code)]
    fn new(device_sdk: T, subscribers: SubscribersMap) -> Self {
        DevicePublisher {
            device_sdk,
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
            self.device_sdk
                .send_with_timestamp(interface_name, path, astarte_type, timestamp)
                .await
        } else {
            self.device_sdk
                .send(interface_name, path, astarte_type)
                .await
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
        let astarte_data_individual_map: HashMap<String, AstarteDataTypeIndividual> =
            object_data.object_data;

        // TODO: move `map_values_to_astarte_type` from grpc::convert to mod.rs
        let aggr = astarte_device_sdk::transport::grpc::convert::map_values_to_astarte_type(
            astarte_data_individual_map,
        )?;

        if let Some(timestamp) = timestamp {
            let timestamp = timestamp
                .try_into()
                .map_err(AstarteMessageHubError::Timestamp)?;
            self.device_sdk
                .send_object_with_timestamp(interface_name, path, aggr, timestamp)
                .await
        } else {
            self.device_sdk
                .send_object(interface_name, path, aggr)
                .await
        }
        .map_err(AstarteMessageHubError::AstarteError)
    }
}

#[async_trait]
impl<T> AstarteSubscriber for DevicePublisher<T>
where
    T: Client + Sync + Send,
{
    async fn subscribe(
        &self,
        astarte_node: &AstarteNode,
    ) -> Result<Receiver<Result<AstarteMessage, Status>>, AstarteMessageHubError> {
        use astarte_device_sdk::Interface;

        let (tx, rx) = channel(32);

        let mut astarte_interfaces: Vec<Interface> = vec![];

        for interface in astarte_node.introspection.iter() {
            let astarte_interface: Interface = interface.clone().try_into()?;
            self.device_sdk
                .add_interface(astarte_interface.clone())
                .await?;

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
            self.device_sdk
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
impl<T> AstartePublisher for DevicePublisher<T>
where
    T: Client + Send + Sync,
{
    async fn publish(
        &self,
        astarte_message: &AstarteMessage,
    ) -> Result<(), AstarteMessageHubError> {
        let astarte_message_payload = astarte_message.clone().payload.ok_or_else(|| {
            AstarteMessageHubError::AstarteInvalidData("Invalid payload".to_string())
        })?;

        match astarte_message_payload {
            Payload::AstarteUnset(_) => self
                .device_sdk
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

    use astarte_device_sdk::Aggregation;
    use std::str::FromStr;

    use astarte_device_sdk::error::Error as AstarteSdkError;
    use astarte_device_sdk::store::memory::MemoryStore;
    use astarte_device_sdk::transport::mqtt::Mqtt;
    use astarte_message_hub_proto::astarte_data_type_individual::IndividualData;
    use astarte_message_hub_proto::AstarteUnset;
    use chrono::Utc;
    use pbjson_types::Timestamp;

    use crate::data::mock_astarte_sdk::MockAstarteDeviceSdk;

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
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();

        device_sdk
            .expect_add_interface()
            .withf(|i| i.interface_name() == "org.astarte-platform.test.test")
            .returning(|_| Ok(()));

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let astarte_handler = DevicePublisher::new(device_sdk, Default::default());

        let result = astarte_handler.subscribe(&astarte_node).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn subscribe_failed_invalid_interface() {
        let device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
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

        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (tx_event, rx_event) = tokio::sync::mpsc::channel(2);

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        device_sdk.expect_handle_events().returning(|| Ok(()));
        device_sdk
            .expect_add_interface()
            .withf(|i| i.interface_name() == "org.astarte-platform.test.test")
            .returning(|_| Ok(()));

        tx_event
            .send(Ok(AstarteDeviceDataEvent {
                interface: interface_string.clone(),
                path: path.to_string(),
                data: Aggregation::Individual(value.into()),
            }))
            .await
            .unwrap();

        let (publisher, mut subscriber) = init_pub_sub(device_sdk, rx_event);

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

        handle.abort();

        assert!(handle.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn poll_failed_with_astarte_error() {
        let device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();

        let (tx, rx_event) = tokio::sync::mpsc::channel(2);

        let (_publisher, mut subscriber) = init_pub_sub(device_sdk, rx_event);

        // Simulate disconnect
        drop(tx);

        assert!(matches!(
            subscriber.forward_events().await.unwrap_err(),
            DeviceError::Subscriber
        ));
    }

    #[tokio::test]
    async fn publish_failed_with_invalid_payload() {
        let device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        let expected_interface_name = "io.demo.Properties";

        let astarte_message = create_astarte_message(expected_interface_name, "/test", None, None);

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);

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
        let device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        let astarte_message = create_astarte_message("io.demo.Properties", "/test", None, None);

        let (publisher, _) = init_pub_sub(device_sdk, rx_event);

        let result = publisher.publish(&astarte_message).await;

        let result_err = result.unwrap_err();
        assert!(matches!(
            result_err,
            AstarteMessageHubError::AstarteInvalidData(_),
        ));
    }

    #[tokio::test]
    async fn publish_individual_success() {
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        let expected_interface_name = "io.demo.Properties";

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteData(5.into())),
            None,
        );

        device_sdk
            .expect_send()
            .withf(move |interface_name: &str, _: &str, _: &AstarteType| {
                interface_name == expected_interface_name
            })
            .returning(|_: &str, _: &str, _: AstarteType| Ok(()));

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_individual_with_timestamp_success() {
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        let expected_interface_name = "io.demo.Properties";

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteData(5.into())),
            Some(Utc::now().into()),
        );

        device_sdk
            .expect_send_with_timestamp()
            .withf(
                move |interface_name: &str, _: &str, _: &AstarteType, _: &chrono::DateTime<Utc>| {
                    interface_name == expected_interface_name
                },
            )
            .returning(|_: &str, _: &str, _: AstarteType, _: chrono::DateTime<Utc>| Ok(()));

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_individual_failed() {
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        let expected_interface_name = "io.demo.Properties";

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteData(5.into())),
            None,
        );

        device_sdk
            .expect_send()
            .withf(move |interface_name: &str, _: &str, _: &AstarteType| {
                interface_name == expected_interface_name
            })
            .returning(|_: &str, _: &str, _: AstarteType| {
                Err(AstarteSdkError::MappingNotFound {
                    interface: expected_interface_name.to_string(),
                    mapping: String::new(),
                })
            });

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);

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
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        let expected_interface_name = "io.demo.Properties";

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteData(5.into())),
            Some(Utc::now().into()),
        );

        device_sdk
            .expect_send_with_timestamp()
            .withf(
                move |interface_name: &str, _: &str, _: &AstarteType, _: &chrono::DateTime<Utc>| {
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

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);

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
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

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

        device_sdk
            .expect_send_object()
            .withf(
                move |interface_name: &str, _: &str, _: &HashMap<String, AstarteType>| {
                    interface_name == expected_interface_name
                },
            )
            .returning(|_: &str, _: &str, _: HashMap<String, AstarteType>| Ok(()));

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_object_with_timestamp_success() {
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

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

        device_sdk
             .expect_send_object_with_timestamp()
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

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);

        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_object_failed() {
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

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

        device_sdk
            .expect_send_object()
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

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);

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
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

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

        device_sdk
            .expect_send_object_with_timestamp()
            .withf(
                move |interface_name: &str,
                      _: &str,
                      _: &HashMap<String, AstarteType>,
                      _: &chrono::DateTime<Utc>| {
                    interface_name == expected_interface_name
                },
            )
            .returning(
                |_: &str, _: &str, _: HashMap<String, AstarteType>, _: chrono::DateTime<Utc>| {
                    Err(AstarteSdkError::MappingNotFound {
                        interface: expected_interface_name.to_string(),
                        mapping: String::new(),
                    })
                },
            );

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);

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
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        let expected_interface_name = "io.demo.Object";

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteUnset(AstarteUnset {})),
            None,
        );

        device_sdk
            .expect_unset()
            .withf(move |interface_name: &str, _: &str| interface_name == expected_interface_name)
            .returning(|_: &str, _: &str| Ok(()));

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);
        let result = publisher.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_unset_failed() {
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        let expected_interface_name = "io.demo.Object";

        let astarte_message = create_astarte_message(
            expected_interface_name,
            "/test",
            Some(Payload::AstarteUnset(AstarteUnset {})),
            None,
        );

        device_sdk
            .expect_unset()
            .withf(move |interface_name: &str, _: &str| interface_name == expected_interface_name)
            .returning(|_: &str, _: &str| {
                Err(AstarteSdkError::MappingNotFound {
                    interface: expected_interface_name.to_string(),
                    mapping: String::new(),
                })
            });

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);

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

        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        device_sdk.expect_add_interface().returning(|_| Ok(()));
        device_sdk.expect_remove_interface().returning(|_| Ok(()));

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);

        let result = publisher.subscribe(&astarte_node).await;
        assert!(result.is_ok());

        let detach_result = publisher.unsubscribe(&astarte_node).await;
        assert!(detach_result.is_ok())
    }

    #[tokio::test]
    async fn detach_node_unsubscribe_failed() {
        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        device_sdk.expect_add_interface().returning(|_| Ok(()));
        device_sdk.expect_remove_interface().returning(|_| Ok(()));

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let (publisher, _subscriber) = init_pub_sub(device_sdk, rx_event);

        let detach_result = publisher.unsubscribe(&astarte_node).await;

        assert!(detach_result.is_err());
        assert!(matches!(
            detach_result.unwrap_err(),
            AstarteMessageHubError::AstarteInvalidData(_)
        ))
    }
}
