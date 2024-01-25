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

use astarte_device_sdk::types::AstarteType;
#[cfg(not(test))]
use astarte_device_sdk::Client;
use astarte_device_sdk::EventReceiver;
use astarte_message_hub_proto::astarte_data_type::Data;
use astarte_message_hub_proto::astarte_message::Payload;
use astarte_message_hub_proto::{AstarteDataTypeIndividual, AstarteMessage};
use async_trait::async_trait;
use log::warn;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tonic::Status;
use uuid::Uuid;

use crate::astarte_message_hub::AstarteNode;
use crate::data::astarte::{AstartePublisher, AstarteRunner, AstarteSubscriber};
#[cfg(test)]
use crate::data::mock_astarte_sdk::Client;
use crate::error::AstarteMessageHubError;

/// An Astarte Device SDK based implementation of an Astarte handler.
/// Uses the Astarte Device SDK to provide subscribe and publish functionality.
#[derive(Clone)]
pub struct AstarteHandler<T> {
    device_sdk: T,
    subscribers: Arc<RwLock<HashMap<Uuid, Subscriber>>>,
}

/// A subscriber for the Astarte handler.
#[derive(Clone)]
struct Subscriber {
    introspection: Vec<astarte_device_sdk::Interface>,
    sender: Sender<Result<AstarteMessage, Status>>,
}

#[async_trait]
impl<T> AstarteSubscriber for AstarteHandler<T>
where
    T: Client + Sync,
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
impl<T> AstartePublisher for AstarteHandler<T>
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

#[async_trait]
impl<T> AstarteRunner for AstarteHandler<T>
where
    T: Client + Send,
{
    /// Runner function for the Astarte handler.
    ///
    /// Polls the Astarte Device SDK for received messages. When the received message interface
    /// matches with one or more of the subscribers interface it forwards the message to each
    /// subscriber queue.
    ///
    /// This function should be run periodically.
    /// N.B. the Astarte SDK `poll()` function is blocking and as a consequence so will be this
    /// function.
    #[allow(dead_code)]
    async fn run(&mut self) {
        if let Err(err) = self.device_sdk.handle_events().await {
            warn!("Received an error on handle_events {err:?}");
        };
    }
}

async fn handle_rx_events(
    mut rx_event: EventReceiver,
    subscribers: Arc<RwLock<HashMap<Uuid, Subscriber>>>,
) {
    while let Some(event) = rx_event.recv().await {
        let astarte_data_event = match event {
            Err(err) => {
                warn!("Unable to receive event from astarte device sdk :{}", err);
                return;
            }
            Ok(event) => event,
        };

        let Ok(astarte_message) = AstarteMessage::try_from(astarte_data_event.clone()) else {
            warn!(
                "Unable to convert astarte_data_event to AstarteMessage: {:?}",
                astarte_data_event
            );
            return;
        };

        let subscribers_guard = subscribers.read().await;
        let subscribers = subscribers_guard
            .iter()
            .filter(|(_, subscriber)| {
                subscriber
                    .introspection
                    .iter()
                    .map(|iface| iface.interface_name())
                    .collect::<String>()
                    .contains(&astarte_data_event.interface)
            })
            .map(|(_, subscriber)| subscriber);

        for subscriber in subscribers {
            if let Err(err) = subscriber.sender.send(Ok(astarte_message.clone())).await {
                warn!("Unable to send astate_message to subscriber :{}", err);
            };
        }
    }
}

impl<T> AstarteHandler<T>
where
    T: Client + Send + Sync,
{
    /// Constructs a new handler from the [AstarteDeviceSdk]
    #[allow(dead_code)]
    pub fn new(device_sdk: T, rx_event: EventReceiver) -> Self {
        let subscribers: Arc<RwLock<HashMap<Uuid, Subscriber>>> = Arc::new(Default::default());

        // can't add the handle to the AstarteHandler fields because JoinHandle is not Clone
        let _handle_rx_events = tokio::spawn(handle_rx_events(rx_event, Arc::clone(&subscribers)));

        AstarteHandler {
            device_sdk,
            subscribers,
        }
    }

    /// Publish an AstarteDataTypeIndividual on specific interface and path.
    ///
    /// The AstarteDataTypeIndividual are defined in the `astarte_type.proto` file.
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

    /// Publish an AstarteDataTypeObject on specific interface and path.
    ///
    /// The AstarteDataTypeObject is defined in the `astarte_type.proto` file.
    async fn publish_astarte_object(
        &self,
        object_data: astarte_message_hub_proto::AstarteDataTypeObject,
        interface_name: &str,
        path: &str,
        timestamp: Option<pbjson_types::Timestamp>,
    ) -> Result<(), AstarteMessageHubError> {
        let astarte_data_individual_map: HashMap<String, AstarteDataTypeIndividual> =
            object_data.object_data;

        // let aggr = crate::astarte_device_sdk::types::map_values_to_astarte_type(
        //     astarte_data_individual_map,
        // )?;
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

#[cfg(test)]
mod test {
    use astarte_device_sdk::{Aggregation, AstarteDeviceDataEvent};
    use std::collections::HashMap;
    use std::str::FromStr;

    use astarte_device_sdk::error::Error as AstarteSdkError;
    use astarte_device_sdk::store::memory::MemoryStore;
    use astarte_device_sdk::transport::mqtt::Mqtt;
    use astarte_device_sdk::types::AstarteType;
    use astarte_message_hub_proto::astarte_data_type_individual::IndividualData;
    use astarte_message_hub_proto::astarte_message::Payload;
    use astarte_message_hub_proto::AstarteMessage;
    use astarte_message_hub_proto::{AstarteDataTypeIndividual, AstarteUnset};
    use chrono::Utc;
    use pbjson_types::Timestamp;
    use tokio::sync::mpsc::Receiver;
    use tonic::Status;

    use crate::astarte_message_hub::AstarteNode;
    use crate::data::astarte::{AstartePublisher, AstarteRunner, AstarteSubscriber};
    use crate::data::mock_astarte_sdk::MockAstarteDeviceSdk;
    use crate::error::AstarteMessageHubError;

    use super::AstarteHandler;

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

        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        device_sdk.expect_add_interface().returning(|_| Ok(()));

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.subscribe(&astarte_node).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn subscribe_failed_invalid_interface() {
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);
        let interfaces = vec!["".to_string().into_bytes()];

        device_sdk.expect_add_interface().returning(|_| Ok(()));

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

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
        device_sdk.expect_add_interface().returning(|_| Ok(()));

        tx_event
            .send(Ok(AstarteDeviceDataEvent {
                interface: interface_string.clone(),
                path: path.to_string(),
                data: Aggregation::Individual(value.into()),
            }))
            .await
            .unwrap();

        let mut astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let subscribe_result = astarte_handler.subscribe(&astarte_node).await;
        assert!(subscribe_result.is_ok());

        let mut rx: Receiver<Result<AstarteMessage, Status>> = subscribe_result.unwrap();
        astarte_handler.run().await;

        let astarte_message_result = rx.recv().await.unwrap();
        assert!(astarte_message_result.is_ok());

        let astarte_message = astarte_message_result.unwrap();

        assert_eq!(expected_interface_name, astarte_message.interface_name);
        assert_eq!(path, astarte_message.path);

        let individual_data = astarte_message
            .take_data()
            .and_then(|data| data.take_individual())
            .and_then(|data| data.individual_data)
            .and_then(|data| data.into())
            .unwrap();

        assert_eq!(IndividualData::AstarteInteger(value), individual_data);
    }

    #[tokio::test]
    async fn poll_failed_with_astarte_error() {
        let mut device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        device_sdk.expect_handle_events().returning(|| {
            // this error is irrelevant for the test purpose
            Err(AstarteSdkError::InterfaceNotFound {
                name: String::new(),
            })
        });

        device_sdk.expect_add_interface().returning(|_| Ok(()));

        let mut astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let subscribe_result = astarte_handler.subscribe(&astarte_node).await;
        assert!(subscribe_result.is_ok());

        let mut rx: Receiver<Result<AstarteMessage, Status>> = subscribe_result.unwrap();
        astarte_handler.run().await;

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn publish_failed_with_invalid_payload() {
        let device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        let expected_interface_name = "io.demo.Properties";

        let astarte_message = create_astarte_message(expected_interface_name, "/test", None, None);

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_err());

        let _result_err = result.err().unwrap();
        assert!(matches!(
            AstarteMessageHubError::AstarteInvalidData("Invalid payload".to_string()),
            _result_err
        ));
    }

    #[tokio::test]
    async fn publish_failed_with_invalid_astarte_data() {
        let device_sdk = MockAstarteDeviceSdk::<MemoryStore, Mqtt>::new();
        let (_, rx_event) = tokio::sync::mpsc::channel(2);

        let astarte_message = create_astarte_message("io.demo.Properties", "/test", None, None);

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_err());

        let _result_err = result.err().unwrap();
        assert!(matches!(
            AstarteMessageHubError::AstarteInvalidData("Invalid Astarte data type".to_string()),
            _result_err
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

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.publish(&astarte_message).await;
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

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.publish(&astarte_message).await;
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

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_err());

        let _result_err = result.err().unwrap();
        assert!(matches!(
            AstarteSdkError::MappingNotFound {
                interface: expected_interface_name.to_string(),
                mapping: String::new(),
            },
            _result_err
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

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_err());

        let _result_err = result.err().unwrap();
        assert!(matches!(
            AstarteSdkError::MappingNotFound {
                interface: expected_interface_name.to_string(),
                mapping: String::new(),
            },
            _result_err
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

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.publish(&astarte_message).await;
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

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.publish(&astarte_message).await;
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

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_err());

        let _result_err = result.err().unwrap();
        assert!(matches!(
            AstarteSdkError::MappingNotFound {
                interface: expected_interface_name.to_string(),
                mapping: String::new(),
            },
            _result_err
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

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_err());

        let _result_err = result.err().unwrap();
        assert!(matches!(
            AstarteSdkError::MappingNotFound {
                interface: expected_interface_name.to_string(),
                mapping: String::new(),
            },
            _result_err
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

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);
        let result = astarte_handler.publish(&astarte_message).await;
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

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_err());

        let _result_err = result.err().unwrap();
        assert!(matches!(
            AstarteSdkError::MappingNotFound {
                interface: expected_interface_name.to_string(),
                mapping: String::new(),
            },
            _result_err
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

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let result = astarte_handler.subscribe(&astarte_node).await;
        assert!(result.is_ok());

        let detach_result = astarte_handler.unsubscribe(&astarte_node).await;
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

        let astarte_handler = AstarteHandler::new(device_sdk, rx_event);

        let detach_result = astarte_handler.unsubscribe(&astarte_node).await;

        assert!(detach_result.is_err());
        assert!(matches!(
            detach_result.err().unwrap(),
            AstarteMessageHubError::AstarteInvalidData(_)
        ))
    }
}
