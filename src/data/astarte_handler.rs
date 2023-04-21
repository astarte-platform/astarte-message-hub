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

use async_trait::async_trait;
use log::warn;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tonic::Status;
use uuid::Uuid;

use crate::astarte_message_hub::AstarteNode;
use crate::data::astarte::{AstartePublisher, AstarteRunner, AstarteSubscriber};
use crate::error::AstarteMessageHubError;
use crate::proto_message_hub;

#[cfg(test)]
use crate::data::mock_astarte_sdk::MockAstarteDeviceSdk as AstarteDeviceSdk;
#[cfg(not(test))]
use astarte_device_sdk::AstarteDeviceSdk;

/// An Astarte Device SDK based implementation of an Astarte handler.
/// Uses the Astarte Device SDK to provide subscribe and publish functionality.
#[derive(Clone)]
pub struct AstarteHandler {
    device_sdk: AstarteDeviceSdk,
    subscribers: Arc<RwLock<HashMap<Uuid, Subscriber>>>,
}

/// A subscriber for the Astarte handler.
struct Subscriber {
    introspection: Vec<astarte_device_sdk::Interface>,
    sender: Sender<Result<proto_message_hub::AstarteMessage, Status>>,
}

#[async_trait]
impl AstarteSubscriber for AstarteHandler {
    async fn subscribe(
        &self,
        astarte_node: &AstarteNode,
    ) -> Result<Receiver<Result<proto_message_hub::AstarteMessage, Status>>, AstarteMessageHubError>
    {
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
                            subscriber.introspection.contains(interface).then(|| ())
                        })
                        .is_none()
                })
                .collect::<Vec<astarte_device_sdk::Interface>>()
        };

        for interface in interfaces_to_remove.iter() {
            self.device_sdk
                .remove_interface(&interface.get_name())
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
impl AstartePublisher for AstarteHandler {
    async fn publish(
        &self,
        astarte_message: &proto_message_hub::AstarteMessage,
    ) -> Result<(), AstarteMessageHubError> {
        use crate::proto_message_hub::astarte_data_type::Data;
        use crate::proto_message_hub::astarte_message::Payload;

        match astarte_message.payload.clone().ok_or_else(|| {
            AstarteMessageHubError::AstarteInvalidData("Invalid payload".to_string())
        })? {
            Payload::AstarteData(astarte_data) => {
                match astarte_data.data.ok_or_else(|| {
                    AstarteMessageHubError::AstarteInvalidData(
                        "Invalid Astarte data type".to_string(),
                    )
                })? {
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
            Payload::AstarteUnset(_) => {
                self.device_sdk
                    .unset(&astarte_message.interface_name, &astarte_message.path)
                    .await
            }
            .map_err(AstarteMessageHubError::AstarteError),
        }
    }
}

#[async_trait]
impl AstarteRunner for AstarteHandler {
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
        use crate::proto_message_hub::AstarteMessage;

        if let Ok(astarte_data_event) = self.device_sdk.handle_events().await {
            println!("incoming: {:?}", astarte_data_event);

            if let Ok(astarte_message) = AstarteMessage::try_from(astarte_data_event.clone()) {
                let subscribers_guard = self.subscribers.read().await;
                let subscribers = subscribers_guard
                    .iter()
                    .filter(|(_, subscriber)| {
                        subscriber
                            .introspection
                            .iter()
                            .map(|iface| iface.get_name())
                            .collect::<String>()
                            .contains(&astarte_data_event.interface)
                    })
                    .map(|(_, subscriber)| subscriber);
                for subscriber in subscribers {
                    let _ = subscriber.sender.send(Ok(astarte_message.clone())).await;
                }
            } else {
                warn!(
                    "Unable to convert astarte_data_event to AstarteMessage: {:?}",
                    astarte_data_event
                );
            }
        }
    }
}

impl AstarteHandler {
    /// Constructs a new handler from the [AstarteDeviceSdk]
    #[allow(dead_code)]
    pub fn new(device_sdk: AstarteDeviceSdk) -> Self {
        AstarteHandler {
            device_sdk,
            subscribers: Arc::new(Default::default()),
        }
    }

    /// Publish an AstarteDataTypeIndividual on specific interface and path.
    ///
    /// The AstarteDataTypeIndividual are defined in the `astarte_type.proto` file.
    async fn publish_astarte_individual(
        &self,
        data: proto_message_hub::AstarteDataTypeIndividual,
        interface_name: &str,
        path: &str,
        timestamp: Option<pbjson_types::Timestamp>,
    ) -> Result<(), AstarteMessageHubError> {
        use astarte_device_sdk::types::AstarteType;

        let astarte_type: AstarteType = data
            .individual_data
            .ok_or_else(|| {
                AstarteMessageHubError::AstarteInvalidData("Invalid individual data".to_string())
            })?
            .try_into()?;

        if let Some(timestamp) = timestamp {
            self.device_sdk
                .send_with_timestamp(interface_name, path, astarte_type, timestamp.try_into()?)
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
        object_data: proto_message_hub::AstarteDataTypeObject,
        interface_name: &str,
        path: &str,
        timestamp: Option<pbjson_types::Timestamp>,
    ) -> Result<(), AstarteMessageHubError> {
        use crate::proto_message_hub::AstarteDataTypeIndividual;

        let astarte_data_individual_map: HashMap<String, AstarteDataTypeIndividual> =
            object_data.object_data;

        let aggr = crate::types::map_values_to_astarte_type(astarte_data_individual_map)?;
        if let Some(timestamp) = timestamp {
            self.device_sdk
                .send_object_with_timestamp(interface_name, path, aggr, timestamp.try_into()?)
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
    use super::AstarteHandler;

    use std::collections::HashMap;
    use std::str::FromStr;

    use astarte_device_sdk::types::AstarteType;
    use astarte_device_sdk::{Aggregation, AstarteDeviceDataEvent, AstarteError};
    use chrono::Utc;
    use tokio::sync::mpsc::Receiver;
    use tonic::Status;

    use crate::astarte_message_hub::AstarteNode;
    use crate::data::astarte::{AstartePublisher, AstarteRunner, AstarteSubscriber};
    use crate::data::mock_astarte_sdk::MockAstarteDeviceSdk;
    use crate::error::AstarteMessageHubError;

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
    async fn subscribe_success() {
        let mut device_sdk = MockAstarteDeviceSdk::new();

        device_sdk.expect_add_interface().returning(|_| Ok(()));

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let astarte_handler = AstarteHandler::new(device_sdk);

        let result = astarte_handler.subscribe(&astarte_node).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn subscribe_failed_invalid_interface() {
        let mut device_sdk = MockAstarteDeviceSdk::new();
        let interfaces = vec!["".to_string().into_bytes()];

        device_sdk
            .expect_add_interface()
            .returning(|_| Err(AstarteError::Unreported));

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let astarte_handler = AstarteHandler::new(device_sdk);

        let result = astarte_handler.subscribe(&astarte_node).await;
        assert!(result.is_err());

        assert!(matches!(
            result.err().unwrap(),
            AstarteMessageHubError::AstarteError(astarte_device_sdk::AstarteError::InterfaceError(
                _
            ))
        ))
    }

    #[tokio::test]
    async fn poll_success() {
        use crate::proto_message_hub::astarte_data_type_individual::IndividualData;
        use crate::proto_message_hub::AstarteMessage;

        let prop_interface = astarte_device_sdk::Interface::from_str(SERV_PROPS_IFACE).unwrap();
        let expected_interface_name = prop_interface.get_name();
        let path = "test";
        let value: i32 = 5;

        let mut device_sdk = MockAstarteDeviceSdk::new();

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let interface_cloned = expected_interface_name.clone();
        device_sdk.expect_handle_events().returning(move || {
            Ok(AstarteDeviceDataEvent {
                interface: interface_cloned.to_string(),
                path: path.to_string(),
                data: Aggregation::Individual(value.into()),
            })
        });

        device_sdk.expect_add_interface().returning(|_| Ok(()));

        let mut astarte_handler = AstarteHandler::new(device_sdk);

        let subscribe_result = astarte_handler.subscribe(&astarte_node).await;
        assert!(subscribe_result.is_ok());

        let mut rx: Receiver<Result<AstarteMessage, Status>> = subscribe_result.unwrap();
        astarte_handler.run().await;

        let astarte_message_result = rx.recv().await.unwrap();
        assert!(astarte_message_result.is_ok());

        let astarte_message = astarte_message_result.unwrap();

        assert_eq!(expected_interface_name, astarte_message.interface_name);
        assert_eq!(astarte_message.path, path);

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
        use crate::proto_message_hub::AstarteMessage;

        let mut device_sdk = MockAstarteDeviceSdk::new();
        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        device_sdk
            .expect_handle_events()
            .returning(move || Err(AstarteError::DeserializationError));

        device_sdk.expect_add_interface().returning(|_| Ok(()));

        let mut astarte_handler = AstarteHandler::new(device_sdk);

        let subscribe_result = astarte_handler.subscribe(&astarte_node).await;
        assert!(subscribe_result.is_ok());

        let mut rx: Receiver<Result<AstarteMessage, Status>> = subscribe_result.unwrap();
        astarte_handler.run().await;

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn publish_failed_with_invalid_payload() {
        use crate::proto_message_hub::AstarteMessage;

        let device_sdk = MockAstarteDeviceSdk::new();

        let expected_interface_name = "io.demo.Properties";

        let astarte_message = AstarteMessage {
            interface_name: expected_interface_name.to_string(),
            path: "/test".to_string(),
            payload: None,
            timestamp: None,
        };

        let astarte_handler = AstarteHandler::new(device_sdk);

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
        use crate::proto_message_hub::AstarteMessage;

        let device_sdk = MockAstarteDeviceSdk::new();

        let astarte_message = AstarteMessage {
            interface_name: "io.demo.Properties".to_string(),
            path: "/test".to_string(),
            payload: None,
            timestamp: None,
        };

        let astarte_handler = AstarteHandler::new(device_sdk);

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
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteMessage;

        let mut device_sdk = MockAstarteDeviceSdk::new();

        let expected_interface_name = "io.demo.Properties".to_string();

        let astarte_message = AstarteMessage {
            interface_name: expected_interface_name.clone(),
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(5.into())),
            timestamp: None,
        };

        device_sdk
            .expect_send()
            .withf(move |interface_name: &str, _: &str, _: &AstarteType| {
                interface_name == expected_interface_name
            })
            .returning(|_: &str, _: &str, _: AstarteType| Ok(()));

        let astarte_handler = AstarteHandler::new(device_sdk);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_individual_with_timestamp_success() {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteMessage;

        let mut device_sdk = MockAstarteDeviceSdk::new();

        let expected_interface_name = "io.demo.Properties";

        let astarte_message = AstarteMessage {
            interface_name: expected_interface_name.to_string(),
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(5.into())),
            timestamp: Some(Utc::now().into()),
        };

        device_sdk
            .expect_send_with_timestamp()
            .withf(
                move |interface_name: &str, _: &str, _: &AstarteType, _: &chrono::DateTime<Utc>| {
                    interface_name == expected_interface_name
                },
            )
            .returning(|_: &str, _: &str, _: AstarteType, _: chrono::DateTime<Utc>| Ok(()));

        let astarte_handler = AstarteHandler::new(device_sdk);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_individual_failed() {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteMessage;

        let mut device_sdk = MockAstarteDeviceSdk::new();

        let expected_interface_name = "io.demo.Properties";

        let astarte_message = AstarteMessage {
            interface_name: expected_interface_name.to_string(),
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(5.into())),
            timestamp: None,
        };

        device_sdk
            .expect_send()
            .withf(move |interface_name: &str, _: &str, _: &AstarteType| {
                interface_name == expected_interface_name
            })
            .returning(|_: &str, _: &str, _: AstarteType| {
                Err(AstarteError::SendError(
                    "Unable to send individual data".to_string(),
                ))
            });

        let astarte_handler = AstarteHandler::new(device_sdk);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_err());

        let _result_err = result.err().unwrap();
        assert!(matches!(
            AstarteError::SendError("Unable to send individual data".to_string()),
            _result_err
        ));
    }

    #[tokio::test]
    async fn publish_individual_with_timestamp_failed() {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteMessage;

        let mut device_sdk = MockAstarteDeviceSdk::new();

        let expected_interface_name = "io.demo.Properties";

        let astarte_message = AstarteMessage {
            interface_name: expected_interface_name.to_string(),
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(5.into())),
            timestamp: Some(Utc::now().into()),
        };

        device_sdk
            .expect_send_with_timestamp()
            .withf(
                move |interface_name: &str, _: &str, _: &AstarteType, _: &chrono::DateTime<Utc>| {
                    interface_name == expected_interface_name
                },
            )
            .returning(
                |_: &str, _: &str, _: AstarteType, _: chrono::DateTime<Utc>| {
                    Err(AstarteError::SendError(
                        "Unable to send individual data".to_string(),
                    ))
                },
            );

        let astarte_handler = AstarteHandler::new(device_sdk);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_err());

        let _result_err = result.err().unwrap();
        assert!(matches!(
            AstarteError::SendError("Unable to send individual data".to_string()),
            _result_err
        ));
    }

    #[tokio::test]
    async fn publish_object_success() {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteDataTypeIndividual;
        use crate::proto_message_hub::AstarteMessage;

        let mut device_sdk = MockAstarteDeviceSdk::new();

        let expected_interface_name = "io.demo.Object";

        let expected_i32 = 5;
        let expected_f64 = 5.12;
        let mut map_val: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        map_val.insert("i32".to_owned(), expected_i32.into());
        map_val.insert("f64".to_owned(), expected_f64.into());

        let astarte_message = AstarteMessage {
            interface_name: expected_interface_name.to_string(),
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(map_val.into())),
            timestamp: None,
        };

        device_sdk
            .expect_send_object()
            .withf(
                move |interface_name: &str, _: &str, _: &HashMap<String, AstarteType>| {
                    interface_name == expected_interface_name
                },
            )
            .returning(|_: &str, _: &str, _: HashMap<String, AstarteType>| Ok(()));

        let astarte_handler = AstarteHandler::new(device_sdk);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_object_with_timestamp_success() {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteDataTypeIndividual;
        use crate::proto_message_hub::AstarteMessage;

        let mut device_sdk = MockAstarteDeviceSdk::new();
        let expected_interface_name = "io.demo.Object";

        let expected_i32 = 5;
        let expected_f64 = 5.12;
        let mut map_val: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        map_val.insert("i32".to_owned(), expected_i32.into());
        map_val.insert("f64".to_owned(), expected_f64.into());

        let astarte_message = AstarteMessage {
            interface_name: expected_interface_name.to_string(),
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(map_val.into())),
            timestamp: Some(Utc::now().into()),
        };

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

        let astarte_handler = AstarteHandler::new(device_sdk);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_object_failed() {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteDataTypeIndividual;
        use crate::proto_message_hub::AstarteMessage;

        let mut device_sdk = MockAstarteDeviceSdk::new();

        let expected_interface_name = "io.demo.Object";

        let expected_i32 = 5;
        let expected_f64 = 5.12;
        let mut map_val: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        map_val.insert("i32".to_owned(), expected_i32.into());
        map_val.insert("f64".to_owned(), expected_f64.into());

        let astarte_message = AstarteMessage {
            interface_name: expected_interface_name.to_string(),
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(map_val.into())),
            timestamp: None,
        };

        device_sdk
            .expect_send_object()
            .withf(
                move |interface_name: &str, _: &str, _: &HashMap<String, AstarteType>| {
                    interface_name == expected_interface_name
                },
            )
            .returning(|_: &str, _: &str, _: HashMap<String, AstarteType>| {
                Err(AstarteError::SendError("Unable to send object".to_string()))
            });

        let astarte_handler = AstarteHandler::new(device_sdk);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_err());

        let _result_err = result.err().unwrap();
        assert!(matches!(
            AstarteError::SendError("Unable to send object".to_string()),
            _result_err
        ));
    }

    #[tokio::test]
    async fn publish_object_with_timestamp_failed() {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteDataTypeIndividual;
        use crate::proto_message_hub::AstarteMessage;

        let mut device_sdk = MockAstarteDeviceSdk::new();

        let expected_interface_name = "io.demo.Object";

        let expected_i32 = 5;
        let expected_f64 = 5.12;
        let mut map_val: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        map_val.insert("i32".to_owned(), expected_i32.into());
        map_val.insert("f64".to_owned(), expected_f64.into());

        let astarte_message = AstarteMessage {
            interface_name: expected_interface_name.to_string(),
            path: "/test".to_string(),
            payload: Some(Payload::AstarteData(map_val.into())),
            timestamp: Some(Utc::now().into()),
        };

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
                    Err(AstarteError::SendError("Unable to send object".to_string()))
                },
            );

        let astarte_handler = AstarteHandler::new(device_sdk);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_err());

        let _result_err = result.err().unwrap();
        assert!(matches!(
            AstarteError::SendError("Unable to send object".to_string()),
            _result_err
        ));
    }

    #[tokio::test]
    async fn publish_unset_success() {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteMessage;
        use crate::proto_message_hub::AstarteUnset;

        let mut device_sdk = MockAstarteDeviceSdk::new();
        let expected_interface_name = "io.demo.Object";

        let astarte_message = AstarteMessage {
            interface_name: expected_interface_name.to_string(),
            path: "/test".to_string(),
            payload: Some(Payload::AstarteUnset(AstarteUnset {})),
            timestamp: None,
        };

        device_sdk
            .expect_unset()
            .withf(move |interface_name: &str, _: &str| interface_name == expected_interface_name)
            .returning(|_: &str, _: &str| Ok(()));

        let astarte_handler = AstarteHandler::new(device_sdk);
        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn publish_unset_failed() {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteMessage;
        use crate::proto_message_hub::AstarteUnset;

        let mut device_sdk = MockAstarteDeviceSdk::new();
        let expected_interface_name = "io.demo.Object";

        let astarte_message = AstarteMessage {
            interface_name: expected_interface_name.to_string(),
            path: "/test".to_string(),
            payload: Some(Payload::AstarteUnset(AstarteUnset {})),
            timestamp: None,
        };

        device_sdk
            .expect_unset()
            .withf(move |interface_name: &str, _: &str| interface_name == expected_interface_name)
            .returning(|_: &str, _: &str| {
                Err(AstarteError::SendError("Unable to unset path".to_string()))
            });

        let astarte_handler = AstarteHandler::new(device_sdk);

        let result = astarte_handler.publish(&astarte_message).await;
        assert!(result.is_err());

        let _result_err = result.err().unwrap();
        assert!(matches!(
            AstarteError::SendError("Unable to unset path".to_string()),
            _result_err
        ));
    }

    #[tokio::test]
    async fn detach_node_success() {
        let interfaces = vec![
            SERV_PROPS_IFACE.to_string().into_bytes(),
            SERV_OBJ_IFACE.to_string().into_bytes(),
        ];

        let mut device_sdk = MockAstarteDeviceSdk::new();
        device_sdk.expect_add_interface().returning(|_| Ok(()));
        device_sdk.expect_remove_interface().returning(|_| Ok(()));

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let astarte_handler = AstarteHandler::new(device_sdk);

        let result = astarte_handler.subscribe(&astarte_node).await;
        assert!(result.is_ok());

        let detach_result = astarte_handler.unsubscribe(&astarte_node).await;
        assert!(detach_result.is_ok())
    }

    #[tokio::test]
    async fn detach_node_unsubscribe_failed() {
        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let mut device_sdk = MockAstarteDeviceSdk::new();
        device_sdk.expect_add_interface().returning(|_| Ok(()));
        device_sdk.expect_remove_interface().returning(|_| Ok(()));

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let astarte_handler = AstarteHandler::new(device_sdk);

        let detach_result = astarte_handler.unsubscribe(&astarte_node).await;

        assert!(detach_result.is_err());
        assert!(matches!(
            detach_result.err().unwrap(),
            AstarteMessageHubError::AstarteInvalidData(_)
        ))
    }
}
