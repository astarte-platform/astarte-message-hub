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
use std::io::Error;
use std::sync::Arc;

#[cfg(not(test))]
use astarte_device_sdk::AstarteDeviceSdk;
use astarte_device_sdk::Interface;
use async_trait::async_trait;
use log::warn;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tonic::Status;
use uuid::Uuid;

use crate::astarte_message_hub::AstarteNode;
use crate::data::astarte::AstarteSubscriber;
#[cfg(test)]
use crate::data::mock_astarte::MockAstarteDeviceSdk as AstarteDeviceSdk;
use crate::error::AstarteMessageHubError;
use crate::proto_message_hub::AstarteMessage;

pub struct Astarte {
    pub device_sdk: AstarteDeviceSdk,
    subscribers: Arc<RwLock<HashMap<Uuid, Subscriber>>>,
}

struct Subscriber {
    introspection: Vec<Interface>,
    sender: Sender<Result<AstarteMessage, Status>>,
}

#[async_trait]
impl AstarteSubscriber for Astarte {
    async fn subscribe(
        &self,
        astarte_node: &AstarteNode,
    ) -> Result<Receiver<Result<AstarteMessage, Status>>, AstarteMessageHubError> {
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

    async fn unsubscribe(&self, _astarte_node: &AstarteNode) -> Result<(), Error> {
        Ok(())
    }
}

impl Astarte {
    #[allow(dead_code)]
    pub async fn run(&mut self) {
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

#[cfg(test)]
mod test {
    use std::str::FromStr;
    use std::sync::Arc;

    use astarte_device_sdk::{Aggregation, AstarteDeviceDataEvent, AstarteError};
    use tokio::sync::mpsc::Receiver;
    use tonic::Status;

    use crate::astarte_message_hub::AstarteNode;
    use crate::data::astarte::AstarteSubscriber;
    use crate::data::astarte_provider::Astarte;
    use crate::data::mock_astarte::MockAstarteDeviceSdk;
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

    #[tokio::test]
    async fn subscribe_success() {
        use crate::data::astarte_provider::Astarte;

        let mut device_sdk = MockAstarteDeviceSdk::new();

        device_sdk.expect_add_interface().returning(|_| Ok(()));

        let interfaces = vec![SERV_PROPS_IFACE.to_string().into_bytes()];

        let astarte_node = AstarteNode::new(
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            interfaces,
        );

        let astarte = Astarte {
            device_sdk,
            subscribers: Arc::new(Default::default()),
        };

        let result = astarte.subscribe(&astarte_node).await;
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

        let astarte = Astarte {
            device_sdk,
            subscribers: Arc::new(Default::default()),
        };

        let result = astarte.subscribe(&astarte_node).await;
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
        use crate::data::astarte_provider::Astarte;
        use crate::proto_message_hub::astarte_data_type::Data;
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteDataTypeIndividual;
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

        let mut astarte = Astarte {
            device_sdk,
            subscribers: Arc::new(Default::default()),
        };

        let subscribe_result = astarte.subscribe(&astarte_node).await;
        assert!(subscribe_result.is_ok());

        let mut rx: Receiver<Result<AstarteMessage, Status>> = subscribe_result.unwrap();
        astarte.run().await;

        let astarte_message_result = rx.recv().await.unwrap();
        assert!(astarte_message_result.is_ok());

        let astarte_message = astarte_message_result.unwrap();

        assert_eq!(expected_interface_name, astarte_message.interface_name);
        assert_eq!(astarte_message.path, path);

        if let Payload::AstarteData(astarte_data) = astarte_message.payload.unwrap() {
            if let Data::AstarteIndividual(individual_data) = astarte_data.data.unwrap() {
                let result_value: AstarteDataTypeIndividual = value.into();
                assert_eq!(result_value, individual_data);
            }
        } else {
            panic!()
        }
    }

    #[tokio::test]
    async fn poll_failed_with_astarte_error() {
        use crate::data::astarte_provider::Astarte;
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

        let mut astarte = Astarte {
            device_sdk,
            subscribers: Arc::new(Default::default()),
        };

        let subscribe_result = astarte.subscribe(&astarte_node).await;
        assert!(subscribe_result.is_ok());

        let mut rx: Receiver<Result<AstarteMessage, Status>> = subscribe_result.unwrap();
        astarte.run().await;

        assert!(rx.try_recv().is_err());
    }
}
