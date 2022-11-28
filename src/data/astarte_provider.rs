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
use astarte_sdk::AstarteSdk;
use async_trait::async_trait;
use log::warn;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tonic::Status;
use uuid::Uuid;

use crate::astarte_message_hub::AstarteNode;
use crate::data::astarte::AstarteSubscriber;
#[cfg(test)]
use crate::data::mock_astarte::MockAstarteSdk as AstarteSdk;
use crate::proto_message_hub;
use crate::proto_message_hub::AstarteMessage;

pub struct Astarte {
    pub device_sdk: AstarteSdk,
    subscribers: Arc<RwLock<HashMap<Uuid, Subscriber>>>,
}

struct Subscriber {
    introspection: Vec<proto_message_hub::Interface>,
    sender: Sender<Result<AstarteMessage, Status>>,
}

#[async_trait]
impl AstarteSubscriber for Astarte {
    async fn subscribe(
        &self,
        astarte_node: &AstarteNode,
    ) -> Result<Receiver<Result<AstarteMessage, Status>>, Error> {
        let (tx, rx) = channel(32);
        self.subscribers.write().await.insert(
            astarte_node.id.clone(),
            Subscriber {
                introspection: astarte_node.introspection.clone(),
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
    pub async fn run(&mut self) {
        if let Ok(clientbound) = self.device_sdk.poll().await {
            println!("incoming: {:?}", clientbound);

            if let Ok(astarte_message) = AstarteMessage::try_from(clientbound.clone()) {
                let subscribers_guard = self.subscribers.read().await;
                let subscribers = subscribers_guard
                    .iter()
                    .filter(|(_, subscriber)| {
                        subscriber
                            .introspection
                            .iter()
                            .map(|iface| iface.name.clone())
                            .collect::<String>()
                            .contains(&clientbound.interface)
                    })
                    .map(|(_, subscriber)| subscriber);
                for subscriber in subscribers {
                    let _ = subscriber.sender.send(Ok(astarte_message.clone())).await;
                }
            } else {
                warn!(
                    "Unable to convert clientbound to AstarteMessage: {:?}",
                    clientbound
                );
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use astarte_sdk::{Aggregation, AstarteError, Clientbound};
    use tokio::sync::mpsc::Receiver;
    use tonic::Status;

    use crate::astarte_message_hub::AstarteNode;
    use crate::data::astarte::AstarteSubscriber;
    use crate::data::mock_astarte::MockAstarteSdk;
    use crate::proto_message_hub::Interface;

    #[tokio::test]
    async fn subscribe_success() {
        use crate::data::astarte_provider::Astarte;

        let device_sdk = MockAstarteSdk::new();

        let interfaces = vec![Interface {
            name: "io.demo.ServerProperties".to_owned(),
            minor: 0,
            major: 2,
        }];

        let astarte_node = AstarteNode {
            id: "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            introspection: interfaces,
        };

        let astarte = Astarte {
            device_sdk,
            subscribers: Arc::new(Default::default()),
        };

        let result = astarte.subscribe(&astarte_node).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn poll_success() {
        use crate::data::astarte_provider::Astarte;
        use crate::proto_message_hub::astarte_data_type::Data;
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteDataTypeIndividual;
        use crate::proto_message_hub::AstarteMessage;

        let interface_name = "io.demo.ServerProperties";
        let path = "test";
        let value: i32 = 5;

        let mut device_sdk = MockAstarteSdk::new();

        let interfaces = vec![Interface {
            name: interface_name.to_owned(),
            minor: 0,
            major: 2,
        }];

        let astarte_node = AstarteNode {
            id: "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            introspection: interfaces,
        };

        device_sdk.expect_poll().returning(move || {
            Ok(Clientbound {
                interface: interface_name.to_string(),
                path: path.to_string(),
                data: Aggregation::Individual(value.into()),
            })
        });

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

        assert_eq!(astarte_message.interface.unwrap().name, interface_name);
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

        let interface_name = "io.demo.ServerProperties";

        let mut device_sdk = MockAstarteSdk::new();

        let interfaces = vec![Interface {
            name: interface_name.to_owned(),
            minor: 0,
            major: 2,
        }];

        let astarte_node = AstarteNode {
            id: "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            introspection: interfaces,
        };

        device_sdk
            .expect_poll()
            .returning(move || Err(AstarteError::DeserializationError));

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
