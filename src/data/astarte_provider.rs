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

use astarte_sdk::AstarteSdk;
use async_trait::async_trait;
use log::warn;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tonic::Status;
use uuid::Uuid;

use crate::astarte_message_hub::AstarteNode;
use crate::data::astarte::AstarteSubscriber;
use crate::proto_message_hub;
use crate::proto_message_hub::AstarteMessage;

#[derive(Clone)]
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
