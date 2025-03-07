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
//! The Astarte message hub uses an independent handler to communicate with Astarte.
//!
//! This module contains all the required traits for such an handler.

use astarte_device_sdk::Interface;
use astarte_message_hub_proto::{AstarteMessage, MessageHubEvent};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Status;
use uuid::Uuid;

use crate::error::AstarteMessageHubError;
use crate::server::AstarteNode;

pub mod handler;
pub(crate) mod sdk;

/// A **trait** required for all Astarte handlers that want to publish data on Astarte.
#[async_trait]
pub trait AstartePublisher: Send + Sync {
    /// Publish new data on Astarte.
    ///
    /// The `astarte_message` argument format is
    /// defined in `./proto/astarteplatform/msghub/astarte_message.proto`
    async fn publish(&self, astarte_message: &AstarteMessage)
        -> Result<(), AstarteMessageHubError>;
}

/// Sender end of Astarte [MessageHubEvent]s.
pub(crate) type EventSender = Sender<Result<MessageHubEvent, Status>>;

/// A **trait** required for all Astarte handlers that want to subscribe and unsubscribe a
/// node to Astarte.
#[async_trait]
pub trait AstarteSubscriber {
    /// Subscribe a new node to Astarte.
    async fn subscribe(
        &self,
        astarte_node: &AstarteNode,
    ) -> Result<(Subscription, EventSender), AstarteMessageHubError>;

    /// Unsubscribe a previously subscribed node to Astarte.
    ///
    /// Returns the names of the interfaces that have been removed.
    async fn unsubscribe(&self, id: &Uuid) -> Result<Vec<String>, AstarteMessageHubError>;

    /// Extend the device interfaces
    async fn extend_interfaces(
        &self,
        node_id: &Uuid,
        to_add: HashMap<String, Interface>,
    ) -> Result<Vec<Interface>, AstarteMessageHubError>;

    /// Remove the device interfaces
    async fn remove_interfaces(
        &self,
        node_id: &Uuid,
        interfaces: HashSet<String>,
    ) -> Result<Vec<String>, AstarteMessageHubError>;
}

/// Values returned after a node has subscribed.
#[derive(Debug)]
pub struct Subscription {
    /// Interface that where added from the Astarte node introspection.
    pub added_interfaces: Vec<Interface>,
    /// The node receiver end.
    pub receiver: Receiver<Result<MessageHubEvent, Status>>,
}
