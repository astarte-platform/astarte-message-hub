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
//! An implementation of an handler is included in this crate
//! [AstarteHandler][crate::data::astarte_handler::AstarteHandler].
//! However, nothing stops third parties from developing their own handler by implementing
//! the traits in this file.

use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;
use tonic::Status;

use crate::astarte_message_hub::AstarteNode;
use crate::error::AstarteMessageHubError;
use crate::proto_message_hub;

#[async_trait]
pub trait AstarteRunner {
    async fn run(&mut self);
}

/// A **trait** required for all Astarte handlers that want to publish data on Astarte.
#[async_trait]
pub trait AstartePublisher: Send + Sync {
    /// Publish new data on Astarte.
    ///
    /// The `astarte_message` argument format is
    /// defined in `./proto/astarteplatform/msghub/astarte_message.proto`
    async fn publish(
        &self,
        astarte_message: &proto_message_hub::AstarteMessage,
    ) -> Result<(), AstarteMessageHubError>;
}

/// A **trait** required for all Astarte handlers that want to subscribe and unsubscribe a
/// node to Astarte.
#[async_trait]
pub trait AstarteSubscriber {
    /// Subscribe a new node to Astarte.
    async fn subscribe(
        &self,
        astarte_node: &AstarteNode,
    ) -> Result<Receiver<Result<proto_message_hub::AstarteMessage, Status>>, AstarteMessageHubError>;

    /// Unsubscribe a previously subscribed node to Astarte.
    async fn unsubscribe(&self, astarte_node: &AstarteNode) -> Result<(), AstarteMessageHubError>;
}
