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

use astarte_device_sdk::store::StoredProp;
use astarte_device_sdk::{AstarteType, Interface};
use astarte_message_hub_proto::astarte_message::Payload;
use astarte_message_hub_proto::{
    AstarteDatastreamIndividual, AstarteDatastreamObject, AstarteMessage,
    AstartePropertyIndividual, MessageHubEvent,
};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::Receiver;
use tonic::Status;
use uuid::Uuid;

use crate::error::AstarteMessageHubError;
use crate::server::{AstarteNode, NodeId};

pub mod handler;
pub(crate) mod sdk;

/// A **trait** required for all Astarte handlers that want to publish data on Astarte.
#[async_trait]
pub trait AstartePublisher: Send + Sync {
    /// Publish new data on Astarte.
    ///
    /// The `astarte_message` argument format is
    /// defined in `./proto/astarteplatform/msghub/astarte_message.proto`
    async fn publish(
        &self,
        astarte_message: &AstarteMessage,
    ) -> Result<(), AstarteMessageHubError> {
        let astarte_message_payload = astarte_message.clone().payload.ok_or_else(|| {
            AstarteMessageHubError::AstarteInvalidData("Invalid payload".to_string())
        })?;

        match astarte_message_payload {
            Payload::DatastreamIndividual(data) => {
                self.publish_individual(
                    data,
                    &astarte_message.interface_name,
                    &astarte_message.path,
                )
                .await
            }
            Payload::DatastreamObject(data) => {
                self.publish_object(data, &astarte_message.interface_name, &astarte_message.path)
                    .await
            }
            Payload::PropertyIndividual(data) => {
                self.publish_property(data, &astarte_message.interface_name, &astarte_message.path)
                    .await
            }
        }
    }

    /// Publish an [`AstarteDatastreamIndividual`] on specific interface and path.
    async fn publish_individual(
        &self,
        data: AstarteDatastreamIndividual,
        interface_name: &str,
        path: &str,
    ) -> Result<(), AstarteMessageHubError>;

    /// Publish an [`AstarteDatastreamObject`] on specific interface and path.
    async fn publish_object(
        &self,
        object_data: AstarteDatastreamObject,
        interface_name: &str,
        path: &str,
    ) -> Result<(), AstarteMessageHubError>;

    /// Publish an [`AstartePropertyIndividual`] on specific interface and path.
    async fn publish_property(
        &self,
        data: AstartePropertyIndividual,
        interface_name: &str,
        path: &str,
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
    ) -> Result<Subscription, AstarteMessageHubError>;

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

/// Trait to access the stored properties of a node.
///
/// Analogous to the `astarte_device_sdk::store::PropAccess` trait, but
/// with the addition of the checks for the interfaces in a certain node introspection
pub trait PropAccessExt {
    /// Get the value of a node property given the interface and path.
    fn property(
        &self,
        node_id: NodeId,
        interface: &str,
        path: &str,
    ) -> impl std::future::Future<Output = Result<Option<AstarteType>, AstarteMessageHubError>>
           + std::marker::Send;

    /// Get all the node properties of the given interface.
    fn interface_props(
        &self,
        node_id: NodeId,
        interface: &str,
    ) -> impl std::future::Future<Output = Result<Vec<StoredProp>, AstarteMessageHubError>>
           + std::marker::Send;

    /// Get all the stored node properties, device or server owners.
    fn all_props(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<Vec<StoredProp>, AstarteMessageHubError>>
           + std::marker::Send;

    /// Get all the stored node device properties.
    fn device_props(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<Vec<StoredProp>, AstarteMessageHubError>>
           + std::marker::Send;

    /// Get all the stored node server properties.
    fn server_props(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<Vec<StoredProp>, AstarteMessageHubError>>
           + std::marker::Send;
}
