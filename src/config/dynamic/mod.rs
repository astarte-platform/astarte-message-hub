// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Dynamic configuration for the Message Hub

use std::sync::Arc;

use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::store::StoreDir;

use super::MessageHubOptions;
use super::file::Config;
use super::file::dynamic::DynamicConfig;
use super::loader::ConfigEntry;

pub mod grpc;
pub mod http;

/// Shared [`Validate`]
pub type SharedValidate<D> = Arc<RwLock<Option<Validate<D>>>>;

/// Create a new dynamic config validator.
#[derive(Debug)]
pub struct Validate<D> {
    /// Astarte client
    pub(crate) client: D,
    /// The Astarte realm the device belongs to.
    pub(crate) realm: String,
    /// A unique ID for the device.
    pub(crate) device_id: String,
    /// The URL of the Astarte pairing API.
    pub(crate) pairing_url: Url,
}

impl<D> Validate<D> {
    /// Create a new dynamic config validator.
    pub fn new(client: D, options: &MessageHubOptions) -> Self {
        Self {
            client,
            realm: options.realm.clone(),
            device_id: options.device_id.clone(),
            pairing_url: options.pairing_url.clone(),
        }
    }

    pub(crate) fn can_change(&self, config: &Config) -> bool
    where
        D: astarte_device_sdk::client::ClientConnection,
    {
        let Self {
            client,
            realm,
            device_id,
            pairing_url,
        } = self;

        if !client.is_paired() {
            return true;
        }

        config
            .realm
            .as_ref()
            .is_none_or(|cfg_realm| cfg_realm == realm)
            && config
                .device_id
                .as_ref()
                .is_none_or(|cfg_device_id| cfg_device_id == device_id)
            && config
                .pairing_url
                .as_ref()
                .is_none_or(|cfg_pairing_url| cfg_pairing_url == pairing_url.as_str())
    }
}

/// Function that get the configurations needed by the Message Hub.
/// The configuration file is first retrieved from one of two default base locations.
/// If no valid configuration file is found in either of these locations, or if the content
/// of the first found file is not valid HTTP and Protobuf APIs are exposed to provide a valid
/// configuration.
pub async fn listen_dynamic_config<D>(
    tasks: &mut JoinSet<eyre::Result<()>>,
    cancel_token: CancellationToken,
    dynamic: &DynamicConfig,
    store_dir: StoreDir,
    validate: &SharedValidate<D>,
) -> eyre::Result<mpsc::Receiver<ConfigEntry>>
where
    D: astarte_device_sdk::client::ClientConnection + Send + Sync + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(2);

    if dynamic.is_http_enabled() {
        let http_address = dynamic.http_address();

        self::http::serve(
            tasks,
            cancel_token.child_token(),
            &http_address,
            tx.clone(),
            store_dir.clone(),
            Arc::clone(validate),
        )
        .await?;
    }

    if dynamic.is_grpc_enabled() {
        let grpc_address = dynamic.grpc_address();

        grpc::serve(
            tasks,
            cancel_token.child_token(),
            grpc_address,
            tx,
            store_dir,
            Arc::clone(validate),
        )
        .await?;
    }

    Ok(rx)
}
