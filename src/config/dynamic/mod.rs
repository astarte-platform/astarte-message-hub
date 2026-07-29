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

use astarte_device_sdk::client::ClientConnection;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::store::StoreDir;

use super::file::Config;
use super::file::dynamic::DynamicConfig;
use super::loader::ConfigEntry;
use super::{Legacy, MessageHubOptions, Pairing};

pub mod grpc;
pub mod http;

/// Shared [`Validate`]
pub type SharedValidate = Arc<RwLock<Option<Validate>>>;

/// Create a new dynamic config validator.
pub struct Validate {
    // NOTE: Required since we have an error in the generic for FDO and Legacy pairing. This is due to
    //       the fact that the SharedValidate is constructed before the connection or the Paring enum. So
    //       at creation we still don't know which connection type we will use.
    // FIXME: this can be a generic with a little cleanup code.
    /// Astarte client
    pub(crate) client: Box<dyn IsPaired + Send + Sync + 'static>,
    /// Pairing configuration
    pub(crate) pairing: Pairing,
}

impl Validate {
    /// Create a new dynamic config validator.
    pub fn new<D>(client: D, options: &MessageHubOptions) -> Self
    where
        D: IsPaired + Send + Sync + 'static,
    {
        Self {
            client: Box::new(client),
            pairing: options.pairing.clone(),
        }
    }

    pub(crate) fn can_change(&self, config: &Config) -> bool {
        match &self.pairing {
            #[cfg(feature = "fdo")]
            Pairing::Fdo(..) => {
                config.realm.is_none() && config.device_id.is_none() && config.pairing_url.is_none()
            }
            Pairing::Legacy(legacy) => {
                let Legacy {
                    realm,
                    device_id,
                    pairing_url,
                    credential: _,
                } = legacy;

                if !self.client.is_paired() {
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
    }
}

/// Trait to check if the device is paired
pub trait IsPaired {
    /// Che if the device is paired
    fn is_paired(&self) -> bool;
}

impl<T> IsPaired for T
where
    T: ClientConnection,
{
    fn is_paired(&self) -> bool {
        ClientConnection::is_paired(self)
    }
}

/// Function that get the configurations needed by the Message Hub.
/// The configuration file is first retrieved from one of two default base locations.
/// If no valid configuration file is found in either of these locations, or if the content
/// of the first found file is not valid HTTP and Protobuf APIs are exposed to provide a valid
/// configuration.
pub async fn listen_dynamic_config(
    tasks: &mut JoinSet<eyre::Result<()>>,
    cancel_token: CancellationToken,
    dynamic: &DynamicConfig,
    store_dir: StoreDir,
    validate: &SharedValidate,
) -> eyre::Result<mpsc::Receiver<ConfigEntry>> {
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
