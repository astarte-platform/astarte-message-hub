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

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::store::StoreDir;

use super::file::Config;
use super::file::dynamic::DynamicConfig;
use super::loader::ConfigEntry;

pub mod grpc;
pub mod http;

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
        )
        .await?;
    }

    Ok(rx)
}
