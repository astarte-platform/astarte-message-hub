// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use astarte_device_sdk::{
    DeviceClient, DeviceEvent,
    builder::DeviceBuilder,
    prelude::*,
    store::memory::MemoryStore,
    transport::grpc::{Grpc, GrpcConfig},
};
use eyre::Context;
use tokio::{
    sync::Barrier,
    task::{AbortHandle, JoinSet},
    time::timeout,
};
use tracing::{debug, info, instrument};

use crate::{GRPC_PORT, UUID, interfaces::INTERFACES};

pub struct Node {
    handle: AbortHandle,
    pub client: DeviceClient<Grpc>,
}

impl Node {
    pub async fn recv(&mut self) -> eyre::Result<DeviceEvent> {
        let event = timeout(Duration::from_secs(5), self.client.recv())
            .await?
            .wrap_err("error from the sdk")?;

        Ok(event)
    }

    pub async fn close(self) -> eyre::Result<()> {
        self.handle.abort();

        Ok(())
    }
}

#[instrument(skip_all)]
pub async fn init_node(
    barrier: &Arc<Barrier>,
    tasks: &mut JoinSet<eyre::Result<()>>,
) -> eyre::Result<Node> {
    let grpc = GrpcConfig::from_url(UUID, format!("http://[::1]:{GRPC_PORT}"))?;

    let mut builder = DeviceBuilder::new().store(MemoryStore::new());

    for iface in INTERFACES {
        builder = builder.interface_str(iface)?
    }

    debug!("Start connecting to the msghub");
    let handle = tokio::spawn(async move {
        let builder = builder.connection(grpc);
        builder.build().await
    });
    barrier.wait().await;
    info!("Connected to the message hub");

    let (client, connection) = handle.await??;

    let handle = tasks.spawn(async move { connection.handle_events().await.map_err(Into::into) });

    Ok(Node { client, handle })
}
