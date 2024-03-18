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

use std::time::Duration;

use astarte_device_sdk::{
    builder::DeviceBuilder,
    prelude::*,
    store::memory::MemoryStore,
    transport::grpc::{Grpc, GrpcConfig},
    AstarteDeviceDataEvent, AstarteDeviceSdk, EventReceiver,
};
use eyre::{Context, OptionExt};
use tokio::{
    task::{AbortHandle, JoinSet},
    time::timeout,
};
use tracing::{debug, info, instrument};

use crate::{interfaces::INTERFACES, GRPC_PORT, UUID};

pub type DeviceSdk = AstarteDeviceSdk<MemoryStore, Grpc>;

#[derive(Debug)]
pub struct Node {
    handle: AbortHandle,
    pub device: DeviceSdk,
    pub rx_events: EventReceiver,
}

impl Node {
    pub async fn recv(&mut self) -> eyre::Result<AstarteDeviceDataEvent> {
        let event = timeout(Duration::from_secs(5), self.rx_events.recv())
            .await?
            .ok_or_eyre("channel closed")?
            .wrap_err("error from the sdk")?;

        Ok(event)
    }

    pub async fn close(self) -> eyre::Result<()> {
        self.handle.abort();

        Ok(())
    }
}

#[instrument(skip_all)]
pub async fn init_node(tasks: &mut JoinSet<eyre::Result<()>>) -> eyre::Result<Node> {
    let grpc = GrpcConfig::new(UUID, format!("http://localhost:{GRPC_PORT}"));

    let mut builder = DeviceBuilder::new().store(MemoryStore::new());

    for iface in INTERFACES {
        builder = builder.interface(iface)?
    }

    debug!("Start connecting to the msghub");
    let handle = tokio::spawn(async move { builder.connect(grpc).await });
    info!("Connected to the message hub");

    let (device, rx_events) = handle.await??.build();

    let mut device_cl = device.clone();
    let handle = tasks.spawn(async move { device_cl.handle_events().await.map_err(Into::into) });

    Ok(Node {
        device,
        rx_events,
        handle,
    })
}
