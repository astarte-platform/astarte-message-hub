// This file is part of Astarte.
//
// Copyright 2022, 2026 SECO Mind Srl
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

//! Message Hub configuration via DBus

use eyre::eyre;
use tracing::info;
use zbus::{Connection, proxy};

#[proxy(
    interface = "io.edgehog.Device1",
    default_service = "io.edgehog.Device",
    default_path = "/io/edgehog/Device"
)]
pub(crate) trait Device {
    fn get_hardware_id(&self, namespace: &str) -> zbus::Result<String>;
}

// TODO: mock the dbus interface
/// DBus configuration handler
#[derive(Debug)]
pub struct DbusConfig<P> {
    proxy: P,
}

impl<'a> DbusConfig<DeviceProxy<'a>> {
    /// Create the dbus config for the given connection
    pub async fn connect(connection: &'a Connection) -> eyre::Result<Self> {
        let proxy = DeviceProxy::new(connection).await?;

        Ok(Self { proxy })
    }

    /// Gets the device id from the Hardware Id using DBUS.
    pub async fn device_id_from_hardware_id(&self) -> eyre::Result<String> {
        let hardware_id = self.proxy.get_hardware_id("").await?;

        if hardware_id.is_empty() {
            return Err(eyre!("DBUS hardware ID is empty"));
        }

        info!(hardware_id, "hardware id retrieved");

        Ok(hardware_id)
    }
}
