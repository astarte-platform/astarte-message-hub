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

use astarte_device_sdk::types::AstarteType;
use astarte_device_sdk::{AstarteDeviceDataEvent, AstarteError, Interface};
use mockall::automock;

pub struct AstarteDeviceSdk {}

#[cfg_attr(test, automock)]
impl AstarteDeviceSdk {
    #[allow(dead_code)]
    pub async fn handle_events(&mut self) -> Result<AstarteDeviceDataEvent, AstarteError> {
        todo!()
    }
    #[allow(dead_code)]
    pub async fn send<D: 'static>(
        &self,
        _interface_name: &str,
        _interface_path: &str,
        _data: D,
    ) -> Result<(), AstarteError>
    where
        D: Into<AstarteType>,
    {
        todo!()
    }
    #[allow(dead_code)]
    pub async fn send_with_timestamp<D: 'static>(
        &self,
        _interface_name: &str,
        _interface_path: &str,
        _data: D,
        _timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), AstarteError>
    where
        D: Into<AstarteType>,
    {
        todo!()
    }
    #[allow(dead_code)]
    pub async fn send_object<T: 'static>(
        &self,
        _interface_name: &str,
        _interface_path: &str,
        _data: T,
    ) -> Result<(), AstarteError>
    where
        T: serde::Serialize,
    {
        todo!()
    }
    #[allow(dead_code)]
    pub async fn send_object_with_timestamp<T: 'static>(
        &self,
        _interface_name: &str,
        _interface_path: &str,
        _data: T,
        _timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), AstarteError>
    where
        T: serde::Serialize,
    {
        todo!()
    }
    #[allow(dead_code)]
    pub async fn unset<D: 'static>(
        &self,
        _interface_name: &str,
        _interface_path: &str,
    ) -> Result<(), AstarteError>
    where
        D: Into<AstarteType>,
    {
        todo!()
    }
    #[allow(dead_code)]
    pub async fn add_interface(&self, _interface: Interface) -> Result<(), AstarteError> {
        todo!()
    }
}
