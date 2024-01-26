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

//! Mocking of the Astarte Device Sdk.

use astarte_device_sdk::{types::AstarteType, AstarteAggregate, Error, Interface};
use async_trait::async_trait;
use mockall::{automock, mock};

/// Equivalent version of the [`Client`](astarte_device_sdk::Client) trait.
///
/// This trait has been redefined because of some mocking constraints regarding generic parameters that prevented
/// it to be mocked.
#[automock]
#[async_trait]
pub trait Client {
    async fn send_object_with_timestamp<D: 'static>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send;

    async fn send_object<D: 'static>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send;

    async fn send<D: 'static>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType> + Send;

    async fn send_with_timestamp<D: 'static>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType> + Send;

    async fn unset(&self, interface_name: &str, interface_path: &str) -> Result<(), Error>;

    async fn handle_events(&mut self) -> Result<(), Error>;
    async fn add_interface(&self, interface: Interface) -> Result<(), Error>;

    async fn remove_interface(&self, interface_name: &str) -> Result<(), Error>;
}

mock! {
    pub AstarteDeviceSdk<S:'static + Sync + Send, C:'static + Sync + Send> {}

    #[async_trait]
    impl<S: 'static + Sync + Send, C: 'static + Sync + Send> Client for AstarteDeviceSdk<S,C> {
        async fn send_object_with_timestamp<D>(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: D,
            timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error>
        where
            D: AstarteAggregate + Send + 'static;

        async fn send_object<D: 'static>(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: D,
        ) -> Result<(), Error>
        where
            D: AstarteAggregate + Send;

        async fn send<D: 'static>(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: D,
        ) -> Result<(), Error>
        where
            D: TryInto<AstarteType> + Send;

        async fn send_with_timestamp<D: 'static>(
            &self,
            interface_name: &str,
            interface_path: &str,
            data: D,
            timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), Error>
        where
            D: TryInto<AstarteType> + Send;

        async fn unset(&self, interface_name: &str, interface_path: &str) -> Result<(), Error>;

        async fn handle_events(&mut self) -> Result<(), Error>;
        async fn add_interface(&self, interface: Interface) -> Result<(), Error>;

        async fn remove_interface(&self, interface_name: &str) -> Result<(), Error>;
    }


    impl<S: 'static + Sync + Send, C: 'static + Sync + Send> Clone for AstarteDeviceSdk<S,C> {
        fn clone(&self) -> Self;
    }
}
