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
#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![expect(clippy::result_large_err)]

pub use crate::server::AstarteMessageHub;

pub mod astarte;
pub mod cache;
pub mod config;
pub mod error;
#[cfg(feature = "security-events")]
pub mod events;
mod messages;
mod server;
pub mod store;

#[cfg(test)]
pub(crate) mod tests {
    use astarte_device_sdk::pairing::api::PairingApi;
    use astarte_device_sdk::store::SqliteStore;
    use astarte_device_sdk::transport::mqtt::Mqtt;
    use insta::assert_snapshot;

    pub(crate) type MockClient =
        astarte_device_sdk_mock::MockDeviceClient<Mqtt<SqliteStore, PairingApi>>;
    pub(crate) type MockConnection =
        astarte_device_sdk_mock::MockDeviceConnection<Mqtt<SqliteStore, PairingApi>>;

    macro_rules! with_settings {
        ($asserts:block) => {
            ::insta::with_settings!({
                snapshot_path => concat!(env!("CARGO_MANIFEST_DIR"), "/snapshots")
            }, $asserts);
        };
    }

    pub(crate) use with_settings;

    #[test]
    fn use_macro() {
        self::with_settings!({
            assert_snapshot!("using the macro");
        });
    }
}
