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

//! Configuration for the Message Hub.

use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use astarte_device_sdk::builder::DeviceBuilder;
use astarte_device_sdk::store::SqliteStore;
use astarte_device_sdk::transport::mqtt::{Credential, Mqtt, MqttConfig};
use astarte_device_sdk::{DeviceClient, DeviceConnection};
use eyre::{Context, OptionExt};
use tracing::{debug, info};

use crate::store::StoreDir;

use self::dbus::DbusConfig;
use self::file::Config;
use self::file::sdk::{DeviceSdkOptions, DeviceSdkStoreOptions, DeviceSdkVolatileOptions};
use self::loader::ConfigRepository;

pub mod dbus;
pub mod dynamic;
pub mod file;
pub mod loader;

/// Default host to bind the HTTP/gRPC server to.
pub const DEFAULT_HOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
/// Default port to bind the gRPC server to.
pub const DEFAULT_GRPC_PORT: u16 = 50051;
/// Default port to bind the gRPC dynamic config server to.
pub const DEFAULT_GRPC_CONFIG_PORT: u16 = 50049;
/// Default port to bind for the dynamic configuration over HTTP.
pub const DEFAULT_HTTP_PORT: u16 = 40041;

/// Merges a configuration with an override.
///
/// The value set in the override must take precedence.
pub trait Override {
    /// Merges the override into self.
    fn merge(&mut self, overrides: Self);
}

impl<T> Override for Option<T> {
    fn merge(&mut self, value: Self) {
        if value.is_some() {
            *self = value;
        }
    }
}

/// Struct containing all the configuration options for the Astarte message hub.
#[derive(Debug, PartialEq, Eq)]
pub struct MessageHubOptions {
    /// The Astarte realm the device belongs to.
    pub realm: String,
    /// A unique ID for the device.
    pub device_id: String,
    /// The URL of the Astarte pairing API.
    pub pairing_url: String,
    /// The credentials secret used to authenticate with Astarte.
    pub credential: Credential,
    /// Directory containing the Astarte interfaces.
    pub interfaces_directory: Option<PathBuf>,
    /// The gRPC host to use bind.
    pub grpc_socket_host: IpAddr,
    /// The gRPC port to use.
    pub grpc_socket_port: u16,
    /// Astarte device SDK options.
    pub astarte: DeviceSdkOptions,
}

impl MessageHubOptions {
    /// Returns the builder for the message hub options
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// Initializes the Astarte Device client and connection
    pub async fn create_connection(
        &self,
        store_dir: &StoreDir,
    ) -> eyre::Result<(
        DeviceClient<Mqtt<SqliteStore>>,
        DeviceConnection<Mqtt<SqliteStore>>,
    )> {
        // initialize the device options and mqtt config
        let mut mqtt_config = MqttConfig::new(
            &self.realm,
            &self.device_id,
            self.credential.clone(),
            &self.pairing_url,
        );

        if self.astarte.ignore_ssl == Some(true) {
            mqtt_config.ignore_ssl_errors();
        }

        if let Some(keep_alive) = self.astarte.keep_alive_secs {
            mqtt_config.keepalive(Duration::from_secs(keep_alive));
        }

        let store_directory = store_dir.get_store_dir().await?;
        let mut builder = DeviceBuilder::new().writable_dir(store_directory)?;

        if let Some(timeout) = self.astarte.timeout_secs {
            builder = builder.connection_timeout(Duration::from_secs(timeout))
        }

        if let Some(ref int_dir) = self.interfaces_directory {
            debug!("reading interfaces from {}", int_dir.display());

            builder = builder.interface_directory(int_dir)?;
        }

        if let Some(interface_cache) = store_dir.get_interfaces_cache_dir().await {
            builder = builder.interface_directory(interface_cache)?;
        }

        let DeviceSdkVolatileOptions {
            max_retention_items,
        } = self.astarte.volatile.clone().unwrap_or_default();

        if let Some(max_volatile_items) = max_retention_items {
            debug!("setting astarte max number of volatile items to {max_volatile_items}");
            builder = builder.max_volatile_retention(max_volatile_items);
        }

        let db_path = store_directory
            .to_str()
            .map(|d| format!("{d}/database.db"))
            .ok_or_eyre("non UTF-8 store directory option")?;

        let mut store = SqliteStore::connect_db(&db_path).await?;

        let DeviceSdkStoreOptions {
            max_db_size,
            max_db_journal_size,
            max_retention_items,
        } = self.astarte.store.clone().unwrap_or_default();

        if let Some(s) = max_db_size {
            debug!("setting astarte max db size to {s:?}");
            store.set_db_max_size(s).await?;
        } else {
            debug!("astarte max db size is not set, using default");
        }

        if let Some(s) = max_db_journal_size {
            debug!("setting astarte max db journal size to {s:?}");
            store.set_journal_size_limit(s).await?;
        } else {
            debug!("astarte max db journal size is not set, using default");
        }

        let mut builder = builder.store(store);

        if let Some(max_stored_items) = max_retention_items {
            debug!("setting astarte max number of stored items to {max_stored_items}");
            builder = builder.max_stored_retention(max_stored_items);
        }

        // create a device instance
        let (client, connection) = builder.connection(mqtt_config).build().await?;

        info!("Astarte device initialized");

        Ok((client, connection))
    }
}

/// Custom config argument
#[derive(Debug)]
pub enum CustomConfig {
    /// A path to ta TOML file
    File(PathBuf),
    /// A path to ta a directory
    Dir(PathBuf),
}

/// Builder for the [`MessageHubOptions`].
#[derive(Debug, Default)]
pub struct ConfigBuilder {
    overrides: Config,
    custom: Option<CustomConfig>,
}

impl ConfigBuilder {
    /// Sets the custom overrides
    pub fn set_overrides(&mut self, config: Config) -> &mut Self {
        self.overrides = config;

        self
    }

    /// Sets the configuration directory
    pub fn set_custom(&mut self, custom: CustomConfig) -> &mut Self {
        self.custom = Some(custom);

        self
    }

    /// Builds the message hub options
    pub async fn build(self) -> eyre::Result<(ConfigRepository, StoreDir)> {
        let mut config = ConfigRepository::with_overrides(self.overrides);

        config.read_configs(&self.custom).await?;

        let store_dir = config
            .get_store_directory()
            .map(Path::to_path_buf)
            .unwrap_or_else(Config::default_storage_dir);

        info!(path = %store_dir.display(), "using store directory");

        let store_dir = StoreDir::create(store_dir).await?;

        config.read_dynamic(&store_dir).await?;

        Ok((config, store_dir))
    }
}

pub(crate) async fn hardware_id() -> eyre::Result<String> {
    let connection = zbus::Connection::system()
        .await
        .wrap_err("couldn't connect to dbus")?;

    let dbus = DbusConfig::connect(&connection).await?;

    dbus.device_id_from_hardware_id().await
}
