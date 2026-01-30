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
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use self::dbus::DbusConfig;
use self::file::dynamic::DynamicConfig;
use self::file::sdk::{DeviceSdkOptions, DeviceSdkStoreOptions, DeviceSdkVolatileOptions};
use self::file::{CONFIG_FILE_NAME, Config};

pub mod dbus;
pub mod file;
pub mod grpc;
pub mod http;

/// Default host to bind the HTTP/gRPC server to.
pub const DEFAULT_HOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
/// Default port to bind the gRPC server to.
pub const DEFAULT_GRPC_PORT: u16 = 50051;
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
    /// Directory to cache the nodes introspection.
    pub introspection_cache: PathBuf,
    /// The gRPC host to use bind.
    pub grpc_socket_host: IpAddr,
    /// The gRPC port to use.
    pub grpc_socket_port: u16,
    /// Directory used by Astarte-Message-Hub to retain configuration and other persistent data.
    pub store_directory: PathBuf,
    /// Astarte device SDK options.
    pub astarte: DeviceSdkOptions,
}

impl MessageHubOptions {
    /// Returns the builder for the message hub options
    pub fn builder() -> MessageHubOptionsBuilder<'static> {
        MessageHubOptionsBuilder::default()
    }

    /// Default the store directory to the current working directory.
    fn default_store_directory() -> PathBuf {
        dirs::data_dir()
            .map(|p| p.join("message-hub"))
            .unwrap_or_else(|| {
                warn!("couldn't get data directory, using current working directory");

                PathBuf::from(".")
            })
    }

    /// Initializes the Astarte Device client and connection
    pub async fn create_connection(
        &self,
    ) -> eyre::Result<(
        DeviceClient<Mqtt<SqliteStore>>,
        DeviceConnection<Mqtt<SqliteStore>>,
    )> {
        tokio::fs::create_dir_all(&self.store_directory)
            .await
            .wrap_err_with(|| {
                format!(
                    "couldn't create store directory {}",
                    self.store_directory.display()
                )
            })?;

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

        let mut builder = DeviceBuilder::new().writable_dir(&self.store_directory)?;

        if let Some(timeout) = self.astarte.timeout_secs {
            builder = builder.connection_timeout(Duration::from_secs(timeout))
        }

        if let Some(ref int_dir) = self.interfaces_directory {
            debug!("reading interfaces from {}", int_dir.display());

            builder = builder.interface_directory(int_dir)?;
        }

        builder = builder.interface_directory(&self.introspection_cache)?;

        let DeviceSdkVolatileOptions {
            max_retention_items,
        } = self.astarte.volatile.clone().unwrap_or_default();

        if let Some(max_volatile_items) = max_retention_items {
            debug!("setting astarte max number of volatile items to {max_volatile_items}");
            builder = builder.max_volatile_retention(max_volatile_items);
        }

        let store_path = self
            .store_directory
            .to_str()
            .map(|d| format!("{d}/database.db"))
            .ok_or_eyre("non UTF-8 store directory option")?;

        let mut store = SqliteStore::connect_db(&store_path).await?;

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

/// Builder for the [`MessageHubOptions`].
#[derive(Debug, Default)]
pub struct MessageHubOptionsBuilder<'a> {
    overrides: Config,
    custom_config: Option<&'a Path>,
    config_dir: Option<&'a Path>,
    storage_dir: Option<&'a Path>,
}

impl<'a> MessageHubOptionsBuilder<'a> {
    /// Sets the custom overrides
    pub fn set_overrides(mut self, config: Config) -> Self {
        self.overrides = config;

        self
    }

    /// Sets the configuration directory
    pub fn set_config_dir(&mut self, config_dir: &'a Path) -> &mut Self {
        self.config_dir = Some(config_dir);

        self
    }

    /// Sets the storage directory
    pub fn set_storage_dir(&mut self, storage_dir: &'a Path) -> &mut Self {
        self.storage_dir = Some(storage_dir);

        self
    }

    /// Set custom config
    pub fn set_custom_config(&mut self, custom_config: &'a Path) -> &mut Self {
        self.custom_config = Some(custom_config);

        self
    }

    /// Builds the message hub options
    pub async fn build(
        self,
        tasks: &mut JoinSet<eyre::Result<()>>,
        cancel_token: CancellationToken,
    ) -> eyre::Result<Option<MessageHubOptions>> {
        let mut config = Config::read_files(self.config_dir, self.storage_dir).await?;

        if let Some(custom) = self.custom_config {
            config.read_and_merge(custom).await?;
        }

        config.merge(self.overrides);

        if let Some(dynamic) = &config.dynamic_config {
            if dynamic.enabled == Some(true) {
                let res = listen_dynamic_config(
                    tasks,
                    cancel_token,
                    dynamic,
                    config.store_directory.as_deref(),
                )
                .await?;

                let Some(dyn_config) = res else {
                    info!("tasks cancelled, exiting");

                    return Ok(None);
                };

                config.merge(dyn_config);
            }
        }

        config.legacy_credential_secret().await?;

        if config
            .device_id
            .as_ref()
            .is_none_or(|device_id| device_id.is_empty())
        {
            let device_id = hardware_id().await?;

            config.device_id = Some(device_id);
        }

        MessageHubOptions::try_from(config)
            .map(Some)
            .wrap_err("couldn't get Message Hub options")
    }
}

async fn hardware_id() -> eyre::Result<String> {
    let connection = zbus::Connection::system().await?;

    let dbus = DbusConfig::connect(&connection).await?;

    dbus.device_id_from_hardware_id().await
}

/// Function that get the configurations needed by the Message Hub.
/// The configuration file is first retrieved from one of two default base locations.
/// If no valid configuration file is found in either of these locations, or if the content
/// of the first found file is not valid HTTP and Protobuf APIs are exposed to provide a valid
/// configuration.
async fn listen_dynamic_config(
    tasks: &mut JoinSet<eyre::Result<()>>,
    cancel_token: CancellationToken,
    dynamic: &DynamicConfig,
    store_dir: Option<&Path>,
) -> eyre::Result<Option<Config>> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);

    let config_file = store_dir.map(|p| p.join(CONFIG_FILE_NAME));

    let http_address = (
        dynamic.http_host.unwrap_or(DEFAULT_HOST),
        dynamic.http_port.unwrap_or(DEFAULT_HTTP_PORT),
    )
        .into();

    let grpc_address = (
        dynamic.grpc_host.unwrap_or(DEFAULT_HOST),
        dynamic.grpc_port.unwrap_or(DEFAULT_GRPC_PORT),
    )
        .into();

    self::http::serve(
        tasks,
        cancel_token.child_token(),
        &http_address,
        tx.clone(),
        config_file.clone(),
    )
    .await?;

    grpc::serve(
        tasks,
        cancel_token.child_token(),
        grpc_address,
        tx,
        config_file,
    )
    .await?;

    let res = cancel_token
        .run_until_cancelled(rx.recv())
        .await
        .ok_or_eyre("dynamic config server exited")?;

    match res {
        Some(config) => {
            info!("dynamic config received");

            Ok(Some(config))
        }
        None => {
            info!("dynamic config cancelled");

            Ok(None)
        }
    }
}
