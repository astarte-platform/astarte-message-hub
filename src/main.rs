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
//! Contains the main application runner for the Astarte message hub.

//! A central service that runs on (Linux) devices for collecting and delivering messages from N
//! apps using 1 MQTT connection to Astarte.

#![warn(missing_docs)]

use astarte_device_sdk::builder::DeviceBuilder;
use astarte_device_sdk::store::SqliteStore;
use astarte_device_sdk::transport::mqtt::{Mqtt, MqttConfig};
use astarte_device_sdk::{DeviceClient, DeviceConnection, EventLoop};
use astarte_message_hub::config::{Config, DEFAULT_HOST, DEFAULT_HTTP_PORT};
use eyre::{Context, OptionExt};
use std::convert::identity;
use std::io::{stdout, IsTerminal};
use std::path::Path;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use astarte_message_hub::{
    astarte::handler::init_pub_sub, config::MessageHubOptions, AstarteMessageHub,
};
use astarte_message_hub_proto::message_hub_server::MessageHubServer;
use clap::Parser;
use eyre::eyre;
use log::{debug, error, info, warn};

use crate::cli::Cli;

mod cli;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    stable_eyre::install()?;

    init_tracing()?;

    // Set default crypto provider
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| eyre!("failed to install default crypto provider"))?;

    let args = Cli::parse();

    if args.toml.is_some() {
        warn!(
            "DEPRECATED: the '-t/--toml' option is deprecated in favour of the '-c/--config' flag"
        )
    }

    let options = get_config_options(args).await?;

    // Directory to store the Nodes introspection
    let interfaces_dir = options.store_directory.join("interfaces");
    if !interfaces_dir.exists() {
        tokio::fs::create_dir_all(&interfaces_dir)
            .await
            .wrap_err("couldn't create interface directory")?;
    }

    // Initialize an Astarte device
    let (client, connection) = initialize_astarte_device_sdk(&options, &interfaces_dir).await?;
    info!("Connection to Astarte established.");

    let (publisher, mut subscriber) = init_pub_sub(client);

    // Create a new message hub
    let message_hub = AstarteMessageHub::new(publisher, interfaces_dir);

    let mut tasks = JoinSet::new();

    // Event loop for the astarte device sdk
    tasks.spawn(async move {
        connection
            .handle_events()
            .await
            .wrap_err("Astarte disconnected")
    });

    // Forward the astarte events to the subscribers
    tasks.spawn(async move {
        subscriber
            .forward_events()
            .await
            .wrap_err("subscriber disconnected")
    });

    let addrs = (options.grpc_socket_host, options.grpc_socket_port).into();

    // Handles the Message Hub gRPC server
    tasks.spawn(async move {
        // Run the proto-buff server

        info!("Starting the server on grpc://{addrs}");

        let shutdown = shutdown()?;

        tonic::transport::Server::builder()
            .layer(message_hub.make_interceptor_layer())
            .add_service(MessageHubServer::new(message_hub))
            .serve_with_shutdown(addrs, shutdown)
            .await
            .wrap_err("couldn't start the server")
    });

    while let Some(res) = tasks.join_next().await {
        // Crash if one of the tasks returned an error
        res.wrap_err("failed to join task").and_then(identity)?;

        // Otherwise abort all if one exited
        tasks.abort_all();
    }

    Ok(())
}

async fn get_config_options(args: Cli) -> eyre::Result<MessageHubOptions> {
    let store_directory = args.device.store_dir.as_deref();
    let custom_config = args.config.as_deref().or(args.toml.as_deref());

    let mut config = match Config::find_config(custom_config, store_directory).await? {
        Some(config) => config,
        None => {
            let store_directory = args.device.store_dir.as_deref().ok_or_eyre(
                "no configuration file specified and store directory missing  to start dynamic configuration",
            )?;

            let http = (
                args.http.http_host.unwrap_or(DEFAULT_HOST),
                args.http.http_port.unwrap_or(DEFAULT_HTTP_PORT),
            )
                .into();

            let grpc = (
                args.grpc.host.unwrap_or(DEFAULT_HOST),
                args.grpc.port.unwrap_or(DEFAULT_HTTP_PORT),
            )
                .into();

            Config::listen_dynamic_config(store_directory, http, grpc).await?
        }
    };

    args.merge(&mut config);

    if config.device_id.is_none() {
        // retrieve the device id
        config.device_id_from_hardware_id().await?;
    }

    // Read the credentials secret, the store defaults to the current directory
    config.read_credential_secret().await?;

    Ok(config.try_into()?)
}

async fn initialize_astarte_device_sdk(
    msg_hub_opts: &MessageHubOptions,
    interfaces_dir: &Path,
) -> eyre::Result<(
    DeviceClient<Mqtt<SqliteStore>>,
    DeviceConnection<Mqtt<SqliteStore>>,
)> {
    tokio::fs::create_dir_all(&msg_hub_opts.store_directory)
        .await
        .wrap_err_with(|| {
            format!(
                "couldn't create store directory {}",
                msg_hub_opts.store_directory.display()
            )
        })?;

    // initialize the device options and mqtt config
    let mut mqtt_config = MqttConfig::new(
        &msg_hub_opts.realm,
        &msg_hub_opts.device_id,
        msg_hub_opts.credential.clone(),
        &msg_hub_opts.pairing_url,
    );

    if msg_hub_opts.astarte.ignore_ssl {
        mqtt_config.ignore_ssl_errors();
    }

    if let Some(timeout) = msg_hub_opts.astarte.timeout_secs {
        mqtt_config.connection_timeout(Duration::from_secs(timeout));
    }

    if let Some(keep_alive) = msg_hub_opts.astarte.keep_alive_secs {
        mqtt_config.keepalive(Duration::from_secs(keep_alive));
    }

    let mut builder = DeviceBuilder::new().writable_dir(&msg_hub_opts.store_directory)?;

    if let Some(ref int_dir) = msg_hub_opts.interfaces_directory {
        debug!("reading interfaces from {}", int_dir.display());

        builder = builder.interface_directory(int_dir)?;
    }

    builder = builder.interface_directory(interfaces_dir)?;

    if let Some(max_volatile_items) = msg_hub_opts.astarte.volatile.max_retention_items {
        debug!("setting astarte max number of volatile items to {max_volatile_items}");
        builder = builder.max_volatile_retention(max_volatile_items);
    }

    let store_path = msg_hub_opts
        .store_directory
        .to_str()
        .map(|d| format!("{d}/database.db"))
        .ok_or_else(|| eyre!("non UTF-8 store directory option"))?;

    let mut store = SqliteStore::connect_db(&store_path).await?;

    if let Some(s) = msg_hub_opts.astarte.store.max_db_size {
        debug!("setting astarte max db size to {s:?}");
        store.set_db_max_size(s).await?;
    } else {
        debug!("astarte max db size is not set, using default");
    }

    if let Some(s) = msg_hub_opts.astarte.store.max_db_journal_size {
        debug!("setting astarte max db journal size to {s:?}");
        store.set_journal_size_limit(s).await?;
    } else {
        debug!("astarte max db journal size is not set, using default");
    }

    let mut builder = builder.store(store);

    if let Some(max_stored_items) = msg_hub_opts.astarte.store.max_retention_items {
        debug!("setting astarte max number of stored items to {max_stored_items}");
        builder = builder.max_stored_retention(max_stored_items);
    }

    // create a device instance
    let (client, connection) = builder.connection(mqtt_config).build().await?;

    Ok((client, connection))
}

#[cfg(unix)]
fn shutdown() -> eyre::Result<impl std::future::Future<Output = ()>> {
    use futures::FutureExt;
    use tokio::signal::unix::SignalKind;

    let mut term = tokio::signal::unix::signal(SignalKind::terminate())
        .wrap_err("couldn't create SIGTERM listener")?;

    let future = async move {
        let term = std::pin::pin!(async move {
            if term.recv().await.is_none() {
                error!("no more signal events can be received")
            }
        });

        let ctrl_c = std::pin::pin!(tokio::signal::ctrl_c().map(|res| {
            if let Err(err) = res {
                error!("couldn't receive SIGINT {err}");
            }
        }));

        futures::future::select(term, ctrl_c).await;
    };

    Ok(future)
}

#[cfg(not(unix))]
fn shutdown() -> eyre::Result<impl std::future::Future<Output = ()>> {
    use futures::FutureExt;

    Ok(tokio::signal::ctrl_c().map(|res| {
        if let Err(err) = res {
            error!("couldn't receive SIGINT {err}");
        }
    }))
}

fn init_tracing() -> eyre::Result<()> {
    let fmt = tracing_subscriber::fmt::layer().with_ansi(stdout().is_terminal());

    tracing_subscriber::registry()
        .with(fmt)
        .with(
            EnvFilter::builder()
                .with_default_directive("astarte_message_hub=info".parse()?)
                .from_env_lossy(),
        )
        .try_init()?;

    Ok(())
}
