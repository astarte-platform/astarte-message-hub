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

use astarte_device_sdk::builder::{DeviceBuilder, DeviceSdkBuild};
use astarte_device_sdk::store::SqliteStore;
use astarte_device_sdk::transport::mqtt::{Mqtt, MqttConfig};
use astarte_device_sdk::{DeviceClient, DeviceConnection, EventLoop};
use eyre::Context;
use std::convert::identity;
use std::net::Ipv6Addr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::task::JoinSet;

use astarte_message_hub::{
    astarte::handler::init_pub_sub, config::MessageHubOptions, AstarteMessageHub,
};
use astarte_message_hub_proto::message_hub_server::MessageHubServer;
use clap::Parser;
use eyre::eyre;
use log::{debug, info};

/// A central service that runs on (Linux) devices for collecting and delivering messages from N
/// apps using 1 MQTT connection to Astarte.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Path to a valid .toml file containing the message hub configuration.
    #[clap(short, long, conflicts_with = "store-directory")]
    toml: Option<String>,
    /// Directory used by Astarte-Message-Hub to retain configuration and other persistent data.
    #[clap(short, long, conflicts_with = "toml")]
    store_directory: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    stable_eyre::install()?;
    env_logger::try_init()?;

    let args = Cli::parse();

    let store_directory = args.store_directory.as_deref();

    let mut options = MessageHubOptions::get(args.toml, store_directory).await?;

    // Initialize an Astarte device
    let (client, mut connection) = initialize_astarte_device_sdk(&mut options).await?;
    info!("Connection to Astarte established.");

    let (publisher, mut subscriber) = init_pub_sub(client);

    // Create a new message hub
    let message_hub = AstarteMessageHub::new(publisher);

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

    // Handles the Message Hub gRPC server
    tasks.spawn(async move {
        // Run the proto-buff server
        let addrs = (Ipv6Addr::LOCALHOST, options.grpc_socket_port).into();

        tonic::transport::Server::builder()
            .add_service(MessageHubServer::new(message_hub))
            .serve(addrs)
            .await
            .wrap_err("couldn't start the server")
    });

    while let Some(res) = tasks.join_next().await {
        // Crash if one of the tasks returned an error
        res.wrap_err("failed to join task").and_then(identity)?;
    }

    Ok(())
}

async fn initialize_astarte_device_sdk(
    msg_hub_opts: &mut MessageHubOptions,
) -> eyre::Result<(
    DeviceClient<SqliteStore>,
    DeviceConnection<SqliteStore, Mqtt>,
)> {
    tokio::fs::create_dir_all(&msg_hub_opts.store_directory)
        .await
        .wrap_err_with(|| {
            format!(
                "couldn't create store directory {}",
                msg_hub_opts.store_directory.display()
            )
        })?;

    // retrieve the device id
    msg_hub_opts.obtain_device_id().await?;

    // Obtain the credentials secret, the store defaults to the current directory
    let cred = msg_hub_opts.obtain_credential().await?;

    // initialize the device options and mqtt config
    let mut mqtt_config = MqttConfig::new(
        &msg_hub_opts.realm,
        msg_hub_opts.device_id.as_ref().unwrap(),
        cred,
        &msg_hub_opts.pairing_url,
    );

    #[allow(deprecated)]
    if msg_hub_opts.astarte_ignore_ssl || msg_hub_opts.astarte.ignore_ssl {
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

    let store_path = msg_hub_opts
        .store_directory
        .to_str()
        .map(|d| format!("sqlite://{d}/database.db"))
        .ok_or_else(|| eyre!("non UTF-8 store directory option"))?;

    let store = SqliteStore::from_uri(&store_path).await?;

    // create a device instance
    let (client, connection) = builder.store(store).connect(mqtt_config).await?.build();

    Ok((client, connection))
}
