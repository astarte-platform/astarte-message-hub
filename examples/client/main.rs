/*
 * This file is part of Astarte.
 *
 * Copyright 2023 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

//! Astarte Message Hub client example, will send the uptime every 3 seconds to Astarte.

use astarte_device_sdk::builder::DeviceBuilder;
use astarte_device_sdk::client::{ClientDisconnect, RecvError};
use astarte_device_sdk::store::memory::MemoryStore;
use astarte_device_sdk::transport::grpc::GrpcConfig;
use astarte_device_sdk::{Client, EventLoop};

use clap::Parser;
use log::{error, info};
use std::time;
use tokio::signal::ctrl_c;
use tokio::task::JoinSet;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const DEVICE_DATASTREAM: &str = include_str!(
    "./interfaces/org.astarte-platform.rust.examples.datastream.DeviceDatastream.json"
);
const SERVER_DATASTREAM: &str = include_str!(
    "./interfaces/org.astarte-platform.rust.examples.datastream.ServerDatastream.json"
);

/// Create a ProtoBuf client for the Astarte message hub.
#[derive(Parser, Debug)]
#[clap(version, about)]
struct Cli {
    /// UUID to be used when registering the client as an Astarte message hub node.
    #[clap(default_value = "d1e7a6e9-cf99-4694-8fb6-997934be079c")]
    uuid: String,

    /// Endpoint of the Astarte Message Hub server instance
    #[clap(default_value = "http://[::1]:50051")]
    endpoint: String,

    /// Stop after sending COUNT messages.
    #[clap(short, long)]
    count: Option<u64>,

    /// Milliseconds to wait between messages.
    #[clap(short, long, default_value = "3000")]
    time: u64,
}

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    stable_eyre::install()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()?;

    let args = Cli::parse();
    let node_id: Uuid = Uuid::parse_str(&args.uuid)?;

    let grpc_cfg = GrpcConfig::from_url(node_id, args.endpoint)?;

    let (client, connection) = DeviceBuilder::new()
        .store(MemoryStore::new())
        .interface_str(DEVICE_DATASTREAM)?
        .interface_str(SERVER_DATASTREAM)?
        .connection(grpc_cfg)
        .build()
        .await?;

    let mut tasks = JoinSet::<Result<(), DynError>>::new();

    let client_cl = client.clone();

    tasks.spawn(async move {
        info!("start receiving messages from the Astarte Message Hub Server");
        loop {
            match client_cl.recv().await {
                Ok(event) => {
                    info!("received {event:?}");
                }
                Err(RecvError::Disconnected) => break,
                Err(err) => {
                    error!("error while receiving data from Astarte Message Hub Server: {err:?}")
                }
            }
        }

        Ok(())
    });

    // Create a task to transmit
    tasks.spawn({
        let client = client.clone();

        async move {
            let now = time::SystemTime::now();
            let mut count = 0;
            // Consistent interval of 3 seconds
            let mut interval = tokio::time::interval(time::Duration::from_millis(args.time));

            while args.count.is_none() || Some(count) < args.count {
                interval.tick().await;

                info!("Publishing the uptime through the message hub.");

                let elapsed = now.elapsed()?.as_secs();

                let elapsed_str = format!("Uptime for node {}: {}", args.uuid, elapsed);

                client
                    .send(
                        "org.astarte-platform.rust.examples.datastream.DeviceDatastream",
                        "/uptime",
                        elapsed_str,
                    )
                    .await?;

                count += 1;
            }

            info!("Done sending messages");

            Ok(())
        }
    });

    tasks.spawn(async move {
        connection.handle_events().await?;

        Ok(())
    });

    tasks.spawn(async {
        // wait for CTRL C to terminate the node execution
        ctrl_c().await?;

        Ok(())
    });

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(res) => {
                res?;
                tasks.abort_all();
            }
            Err(err) if err.is_cancelled() => {}
            Err(err) => {
                return Err(err.into());
            }
        };
    }

    client.disconnect().await?;

    Ok(())
}
