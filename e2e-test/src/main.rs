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

use std::{env::VarError, net::Ipv6Addr, path::Path, time::Duration};

use astarte_device_sdk::{
    builder::DeviceBuilder,
    prelude::*,
    store::{memory::MemoryStore, SqliteStore},
    transport::{
        grpc::{Grpc, GrpcConfig},
        mqtt::MqttConfig,
    },
    AstarteDeviceSdk, EventReceiver,
};
use astarte_message_hub::{AstarteHandler, AstarteMessageHub};
use astarte_message_hub_proto::message_hub_server::MessageHubServer;
use eyre::{ensure, eyre, Context, OptionExt};
use interfaces::{ENDPOINTS, INTERFACES};
use tempfile::tempdir;
use tokio::task::{AbortHandle, JoinSet};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing::{debug, info, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use utils::read_env;
use uuid::{uuid, Uuid};

use crate::{
    api::Api,
    interfaces::{DeviceAggregate, DeviceDatastream, INTERFACE_NAMES},
};

pub mod api;
pub mod interfaces;
pub mod utils;

const GRPC_PORT: u16 = 50051;
const UUID: Uuid = uuid!("acc78dae-194c-4942-8f33-9f719629e316");

fn env_filter() -> eyre::Result<EnvFilter> {
    let filter = std::env::var("RUST_LOG").or_else(|err| match err {
        VarError::NotPresent => Ok(
            "e2e_test=trace,astarte_message_hub=debug,astarte_device_sdk=debug,tower_http=debug"
                .to_string(),
        ),
        err @ VarError::NotUnicode(_) => Err(err),
    })?;

    let env_filter = EnvFilter::try_new(filter)?;

    Ok(env_filter)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    stable_eyre::install()?;

    let filter = env_filter()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(filter)
        .try_init()?;

    let dir = tempdir()?;

    let api = Api::try_from_env()?;

    let mut tasks = JoinSet::new();

    let msghub = init_message_hub(dir.path(), &mut tasks).await?;
    let node = init_node(&mut tasks).await?;

    tasks.spawn(async move { e2e_test(api, msghub, node).await });

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(res) => {
                res.wrap_err("task failed")?;
            }
            Err(err) if err.is_cancelled() => {}
            Err(err) => {
                return Err(err).wrap_err("couldn't join task");
            }
        }
    }

    Ok(())
}

#[instrument(skip_all)]
async fn e2e_test(api: Api, msghub: MsgHub, node: Node) -> eyre::Result<()> {
    let count = 0;

    loop {
        let mut interfaces = api.interfaces().await?;
        interfaces.sort_unstable();

        if interfaces == INTERFACE_NAMES {
            break;
        }

        // Re-try three times
        ensure!(
            interfaces == INTERFACE_NAMES || count < 3,
            "to many attempts"
        );
    }

    debug!("sending DeviceAggregate");

    node.device
        .send_object(
            DeviceAggregate::name(),
            DeviceAggregate::path(),
            DeviceAggregate::default(),
        )
        .await?;

    let data: DeviceAggregate = api
        .aggregate_value(DeviceAggregate::name(), DeviceAggregate::path())
        .await?
        .pop()
        .ok_or_else(|| eyre!("missing data from publish"))?;

    assert_eq!(data, DeviceAggregate::default());

    debug!("sending DeviceDatastream");
    let mut data = DeviceDatastream::default().astarte_aggregate()?;
    for &endpoint in ENDPOINTS {
        let value = data.remove(endpoint).ok_or_eyre("endpoint not found")?;

        node.device
            .send(DeviceDatastream::name(), &format!("/{endpoint}"), value)
            .await?;
    }

    debug!("checking result");
    api.check_individual(DeviceDatastream::name(), &data)
        .await?;

    node.close().await?;
    msghub.close();

    Ok(())
}

struct MsgHub {
    handle: AbortHandle,
}

impl MsgHub {
    fn close(self) {
        self.handle.abort();
    }
}

#[must_use]
async fn init_message_hub(
    path: &Path,
    tasks: &mut JoinSet<eyre::Result<()>>,
) -> eyre::Result<MsgHub> {
    let realm = read_env("E2E_REALM")?;
    let device_id = read_env("E2E_DEVICE_ID")?;
    let credentials_secret = read_env("E2E_CREDENTIAL_SECRET")?;
    let pairing_url = read_env("E2E_PAIRING_URL")?;

    let mut mqtt_config = MqttConfig::new(realm, device_id, credentials_secret, pairing_url);

    if read_env("E2E_IGNORE_SSL").is_ok() {
        mqtt_config.ignore_ssl_errors();
    }

    let path = path.to_str().ok_or_eyre("invalid_path")?;

    let uri = format!("sqlite://{path}/store.db");
    let store = SqliteStore::new(&uri).await?;

    let (device, rx_events) = DeviceBuilder::new()
        .store(store)
        .connect(mqtt_config)
        .await?
        .build();

    let handler = AstarteHandler::new(device, rx_events);

    let message_hub = AstarteMessageHub::new(handler);

    let handle = tasks.spawn(async {
        let layer = ServiceBuilder::new()
            .timeout(Duration::from_secs(10))
            .layer(TraceLayer::new_for_grpc());

        tonic::transport::Server::builder()
            .trace_fn(|_| tracing::debug_span!("message_hub"))
            .layer(layer)
            .add_service(MessageHubServer::new(message_hub))
            .serve((Ipv6Addr::LOCALHOST, GRPC_PORT).into())
            .await
            .map_err(Into::into)
    });

    Ok(MsgHub { handle })
}

type DeviceSdk = AstarteDeviceSdk<MemoryStore, Grpc>;

#[derive(Debug)]
struct Node {
    handle: AbortHandle,
    device: DeviceSdk,
    _rx_events: EventReceiver,
}

impl Node {
    async fn close(self) -> eyre::Result<()> {
        self.handle.abort();

        Ok(())
    }
}

#[instrument(skip_all)]
#[must_use]
async fn init_node(tasks: &mut JoinSet<eyre::Result<()>>) -> eyre::Result<Node> {
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
        _rx_events: rx_events,
        handle,
    })
}
