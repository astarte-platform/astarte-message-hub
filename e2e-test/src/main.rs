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

use std::{net::Ipv6Addr, path::Path, sync::Arc};

use astarte_device_sdk::{
    builder::DeviceBuilder,
    prelude::*,
    store::{memory::MemoryStore, SqliteStore},
    transport::{grpc::GrpcConfig, mqtt::MqttConfig},
};
use astarte_message_hub::{AstarteHandler, AstarteMessageHub};
use astarte_message_hub_proto::message_hub_server::MessageHubServer;
use eyre::OptionExt;
use futures::future::BoxFuture;
use interfaces::INTERFACES;
use tempfile::tempdir;
use tokio::sync::Barrier;
use tonic::body::BoxBody;
use tower::{Layer, Service};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use utils::read_env;
use uuid::{uuid, Uuid};

pub mod interfaces;
pub mod utils;

const GRPC_PORT: u16 = 50051;
const UUID: Uuid = uuid!("acc78dae-194c-4942-8f33-9f719629e316");

#[tokio::main]
async fn main() -> eyre::Result<()> {
    stable_eyre::install()?;

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::DEBUG.into())
                .from_env_lossy(),
        )
        .try_init()?;

    let dir = tempdir()?;

    // Barrier to synchronize the MessageHub server and the sender
    //
    // ```
    // 1 -> MessageSent -> start wait --------------------------|-> check astarte API
    // 2 -> |-> Tower middleware (e2e)                 |-> wait |
    //      |-> Send to handler -> Result from handler |
    //
    // ```
    let barrier = Arc::new(Barrier::new(2));

    init_message_hub(dir.path(), &barrier).await?;
    init_node().await?;

    Ok(())
}

/// We don't want to clone the barrier
#[derive(Debug, Clone)]
struct BarrierService<S> {
    barrier: Arc<Barrier>,
    inner: S,
}

impl<S> Service<hyper::Request<hyper::Body>> for BarrierService<S>
where
    S: Service<hyper::Request<hyper::Body>, Response = hyper::Response<BoxBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<hyper::Body>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let barrier = Arc::clone(&self.barrier);

        Box::pin(async move {
            // Pre call
            barrier.wait().await;

            let res = inner.call(req).await;

            // Post call
            barrier.wait().await;

            res
        })
    }
}

#[derive(Debug, Clone)]
struct BarrierLayer {
    barrier: Arc<Barrier>,
}

impl BarrierLayer {
    fn new(barrier: Arc<Barrier>) -> Self {
        Self { barrier }
    }
}

impl<S> Layer<S> for BarrierLayer {
    type Service = BarrierService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        let barrier = Arc::clone(&self.barrier);

        BarrierService { barrier, inner }
    }
}

async fn init_message_hub(path: &Path, barrier: &Arc<Barrier>) -> eyre::Result<()> {
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

    let barrier = Arc::clone(barrier);

    let _server_handle = tokio::spawn(async {
        tonic::transport::Server::builder()
            .layer(BarrierLayer::new(barrier))
            .add_service(MessageHubServer::new(message_hub))
            .serve((Ipv6Addr::LOCALHOST, GRPC_PORT).into())
            .await
    });

    Ok(())
}

async fn init_node() -> eyre::Result<()> {
    let grpc = GrpcConfig::new(UUID, format!("http://localhost:{GRPC_PORT}"));

    let mut builder = DeviceBuilder::new();

    for iface in INTERFACES {
        builder = builder.interface(iface)?
    }

    let (_device, _rx_events) = builder
        .store(MemoryStore::new())
        .connect(grpc)
        .await?
        .build();

    Ok(())
}
