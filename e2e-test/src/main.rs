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

use std::{env::VarError, net::Ipv6Addr, path::Path, sync::Arc};

use astarte_device_sdk::{
    builder::DeviceBuilder,
    prelude::*,
    store::{memory::MemoryStore, SqliteStore},
    transport::{
        grpc::{Grpc, GrpcConfig},
        mqtt::MqttConfig,
    },
    AstarteDeviceSdk, ClientDisconnect, EventReceiver,
};
use astarte_message_hub::{AstarteHandler, AstarteMessageHub};
use astarte_message_hub_proto::message_hub_server::MessageHubServer;
use eyre::OptionExt;
use futures::{future::BoxFuture, Future};
use interfaces::INTERFACES;
use tempfile::tempdir;
use tokio::{sync::Barrier, task::JoinHandle};
use tonic::body::BoxBody;
use tower::{Layer, Service};
use tracing::{debug, info, instrument, trace};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use utils::read_env;
use uuid::{uuid, Uuid};

pub mod interfaces;
pub mod utils;

const GRPC_PORT: u16 = 50051;
const UUID: Uuid = uuid!("acc78dae-194c-4942-8f33-9f719629e316");

fn env_filter() -> eyre::Result<EnvFilter> {
    let filter = std::env::var("RUST_LOG").or_else(|err| match err {
        VarError::NotPresent => Ok("e2e_test=debug,astarte_message_hub=debug".to_string()),
        err @ VarError::NotUnicode(_) => Err(err),
    })?;

    let env_fitler = EnvFilter::try_new(filter)?;

    Ok(env_fitler)
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

    // Barrier to synchronize the MessageHub server and the sender
    //
    // ```
    // 1 -> MessageSent -> start wait --------------------------|-> check astarte API
    // 2 -> |-> Tower middleware (e2e)                 |-> wait |
    //      |-> Send to handler -> Result from handler |
    //
    // ```
    let barrier = Arc::new(Barrier::new(2));

    let msghub = init_message_hub(dir.path(), &barrier).await?;
    let node = init_node(barrier).await?;

    node.close().await?;
    msghub.close().await?;

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
            debug!("waiting for call");
            // Pre call
            barrier.wait().await;

            debug!("call received");
            let res = inner.call(req).await;

            info!("sending response");
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

struct MsgHub {
    handle: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl MsgHub {
    async fn close(self) -> eyre::Result<()> {
        self.handle.abort();
        let join = self.handle.await;

        match join {
            Ok(res) => res.map_err(Into::into),
            Err(err) if err.is_cancelled() => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}

#[must_use]
async fn init_message_hub(path: &Path, barrier: &Arc<Barrier>) -> eyre::Result<MsgHub> {
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

    let handle = tokio::spawn(async {
        tonic::transport::Server::builder()
            .layer(BarrierLayer::new(barrier))
            .add_service(MessageHubServer::new(message_hub))
            .serve((Ipv6Addr::LOCALHOST, GRPC_PORT).into())
            .await
    });

    Ok(MsgHub { handle })
}

type DeviceSdk = AstarteDeviceSdk<MemoryStore, Grpc>;

struct Node {
    barrier: Arc<Barrier>,
    device: DeviceSdk,
    rx_events: EventReceiver,
}

impl Node {
    async fn close(self) -> eyre::Result<()> {
        self.take_sync(|device| device.disconnect()).await?;

        Ok(())
    }

    async fn sync<F, T, O>(&mut self, mut f: F) -> eyre::Result<O>
    where
        F: FnMut(&mut DeviceSdk) -> T,
        T: Future<Output = O> + Send + 'static,
        O: Send + 'static,
    {
        let future = (f)(&mut self.device);

        Self::sync_future(&self.barrier, future).await
    }

    async fn take_sync<F, T, O>(self, mut f: F) -> eyre::Result<O>
    where
        F: FnMut(DeviceSdk) -> T,
        T: Future<Output = O> + Send + 'static,
        O: Send + 'static,
    {
        let future = (f)(self.device);

        Self::sync_future(&self.barrier, future).await
    }

    async fn sync_future<F>(barrier: &Barrier, f: F) -> eyre::Result<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = tokio::spawn(f);

        trace!("syncronize with the server");
        barrier.wait().await;

        trace!("Waiting for the response");
        barrier.wait().await;

        trace!("Response received");
        let out = handle.await?;

        Ok(out)
    }
}

#[instrument(skip_all)]
#[must_use]
async fn init_node(barrier: Arc<Barrier>) -> eyre::Result<Node> {
    let grpc = GrpcConfig::new(UUID, format!("http://localhost:{GRPC_PORT}"));

    let mut builder = DeviceBuilder::new().store(MemoryStore::new());

    for iface in INTERFACES {
        builder = builder.interface(iface)?
    }

    debug!("Start connect to the server");
    let handle = tokio::spawn(async move { builder.connect(grpc).await });

    barrier.wait().await;
    debug!("Wait for server response");

    barrier.wait().await;
    info!("Connected to the message hub");

    let (device, rx_events) = handle.await??.build();

    Ok(Node {
        device,
        rx_events,
        barrier,
    })
}
