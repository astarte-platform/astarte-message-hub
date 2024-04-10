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

use std::{net::Ipv6Addr, path::Path, sync::Arc, time::Duration};

use astarte_device_sdk::{
    builder::DeviceBuilder, prelude::*, store::SqliteStore, transport::mqtt::MqttConfig,
};
use astarte_message_hub::{astarte::handler::init_pub_sub, AstarteMessageHub};
use astarte_message_hub_proto::message_hub_server::MessageHubServer;
use eyre::{Context, OptionExt};
use futures::{future::BoxFuture, FutureExt};
use tokio::{
    sync::Barrier,
    task::{AbortHandle, JoinSet},
};
use tower::{Layer, Service, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing::{instrument, trace};

use crate::{utils::read_env, GRPC_PORT};

pub struct MsgHub {
    server: AbortHandle,
    event_loop: AbortHandle,
    forwarder: AbortHandle,
}

impl MsgHub {
    pub fn close(self) {
        self.forwarder.abort();
        self.event_loop.abort();
        self.server.abort();
    }
}

#[instrument(skip_all)]
pub async fn init_message_hub(
    path: &Path,
    barrier: Arc<Barrier>,
    tasks: &mut JoinSet<eyre::Result<()>>,
) -> eyre::Result<MsgHub> {
    let realm = read_env("E2E_REALM")?;
    let device_id = read_env("E2E_DEVICE_ID")?;
    let credentials_secret = read_env("E2E_CREDENTIAL_SECRET")?;
    let pairing_url = read_env("E2E_PAIRING_URL")?;

    let mut mqtt_config =
        MqttConfig::with_credential_secret(realm, device_id, credentials_secret, pairing_url);

    if read_env("E2E_IGNORE_SSL").is_ok() {
        mqtt_config.ignore_ssl_errors();
    }

    let interfaces = path.join("interfaces");

    let path = path.to_str().ok_or_eyre("invalid_path")?;

    let uri = format!("sqlite://{path}/store.db");
    let store = SqliteStore::from_uri(&uri).await?;

    let (client, mut connection) = DeviceBuilder::new()
        .store(store)
        .connect(mqtt_config)
        .await?
        .build();

    let (publisher, mut subscriber) = init_pub_sub(client);

    let message_hub = AstarteMessageHub::new(publisher, interfaces);

    let server = tasks.spawn(async move {
        let layer = ServiceBuilder::new()
            .layer(BarrierLayer::new(barrier))
            .layer(TraceLayer::new_for_grpc())
            .into_inner();

        tonic::transport::Server::builder()
            .layer(layer)
            .trace_fn(|_| tracing::debug_span!("message_hub"))
            .timeout(Duration::from_secs(10))
            .layer(message_hub.make_interceptor_layer())
            .add_service(MessageHubServer::new(message_hub))
            .serve((Ipv6Addr::LOCALHOST, GRPC_PORT).into())
            .await
            .map_err(Into::into)
    });

    // Event loop for the astarte device sdk
    let event_loop = tasks.spawn(async move {
        connection
            .handle_events()
            .await
            .wrap_err("disconnected from astarte")
    });

    // Forward the astarte events to the subscribers
    let forwarder = tasks.spawn(async move {
        subscriber
            .forward_events()
            .await
            .wrap_err("subscriber disconnected")
    });

    Ok(MsgHub {
        server,
        event_loop,
        forwarder,
    })
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
        BarrierService {
            inner,
            barrier: self.barrier.clone(),
        }
    }
}

/// Wait for the client to send a message and sync after the response is completed.
#[derive(Debug, Clone)]
struct BarrierService<S> {
    barrier: Arc<Barrier>,
    inner: S,
}

impl<S, R> Service<R> for BarrierService<S>
where
    S: Service<R> + Clone + Send + 'static,
    S::Future: Send,
    S::Response: Send,
    S::Error: Send,
    R: Send + 'static,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: R) -> Self::Future {
        let service = self.clone();
        let mut service = std::mem::replace(self, service);

        async move {
            let res = service.inner.call(req).await;

            trace!("inner call resolved");

            service.barrier.wait().await;

            trace!("synced with client");

            res
        }
        .boxed()
    }
}
