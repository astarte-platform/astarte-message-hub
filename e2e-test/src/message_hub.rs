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

use std::{net::Ipv6Addr, path::Path, time::Duration};

use astarte_device_sdk::{
    builder::DeviceBuilder, prelude::*, store::SqliteStore, transport::mqtt::MqttConfig,
};
use astarte_message_hub::{init_pub_sub, AstarteMessageHub};
use astarte_message_hub_proto::message_hub_server::MessageHubServer;
use eyre::{Context, OptionExt};
use tokio::task::{AbortHandle, JoinSet};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing::instrument;

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

    let (mut device, rx_events) = DeviceBuilder::new()
        .store(store)
        .connect(mqtt_config)
        .await?
        .build();

    let (publisher, mut subscriber) = init_pub_sub(device.clone(), rx_events);

    let message_hub = AstarteMessageHub::new(publisher);

    let server = tasks.spawn(async {
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

    // Event loop for the astarte device sdk
    let event_loop = tasks.spawn(async move {
        device
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
