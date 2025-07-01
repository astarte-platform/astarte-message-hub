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

use std::str::FromStr;
use std::{env::VarError, future::Future, sync::Arc};

use astarte_device_sdk::astarte_interfaces::Interface;
use astarte_device_sdk::transport::grpc::Grpc;
use astarte_device_sdk::{prelude::*, DeviceClient, Value};
use eyre::{bail, ensure, eyre, Context, OptionExt};
use interfaces::ServerAggregate;
use itertools::Itertools;
use tempfile::tempdir;
use tokio::{sync::Barrier, task::JoinSet};
use tracing::{debug, error, instrument, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use uuid::{uuid, Uuid};

use crate::interfaces::ADDITIONAL_INTERFACES;
use crate::{
    api::Api,
    device_sdk::{init_node, Node},
    interfaces::{
        AdditionalDeviceDatastream, DeviceAggregate, DeviceDatastream, DeviceProperty,
        ServerDatastream, ServerProperty, ADDITIONAL_INTERFACE_NAMES, ENDPOINTS, INTERFACE_NAMES,
    },
    message_hub::{init_message_hub, MsgHub},
};

pub mod api;
pub mod device_sdk;
pub mod interfaces;
pub mod message_hub;
pub mod utils;

pub const GRPC_PORT: u16 = 50051;
pub const UUID: Uuid = uuid!("acc78dae-194c-4942-8f33-9f719629e316");

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
    color_eyre::install()?;

    let filter = env_filter()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(filter)
        .try_init()?;

    let dir = tempdir()?;

    let api = Api::try_from_env()?;

    let mut tasks = JoinSet::new();

    // Barrier to sync client and server
    let barrier = Arc::new(Barrier::new(2));

    let msghub = init_message_hub(dir.path(), barrier.clone(), &mut tasks).await?;
    let node = init_node(&barrier, &mut tasks).await?;

    tasks.spawn(async move { e2e_test(api, msghub, node, barrier).await });

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

/// Retry the future multiple times
async fn retry<F, T, U>(times: usize, mut f: F) -> eyre::Result<U>
where
    F: FnMut() -> T,
    T: Future<Output = eyre::Result<U>>,
{
    for i in 1..=times {
        match (f)().await {
            Ok(o) => return Ok(o),
            Err(err) => {
                error!("failed retry {i} for: {err}");

                tokio::task::yield_now().await
            }
        }
    }

    bail!("to many attempts")
}

#[instrument(skip_all)]
async fn e2e_test(
    api: Api,
    msghub: MsgHub,
    mut node: Node,
    barrier: Arc<Barrier>,
) -> eyre::Result<()> {
    // Check that the attach worked by checking the message hub interfaces
    retry(20, || async {
        let mut interfaces = api.interfaces().await?;
        interfaces.sort_unstable();

        debug!(?interfaces);

        ensure!(
            interfaces == INTERFACE_NAMES,
            "different number of interfaces"
        );

        Ok(())
    })
    .await?;

    // Send the device data
    send_device_data(&mut node, &api, &barrier).await?;

    // Receive the server data
    receive_server_data(&mut node, &api, &barrier).await?;

    // Extend the node interfaces
    let additional_interfaces = additional_interfaces()?;
    extend_node_interfaces(&mut node, &api, &barrier, additional_interfaces.clone()).await?;

    // Remove some node interfaces
    let to_remove = ADDITIONAL_INTERFACE_NAMES
        .iter()
        .map(|i| i.to_string())
        .collect_vec();
    remove_node_interfaces(&mut node, &api, &barrier, to_remove).await?;

    // Disconnect the message hub and cleanup
    node.close().await?;
    msghub.close();

    Ok(())
}

async fn send<F, O>(node: &Node, barrier: &Barrier, f: F) -> eyre::Result<()>
where
    F: FnOnce(DeviceClient<Grpc>) -> O,
    O: Future<Output = eyre::Result<()>> + Send + 'static,
{
    let client = node.client.clone();
    let handle = tokio::spawn((f)(client));

    // wait for send call
    barrier.wait().await;

    handle.await?
}

async fn send_prop<F, O>(node: &Node, barrier: &Barrier, f: F) -> eyre::Result<()>
where
    F: FnOnce(DeviceClient<Grpc>) -> O,
    O: Future<Output = eyre::Result<()>> + Send + 'static,
{
    let client = node.client.clone();
    let handle = tokio::spawn((f)(client));

    // wait for inner try_load_prop call
    barrier.wait().await;
    // wait for send call
    barrier.wait().await;

    handle.await?
}

#[instrument(skip_all)]
async fn send_device_data(node: &mut Node, api: &Api, barrier: &Barrier) -> eyre::Result<()> {
    debug!("sending DeviceAggregate");
    send(node, barrier, |mut client| async move {
        client
            .send_object_with_timestamp(
                DeviceAggregate::name(),
                DeviceAggregate::path(),
                DeviceAggregate::default().into_object()?,
                chrono::Utc::now(),
            )
            .await?;

        Ok(())
    })
    .await?;

    retry(10, || async move {
        let data: DeviceAggregate = api
            .aggregate_value(DeviceAggregate::name(), DeviceAggregate::path())
            .await?
            .pop()
            .ok_or_else(|| eyre!("missing data from publish"))?;

        assert_eq!(data, DeviceAggregate::default());

        Ok(())
    })
    .await?;

    debug!("sending DeviceDatastream");
    let mut data = DeviceDatastream::default().into_object()?;
    for &endpoint in ENDPOINTS {
        let value = data.remove(endpoint).ok_or_eyre("endpoint not found")?;
        send(node, barrier, |mut client| async move {
            client
                .send_individual_with_timestamp(
                    DeviceDatastream::name(),
                    &format!("/{endpoint}"),
                    value,
                    chrono::Utc::now(),
                )
                .await?;

            Ok(())
        })
        .await?;
    }

    retry(10, || {
        let value = data.clone();

        async move {
            debug!("checking result");

            api.check_individual(DeviceDatastream::name(), &value)
                .await?;

            Ok(())
        }
    })
    .await?;

    debug!("sending DeviceProperty");
    let mut data = DeviceProperty::default().into_object()?;
    for &endpoint in ENDPOINTS {
        let value = data.remove(endpoint).ok_or_eyre("endpoint not found")?;

        send_prop(node, barrier, |mut client| async move {
            client
                .set_property(DeviceProperty::name(), &format!("/{endpoint}"), value)
                .await?;

            Ok(())
        })
        .await?;
    }

    retry(10, || {
        let value = data.clone();

        async move {
            debug!("checking result");
            api.check_individual(DeviceProperty::name(), &value).await?;

            Ok(())
        }
    })
    .await?;

    debug!("retrieving property values given the property name and the specific endpoint");
    let mut data = DeviceProperty::default().into_object()?;
    for &endpoint in ENDPOINTS {
        let client = node.client.clone();
        let handle = tokio::spawn(async move {
            client
                .property(DeviceProperty::name(), &format!("/{endpoint}"))
                .await?
                .ok_or_eyre(format!("property endpoint {endpoint} not stored"))
        });
        barrier.wait().await;
        let stored_prop = handle.await??;

        let exp_value = data.remove(endpoint).ok_or_eyre("endpoint not found")?;

        assert_eq!(exp_value, stored_prop);
    }

    debug!("retrieving property values associated to all endpoints given the property name");
    let mut data = DeviceProperty::default().into_object()?;
    let client = node.client.clone();
    let handle = tokio::spawn(async move { client.interface_props(DeviceProperty::name()).await });
    barrier.wait().await;
    let stored_prop = handle.await??;
    for prop in stored_prop {
        let exp_value = data
            .remove(prop.path.trim_start_matches('/'))
            .ok_or_eyre(format!("endpoint not found for prop {prop:#?}"))?;
        assert_eq!(exp_value, prop.value);
    }

    debug!("retrieving all device property values");
    let mut data = DeviceProperty::default().into_object()?;
    let client = node.client.clone();
    let handle = tokio::spawn(async move { client.device_props().await });
    barrier.wait().await;
    let stored_prop = handle.await??;
    for prop in stored_prop {
        let exp_value = data
            .remove(prop.path.trim_start_matches('/'))
            .ok_or_eyre("endpoint not found")?
            .clone();
        assert_eq!(exp_value, prop.value);
    }

    debug!("unsetting DeviceProperty");
    let data = DeviceProperty::default().into_object()?;
    for &endpoint in ENDPOINTS {
        ensure!(data.get(endpoint).is_some(), "endpoint not found");

        send(node, barrier, |mut client| async move {
            client
                .unset_property(DeviceProperty::name(), &format!("/{endpoint}"))
                .await?;

            Ok(())
        })
        .await?;
    }

    retry(10, || async move {
        let data = api.property(DeviceProperty::name()).await?;
        ensure!(data.is_empty(), "property not unsetted {data:?}");

        Ok(())
    })
    .await?;

    Ok(())
}

#[instrument(skip_all)]
async fn receive_server_data(node: &mut Node, api: &Api, barrier: &Barrier) -> eyre::Result<()> {
    debug!("checking ServerAggregate");
    api.send_interface(
        ServerAggregate::name(),
        ServerAggregate::path(),
        ServerAggregate::default(),
    )
    .await?;

    let event = node.recv().await?;

    assert_eq!(event.interface, ServerAggregate::name());
    assert_eq!(event.path, ServerAggregate::path());

    let (data, _timestamp) = event.data.as_object().ok_or_eyre("not an object")?;
    assert_eq!(*data, ServerAggregate::default().into_object()?);

    debug!("checking ServerDatastream");
    let data = ServerDatastream::default().into_object()?;
    for (k, v) in data.iter() {
        api.send_individual(ServerDatastream::name(), k, v).await?;

        let event = node.recv().await?;

        assert_eq!(event.interface, ServerDatastream::name());
        assert_eq!(event.path, format!("/{k}"));

        let (data, _timestamp) = event.data.as_individual().ok_or_eyre("not an object")?;
        assert_eq!(data, v);
    }

    debug!("checking ServerProperty");
    let data = ServerProperty::default().into_object()?;
    for (k, v) in data.iter() {
        api.send_individual(ServerProperty::name(), k, v).await?;

        let event = node.recv().await?;

        assert_eq!(event.interface, ServerProperty::name());
        assert_eq!(event.path, format!("/{k}"));

        let data = event
            .data
            .as_property()
            .ok_or_eyre("not an object")?
            .as_ref()
            .unwrap();

        assert_eq!(data, v);
    }

    debug!("retrieving all server property values");
    let mut data = ServerProperty::default().into_object()?;
    let client = node.client.clone();
    let handle = tokio::spawn(async move { client.server_props().await });
    barrier.wait().await;
    let stored_prop = handle.await??;
    for prop in stored_prop {
        let exp_value = data
            .remove(prop.path.trim_start_matches('/'))
            .ok_or_eyre("endpoint not found")?
            .clone();
        assert_eq!(exp_value, prop.value);
    }

    debug!("checking unset for ServerProperty");
    let data = ServerProperty::default().into_object()?;
    for (k, _) in data.iter() {
        api.unset(ServerProperty::name(), k).await?;

        let event = node.recv().await?;

        assert_eq!(event.interface, ServerProperty::name());
        assert_eq!(event.path, format!("/{k}"));

        assert_eq!(event.data, Value::Property(None));
    }

    Ok(())
}

#[instrument(skip_all)]
fn additional_interfaces() -> eyre::Result<Vec<Interface>> {
    let to_add = ADDITIONAL_INTERFACES
        .iter()
        .copied()
        .map(Interface::from_str)
        .try_collect()?;

    Ok(to_add)
}

#[instrument(skip_all)]
async fn extend_node_interfaces(
    node: &mut Node,
    api: &Api,
    barrier: &Barrier,
    to_add: Vec<Interface>,
) -> eyre::Result<()> {
    debug!("extending interfaces with AdditionalDeviceDatastream");
    let mut client = node.client.clone();
    let handle = tokio::spawn(async move { client.extend_interfaces(to_add).await });

    barrier.wait().await;

    let mut added = handle.await??;
    added.sort();

    assert_eq!(added, ADDITIONAL_INTERFACE_NAMES);

    let mut exp_interfaces = INTERFACE_NAMES
        .iter()
        .merge(ADDITIONAL_INTERFACE_NAMES)
        .copied()
        .collect_vec();
    exp_interfaces.sort();

    // Check that adding the interfaces worked by checking the message hub interfaces
    retry(20, || async {
        let mut interfaces = api.interfaces().await?;
        interfaces.sort_unstable();

        debug!(?interfaces);

        ensure!(
            interfaces == exp_interfaces,
            "different number of interfaces"
        );

        Ok(())
    })
    .await?;

    debug!("sending AdditionalDeviceDatastream");
    let mut data = AdditionalDeviceDatastream::default().into_object()?;
    for &endpoint in ENDPOINTS {
        let value = data.remove(endpoint).ok_or_eyre("endpoint not found")?;
        send(node, barrier, |mut client| async move {
            client
                .send_individual_with_timestamp(
                    AdditionalDeviceDatastream::name(),
                    &format!("/{endpoint}"),
                    value,
                    chrono::Utc::now(),
                )
                .await?;

            Ok(())
        })
        .await?;
    }

    retry(10, || {
        let value = data.clone();

        async move {
            debug!("checking result");

            api.check_individual(AdditionalDeviceDatastream::name(), &value)
                .await?;

            Ok(())
        }
    })
    .await?;

    Ok(())
}

#[instrument(skip_all)]
async fn remove_node_interfaces(
    node: &mut Node,
    api: &Api,
    barrier: &Barrier,
    mut to_remove: Vec<String>,
) -> eyre::Result<()> {
    debug!("removing interfaces {to_remove:?}");
    let mut client = node.client.clone();
    let to_remove_cl = to_remove.clone();
    let handle = tokio::spawn(async move { client.remove_interfaces(to_remove_cl).await });

    barrier.wait().await;

    let mut removed = handle.await??;
    removed.sort();
    to_remove.sort();

    assert_eq!(removed, to_remove);

    // Check that removing the interfaces worked by checking the message hub interfaces
    retry(20, || async {
        let mut interfaces = api.interfaces().await?;
        interfaces.sort_unstable();

        debug!(?interfaces);

        ensure!(
            interfaces == INTERFACE_NAMES,
            "different number of interfaces"
        );

        Ok(())
    })
    .await?;

    Ok(())
}
