/*
 * This file is part of Astarte.
 *
 * Copyright 2026 SECO Mind Srl
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

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use astarte_device_sdk::{
    DeviceClient, DeviceConnection, EventLoop,
    builder::DeviceBuilder,
    client::ClientDisconnect,
    store::SqliteStore,
    transport::mqtt::{Mqtt, MqttConfig},
};
use astarte_message_hub::{
    AstarteMessageHub,
    astarte::handler::{DevicePublisher, DeviceSubscriber, init_pub_sub},
    config::MessageHubOptions,
};
use astarte_message_hub_proto::message_hub_server::MessageHubServer;
use eyre::{WrapErr, eyre};
use log::{debug, error, info};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub(crate) struct MessageHubTasks {
    client: DeviceClient<Mqtt<SqliteStore>>,
    connection: DeviceConnection<Mqtt<SqliteStore>>,
    publisher: DevicePublisher,
    subscriber: DeviceSubscriber,
    interfaces_dir: PathBuf,
    address: SocketAddr,
}

impl MessageHubTasks {
    const TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

    pub(crate) async fn with_options(
        options: MessageHubOptions,
        interfaces_dir: PathBuf,
    ) -> eyre::Result<Self> {
        // Initialize an Astarte device
        let (client, connection) = initialize_astarte_device_sdk(&options, &interfaces_dir).await?;
        info!("astarte device initialized");

        let (publisher, subscriber) = init_pub_sub(client.clone());

        let address = (options.grpc_socket_host, options.grpc_socket_port).into();

        Ok(Self {
            client,
            connection,
            publisher,
            subscriber,
            interfaces_dir,
            address,
        })
    }

    pub(crate) async fn run(self) -> eyre::Result<()> {
        let Self {
            client,
            connection,
            publisher,
            mut subscriber,
            interfaces_dir,
            address,
        } = self;

        let cancel = CancellationToken::new();
        let mut tasks = JoinSet::new();

        // check the astarte check expiry
        #[cfg(feature = "security-events")]
        tasks.spawn({
            use futures::FutureExt;

            use crate::events::check_cert_expiry;

            let client = client.clone();
            let cancel = cancel.child_token();

            cancel
                .run_until_cancelled_owned(async move { check_cert_expiry(client).await })
                .map(|r| r.unwrap_or(Ok(())))
        });

        // Event loop for the astarte device sdk
        // NOTE the disconnect called on the client exits the handle_events() event loop
        tasks.spawn(async move {
            connection
                .handle_events()
                .await
                .wrap_err("Astarte disconnected")
        });

        // Forward the astarte events to the subscribers
        // NOTE the cancellation token is used to stop the task in the subscriber
        tasks.spawn({
            let cancel = cancel.child_token();

            async move {
                subscriber
                    .forward_events(cancel)
                    .await
                    .wrap_err("subscriber disconnected")?;

                drop(subscriber);

                Ok(())
            }
        });

        // Handles the Message Hub gRPC server
        tasks.spawn({
            let cancel = cancel.child_token();

            async move {
                // Create a new message hub service
                let message_hub = AstarteMessageHub::new(publisher, interfaces_dir);

                info!("Starting the server on grpc://{address}");

                // Run the proto-buff server
                tonic::transport::Server::builder()
                    .layer(message_hub.make_interceptor_layer())
                    .add_service(MessageHubServer::new(message_hub))
                    .serve_with_shutdown(address, cancel.cancelled())
                    .await
                    .wrap_err("couldn't start the server")
            }
        });

        // wait for a shutdown signal and cancel other tasks
        tasks.spawn({
            let cancel = cancel.clone();
            let mut client = client.clone();

            async move {
                info!("waiting for shutdown");

                cancel
                    .run_until_cancelled(shutdown()?)
                    .await
                    .inspect(|_| cancel.cancel());

                client.disconnect().await?;

                Ok(())
            }
        });

        Self::join_tasks(tasks, cancel).await
    }

    async fn join_tasks(
        mut tasks: JoinSet<eyre::Result<()>>,
        cancel: CancellationToken,
    ) -> eyre::Result<()> {
        let mut task_errors = false;

        info!("running tasks until shutdown is called");

        // run until we have to shutdown the tasks or until the first task exits with an error
        while let Some(join_res) = cancel
            .run_until_cancelled(tasks.join_next())
            .await
            .flatten()
        {
            match join_res {
                Ok(Err(e)) => {
                    error!("error in first tasks joined: {e}");
                    task_errors = true;
                    break;
                }
                Err(e) => {
                    error!("error while joining first task: {e}");
                    task_errors = true;
                    break;
                }
                _ => {}
            }
        }

        // shutdown everything
        cancel.cancel();

        info!(
            "waiting for tasks to join cleanly with a '{}' second timeout",
            Self::TASK_SHUTDOWN_TIMEOUT.as_secs()
        );

        // join all tasks with a global timeout
        let deadline = Instant::now()
            .checked_add(Self::TASK_SHUTDOWN_TIMEOUT)
            .unwrap_or(Instant::now());

        while let Some(join_res) = tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            tasks.join_next(),
        )
        .await
        .wrap_err("timeout reached while waiting for tasks to shutdown cleanly")?
        {
            match join_res {
                Ok(Err(e)) => {
                    error!("error in one of the tasks: {e}");
                    task_errors = true;
                }
                Err(e) => {
                    error!("error while joining the tasks: {e}");
                    task_errors = true;
                }
                _ => {}
            }
        }

        if !task_errors {
            Ok(())
        } else {
            Err(eyre!("one or more task failed"))
        }
    }
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

    if let Some(keep_alive) = msg_hub_opts.astarte.keep_alive_secs {
        mqtt_config.keepalive(Duration::from_secs(keep_alive));
    }

    let mut builder = DeviceBuilder::new().writable_dir(&msg_hub_opts.store_directory)?;

    if let Some(timeout) = msg_hub_opts.astarte.timeout_secs {
        builder = builder.connection_timeout(Duration::from_secs(timeout))
    }

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
