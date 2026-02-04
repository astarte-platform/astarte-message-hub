// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::ops::ControlFlow;
use std::{net::SocketAddr, time::Duration};

use astarte_device_sdk::{EventLoop, client::ClientDisconnect};
use astarte_message_hub_proto::message_hub_server::MessageHubServer;
use clap::error::Result;
use eyre::{ContextCompat, OptionExt, WrapErr, eyre};
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace, warn};

use astarte_message_hub::AstarteMessageHub;
use astarte_message_hub::astarte::handler::init_pub_sub;
use astarte_message_hub::cache::Introspection;
use astarte_message_hub::config::ConfigBuilder;
use astarte_message_hub::config::dynamic::listen_dynamic_config;
use astarte_message_hub::config::loader::{ConfigEntry, ConfigRepository};
use astarte_message_hub::store::StoreDir;

pub(crate) struct MessageHubTasks {
    tasks: JoinSet<eyre::Result<()>>,
    config: ConfigRepository,
    store_dir: StoreDir,
    config_rx: Option<mpsc::Receiver<ConfigEntry>>,
}

impl MessageHubTasks {
    const TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

    pub(crate) async fn create(builder: ConfigBuilder) -> eyre::Result<Self> {
        let tasks = JoinSet::new();

        let (config, store_dir) = builder.build().await?;

        Ok(Self {
            tasks,
            config,
            store_dir,
            config_rx: None,
        })
    }

    pub(crate) async fn run(&mut self) -> eyre::Result<ControlFlow<()>> {
        // Cancel token for the run.
        let cancel = CancellationToken::new();

        // wait for a shutdown signal and cancel other tasks
        self.tasks.spawn({
            let cancel = cancel.clone();

            async move {
                info!("waiting for shutdown");

                if cancel.run_until_cancelled(shutdown()?).await.is_some() {
                    info!("cancelling tasks");

                    cancel.cancel();
                };

                Ok(())
            }
        });

        if self.wait_for_dynamic(&cancel).await?.is_break() {
            return Ok(ControlFlow::Break(()));
        }

        let options = self.config.try_into_options(&self.store_dir).await?;

        // Initialize an Astarte device
        let (client, connection) = options.create_connection(&self.store_dir).await?;

        let (publisher, mut subscriber) = init_pub_sub(client.clone());

        let address = SocketAddr::from((options.grpc_socket_host, options.grpc_socket_port));

        // disconnect from astarte
        self.tasks.spawn({
            let cancel = cancel.child_token();
            let mut client = client.clone();

            async move {
                cancel.cancelled().await;

                client.disconnect().await?;

                info!("Astarte client disconnected");

                Ok(())
            }
        });

        // check the astarte check expiry
        #[cfg(feature = "security-events")]
        {
            use astarte_message_hub::events::check_cert_expiry;

            let client = client.clone();
            let cancel = cancel.child_token();

            self.tasks.spawn(async move {
                cancel
                    .run_until_cancelled_owned(check_cert_expiry(client))
                    .await
                    .unwrap_or(Ok(()))
            });
        }

        // Event loop for the astarte device sdk
        self.tasks.spawn({
            let cancel = cancel.child_token();

            async move {
                // NOTE: this is not optimal since the task is aborted, but we need to pass the
                //       cancellation token to the Astarte device Sdk to be cancel safe.
                let Some(res) = cancel.run_until_cancelled(connection.handle_events()).await else {
                    return Ok(());
                };

                res.wrap_err("Astarte disconnected")
            }
        });

        // Forward the astarte events to the subscribers
        // NOTE the cancellation token is used to stop the task in the subscriber
        self.tasks.spawn({
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

        let introspection = Introspection::new(self.store_dir.clone());

        // Handles the Message Hub gRPC server
        self.tasks.spawn({
            let cancel = cancel.child_token();

            async move {
                // Create a new message hub service
                let message_hub = AstarteMessageHub::new(publisher, introspection);

                info!("Starting the server on grpc://{address}");

                // Run the proto-buff server
                tonic::transport::Server::builder()
                    .layer(tower_http::trace::TraceLayer::new_for_grpc())
                    .layer(message_hub.make_interceptor_layer())
                    .add_service(MessageHubServer::new(message_hub))
                    .serve_with_shutdown(address, cancel.cancelled())
                    .await
                    .wrap_err("couldn't start the server")
            }
        });

        self.join_tasks(cancel).await
    }

    async fn wait_for_dynamic(
        &mut self,
        cancel: &CancellationToken,
    ) -> eyre::Result<ControlFlow<()>> {
        let Some(dynamic) = self.config.get_dynamic_config() else {
            return Ok(ControlFlow::Continue(()));
        };

        if !dynamic.is_enabled() {
            return Ok(ControlFlow::Continue(()));
        }

        let rx = listen_dynamic_config(
            &mut self.tasks,
            cancel.child_token(),
            dynamic,
            self.store_dir.clone(),
        )
        .await?;
        let rx = self.config_rx.insert(rx);

        // Wait for fist dynamic config
        if !self.config.has_dynamic() {
            // Wait until cancel
            let Some(res) = cancel.run_until_cancelled(rx.recv()).await else {
                return Ok(ControlFlow::Break(()));
            };

            let entry = res.wrap_err("couldn't receive dynamic config")?;

            self.config.add_dynamic(entry);
        }

        Ok(ControlFlow::Continue(()))
    }

    async fn join_tasks(&mut self, cancel: CancellationToken) -> eyre::Result<ControlFlow<()>> {
        while let Some(event) = self.select_next(&cancel).await {
            match event {
                Event::Task(Ok(())) => {
                    trace!("task joined");
                }
                Event::Config(Some(entry)) => {
                    info!("dynamic config received");

                    self.config.add_dynamic(*entry);

                    cancel.cancel();

                    self.wait_exit().await?;

                    return Ok(ControlFlow::Continue(()));
                }
                Event::Config(None) => {
                    warn!("dynamic config closed");
                }
                Event::Task(Err(error)) => {
                    error!(%error, "task exited with error");

                    cancel.cancel();

                    self.wait_exit().await?;

                    return Err(eyre::eyre!("task exited with error"));
                }

                Event::JoinErr(error) => {
                    error!(%error, "couldn't join a task");

                    cancel.cancel();

                    self.wait_exit().await?;

                    return Err(eyre::eyre!("task exited with error"));
                }
                Event::Cancel => {
                    info!("cancel called");

                    self.wait_exit().await?;

                    return Ok(ControlFlow::Break(()));
                }
            }
        }

        info!("all task exited");

        Ok(ControlFlow::Break(()))
    }

    /// Gets the next event
    async fn select_next(&mut self, cancel: &CancellationToken) -> Option<Event> {
        // Futures here are cancel safe
        if let Some(config_rx) = &mut self.config_rx {
            select! {
                opt_res = self.tasks.join_next() => {
                    opt_res.map(Event::from)
                },
                () = cancel.cancelled() => {
                    Some(Event::Cancel)
                },
                opt_config = config_rx.recv() => {
                    Some(Event::Config(opt_config.map(Box::new)))
                },
            }
        } else {
            select! {
                opt_res = self.tasks.join_next() => {
                    opt_res.map(Event::from)
                },
                _ = cancel.cancelled() => {
                    Some(Event::Cancel)
                },
            }
        }
    }

    async fn wait_exit(&mut self) -> eyre::Result<()> {
        info!(
            timeout_secs = Self::TASK_SHUTDOWN_TIMEOUT.as_secs(),
            "waiting for tasks to join cleanly ",
        );

        // join all tasks with a global timeout
        let deadline = tokio::time::Instant::now()
            .checked_add(Self::TASK_SHUTDOWN_TIMEOUT)
            .ok_or_eyre("incorrect instant now")?;

        let mut task_errors = false;

        while let Some(join_res) = tokio::time::timeout_at(deadline, self.tasks.join_next())
            .await
            .wrap_err("timeout reached while waiting for tasks to shutdown cleanly")?
        {
            match join_res {
                Ok(Ok(())) => {
                    trace!("task joined");
                }
                Ok(Err(error)) => {
                    error!(%error, "task exited with an error");

                    task_errors = true;
                }
                Err(error) => {
                    error!(%error, "couldn't join a task");

                    task_errors = true;
                }
            }
        }

        if !task_errors {
            Ok(())
        } else {
            Err(eyre!("one or more task failed"))
        }
    }
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

#[derive(Debug)]
enum Event {
    Cancel,
    Task(eyre::Result<()>),
    JoinErr(JoinError),
    Config(Option<Box<ConfigEntry>>),
}

impl From<Result<eyre::Result<()>, JoinError>> for Event {
    fn from(value: Result<eyre::Result<()>, JoinError>) -> Self {
        match value {
            Ok(res) => Event::Task(res),
            Err(err) => Event::JoinErr(err),
        }
    }
}
