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

use std::{net::SocketAddr, time::Duration};

use astarte_device_sdk::{EventLoop, client::ClientDisconnect};
use astarte_message_hub::{
    AstarteMessageHub,
    astarte::handler::init_pub_sub,
    cache::Introspection,
    config::{MessageHubOptions, MessageHubOptionsBuilder},
};
use astarte_message_hub_proto::message_hub_server::MessageHubServer;
use eyre::{OptionExt, WrapErr, eyre};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub(crate) struct MessageHubTasks {
    tasks: JoinSet<eyre::Result<()>>,
    options: MessageHubOptions,
    cancel: CancellationToken,
}

impl MessageHubTasks {
    const TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

    pub(crate) async fn create<'a>(
        options: MessageHubOptionsBuilder<'a>,
    ) -> eyre::Result<Option<Self>> {
        let mut tasks = JoinSet::new();
        let cancel = CancellationToken::new();

        // wait for a shutdown signal and cancel other tasks
        tasks.spawn({
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

        let Some(options) = options.build(&mut tasks, cancel.child_token()).await? else {
            Self::join_tasks(tasks, cancel).await?;

            return Ok(None);
        };

        Ok(Some(Self {
            tasks,
            options,
            cancel,
        }))
    }

    pub(crate) async fn run(mut self) -> eyre::Result<()> {
        // Initialize an Astarte device
        let (client, connection) = self.options.create_connection().await?;

        let (publisher, mut subscriber) = init_pub_sub(client.clone());

        let address =
            SocketAddr::from((self.options.grpc_socket_host, self.options.grpc_socket_port));

        info!("starting MessageHub on http://{address}");

        // disconnect from astarte
        self.tasks.spawn({
            let cancel = self.cancel.child_token();
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
        self.tasks.spawn({
            use futures::FutureExt;

            use crate::events::check_cert_expiry;

            let client = client.clone();
            let cancel = self.cancel.child_token();

            cancel
                .run_until_cancelled_owned(async move { check_cert_expiry(client).await })
                .map(|r| r.unwrap_or(Ok(())))
        });

        // Event loop for the astarte device sdk
        // NOTE the disconnect called on the client exits the handle_events() event loop
        self.tasks.spawn(async move {
            connection
                .handle_events()
                .await
                .wrap_err("Astarte disconnected")
        });

        // Forward the astarte events to the subscribers
        // NOTE the cancellation token is used to stop the task in the subscriber
        self.tasks.spawn({
            let cancel = self.cancel.child_token();

            async move {
                subscriber
                    .forward_events(cancel)
                    .await
                    .wrap_err("subscriber disconnected")?;

                drop(subscriber);

                Ok(())
            }
        });

        let introspection = Introspection::create(self.options.introspection_cache).await?;

        // Handles the Message Hub gRPC server
        self.tasks.spawn({
            let cancel = self.cancel.child_token();

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

        Self::join_tasks(self.tasks, self.cancel).await
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
        let deadline = tokio::time::Instant::now()
            .checked_add(Self::TASK_SHUTDOWN_TIMEOUT)
            .ok_or_eyre("incorrect instant now")?;

        while let Some(join_res) = tokio::time::timeout_at(deadline, tasks.join_next())
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
