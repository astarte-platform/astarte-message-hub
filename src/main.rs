// This file is part of Astarte.
//
// Copyright 2022, 2026 SECO Mind Srl
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

//! A central service that runs on (Linux) devices for collecting and delivering messages from N
//! apps using 1 MQTT connection to Astarte.

#![warn(missing_docs)]

use astarte_message_hub::config::{Config, DEFAULT_HOST, DEFAULT_HTTP_PORT};
use eyre::{Context, OptionExt};
use rustls::ClientConfig;
use rustls_platform_verifier::BuilderVerifierExt;
use std::io::{IsTerminal, stdout};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use astarte_message_hub::config::MessageHubOptions;
use clap::Parser;
use eyre::eyre;
use log::{info, warn};

use crate::cli::Cli;
use crate::tasks::MessageHubTasks;

mod cli;
#[cfg(feature = "security-events")]
mod events;
#[cfg(feature = "fdo")]
mod fdo;
mod tasks;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;

    init_tracing()?;

    // Set default crypto provider
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| eyre!("failed to install default crypto provider"))?;

    let args = Cli::parse();

    if args.toml.is_some() {
        warn!(
            "DEPRECATED: the '-t/--toml' option is deprecated in favour of the '-c/--config' flag"
        )
    }

    let tls = ClientConfig::builder()
        .with_platform_verifier()?
        .with_no_client_auth();

    let options = get_config_options(args, tls).await?;

    // Directory to store the Nodes introspection
    let interfaces_dir = options.store_directory.join("interfaces");
    if !interfaces_dir.exists() {
        tokio::fs::create_dir_all(&interfaces_dir)
            .await
            .wrap_err("couldn't create interface directory")?;
    }

    let tasks = MessageHubTasks::with_options(options, interfaces_dir).await?;

    tasks.run().await?;

    Ok(())
}

async fn get_config_options(
    args: Cli,
    tls: rustls::ClientConfig,
) -> eyre::Result<MessageHubOptions> {
    let store_directory = args.device.store_dir.as_deref();
    let custom_config = args.config.as_deref().or(args.toml.as_deref());

    let mut config = match Config::find_config(custom_config, store_directory).await? {
        Some(config) => config,
        None => {
            let store_directory = args.device.store_dir.as_deref().ok_or_eyre(
                "no configuration file specified and store directory missing to start dynamic configuration",
            )?;

            info!("No configuration found, starting dynamic configuration");

            let http = (
                args.http.http_host.unwrap_or(DEFAULT_HOST),
                args.http.http_port.unwrap_or(DEFAULT_HTTP_PORT),
            )
                .into();

            let grpc = (
                args.grpc.host.unwrap_or(DEFAULT_HOST),
                args.grpc.port.unwrap_or(DEFAULT_HTTP_PORT),
            )
                .into();

            info!("listening on http://${http} and grpc://${grpc}");

            Config::listen_dynamic_config(store_directory, http, grpc).await?
        }
    };

    args.merge(&mut config);

    if config.device_id.is_none() {
        info!("device id not configured, reading hardware id");

        // retrieve the device id
        config.device_id_from_hardware_id().await?;
    }

    if let Some(fdo) = &config.fdo {
        let store = config
            .store_directory
            .as_ref()
            .ok_or_eyre("missing store directory for FDO protocol")?
            .join("fdo");

        let amod = self::fdo::fdo(
            store,
            tls,
            fdo.manufactoring_url.parse()?,
            "fdo-test-message-hub",
            // TODO: this should be the device id but is unique to prevent errors
            // &msg_hub_opts.device_id,
            &uuid::Uuid::new_v4().to_string(),
        )
        .await?;

        return config.with_fdo(amod).wrap_err("invalid FDO configuration");
    }
    // Read the credentials secret, the store defaults to the current directory
    config.read_credential_secret().await?;

    Ok(config.try_into()?)
}

fn init_tracing() -> eyre::Result<()> {
    let default_layer = tracing_subscriber::fmt::layer()
        .with_ansi(stdout().is_terminal())
        .with_filter(
            EnvFilter::builder()
                .with_default_directive("INFO".parse()?)
                .from_env_lossy(),
        );

    let subscribers = tracing_subscriber::registry().with(default_layer);

    #[cfg(feature = "security-events")]
    let subscribers = subscribers.with(
        tracing_journald::layer()?
            .with_syslog_identifier("astarte_sdk_security_events".to_string())
            .with_filter(
                tracing_subscriber::filter::Targets::new()
                    .with_target("security-event", tracing::Level::TRACE),
            ),
    );

    subscribers.try_init()?;

    Ok(())
}
