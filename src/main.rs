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

use std::io::{IsTerminal, stdout};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use astarte_message_hub::config::MessageHubOptions;
use clap::Parser;
use eyre::eyre;
use tracing::{debug, warn};

use crate::cli::Cli;
use crate::tasks::MessageHubTasks;

mod cli;
#[cfg(feature = "security-events")]
mod events;
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

    let mut builder = MessageHubOptions::builder();

    if let Some(config_file) = &args.config {
        builder.set_custom_config(config_file);
    }

    if let Some(config_dir) = &args.config_dir {
        builder.set_config_dir(config_dir);
    }

    if let Some(storage_dir) = &args.device.store_dir {
        builder.set_storage_dir(storage_dir);
    }

    let Some(tasks) = MessageHubTasks::create(builder).await? else {
        debug!("cancelled before start");

        return Ok(());
    };

    tasks.run().await?;

    Ok(())
}

fn init_tracing() -> eyre::Result<()> {
    let default_layer = tracing_subscriber::fmt::layer()
        .with_ansi(stdout().is_terminal())
        .with_filter(
            EnvFilter::builder()
                .with_default_directive("astarte_message_hub=info".parse()?)
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
