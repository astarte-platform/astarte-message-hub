// This file is part of Astarte.
//
// Copyright 2024, 2026 SECO Mind Srl
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

use std::{net::IpAddr, path::PathBuf};

use astarte_message_hub::config::file::Config;
use astarte_message_hub::config::file::dynamic::DynamicConfig;
use astarte_message_hub::config::file::sdk::DeviceSdkOptions;
use clap::{Args, Parser};

/// A central service that runs on (Linux) devices for collecting and delivering messages from N
/// apps using 1 MQTT connection to Astarte.
///
/// # Options precedence
///
/// The flags will take precedence over the config, this lets us override the default values set by
/// the configuration.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    /// Path to a valid .toml file containing the message hub configuration.
    #[arg(short, long)]
    pub toml: Option<PathBuf>,
    /// Path to a custom Astarte Message Hub configuration file.
    #[arg(short, long)]
    pub config: Option<PathBuf>,
    /// Path to a custom the Astarte Message Hub configuration directory.
    #[arg(long)]
    pub config_dir: Option<PathBuf>,
    /// Options for the gRPC connection.
    #[command(flatten)]
    pub grpc: GrpcOptions,
    /// Options for the HTTP connection.
    #[command(flatten)]
    pub dynamic: DynamicOptions,
    /// Options for the device configuration and credentials.
    #[command(flatten)]
    pub device: DeviceOptions,
    /// Options to pass to the [Astarte device SDK](`astarte_device_sdk`).
    #[command(flatten)]
    pub astarte: AstarteOptions,
}

impl From<Cli> for Config {
    fn from(value: Cli) -> Self {
        let Cli {
            // The configs will be read afterwards
            toml: _,
            config: _,
            config_dir: _,
            grpc,
            device,
            astarte,
            dynamic,
        } = value;

        let DeviceOptions {
            realm,
            device_id,
            pairing_url,
            credentials_secret,
            pairing_token,
            interfaces_dir,
            store_dir,
        } = device;

        let GrpcOptions { host, port } = grpc;

        let dynamic_config = Option::<DynamicConfig>::from(dynamic).map(|mut c| {
            c.grpc_host = host;
            c.grpc_port = port;
            c
        });

        Config {
            realm,
            device_id,
            pairing_url,
            credentials_secret,
            pairing_token,
            interfaces_directory: interfaces_dir,
            astarte_ignore_ssl: None,
            grpc_socket_host: host,
            grpc_socket_port: port,
            store_directory: store_dir,
            dynamic_config,
            astarte: astarte.into(),
        }
    }
}

/// Options for the dynamic configuration.
///
/// Configures the HTTP and gRPC dynamic configuration services.
#[derive(Args, Debug)]
#[command(next_help_heading = "Dynamic Configuration Options")]
pub struct DynamicOptions {
    /// Enables the dynamic configuration.
    #[arg(long, env = "MSGHUB_DYNAMIC_CONFIG")]
    pub dynamic_config: Option<bool>,
    /// Address the HTTP connection will bind to (e.g. 127.0.0.1).
    #[arg(long, env = "MSGHUB_HTTP_HOST")]
    pub http_host: Option<IpAddr>,
    /// Port the HTTP connection will bind to (e.g 40041)
    #[arg(long, env = "MSGHUB_HTTP_PORT")]
    pub http_port: Option<u16>,
}

impl From<DynamicOptions> for Option<DynamicConfig> {
    fn from(value: DynamicOptions) -> Self {
        let DynamicOptions {
            dynamic_config,
            http_host,
            http_port,
        } = value;

        let has_value = dynamic_config.is_some() || http_host.is_some() || http_port.is_some();

        if has_value {
            Some(DynamicConfig {
                enabled: dynamic_config,
                http_host,
                http_port,
                grpc_host: None,
                grpc_port: None,
            })
        } else {
            None
        }
    }
}

/// Options for the gRPC connection.
#[derive(Args, Debug)]
#[command(next_help_heading = "gRPC Options")]
pub struct GrpcOptions {
    /// Address the gRPC connection will bind to (e.g. 127.0.0.1).
    #[arg(long, env = "MSGHUB_GRPC_HOST")]
    pub host: Option<IpAddr>,
    /// Port the gRPC connection will bind to (e.g 50051)
    #[arg(long, env = "MSGHUB_GRPC_PORT")]
    pub port: Option<u16>,
}

/// Options for the device configuration and credentials.
#[derive(Args, Debug, Default)]
#[command(next_help_heading = "Device Options")]
pub struct DeviceOptions {
    /// The Astarte realm the device belongs to.
    #[arg(long, env = "MSGHUB_REALM")]
    pub realm: Option<String>,
    /// A unique ID for the device.
    #[arg(long, env = "MSGHUB_DEVICE_ID")]
    pub device_id: Option<String>,
    /// Pairing URL of the Astarte instance.
    #[arg(long, env = "MSGHUB_PAIRING_URL")]
    pub pairing_url: Option<String>,
    /// The credentials secret used to authenticate with Astarte.
    #[arg(
        long,
        conflicts_with = "pairing_token",
        env = "MSGHUB_CREDENTIALS_SECRET"
    )]
    pub credentials_secret: Option<String>,
    /// Token used to register the device.
    #[arg(
        long,
        conflicts_with = "credentials_secret",
        env = "MSGHUB_PAIRING_TOKEN"
    )]
    pub pairing_token: Option<String>,
    /// Directory containing the Astarte interfaces.
    #[arg(short, long, env = "MSGHUB_INTERFACES_DIR")]
    pub interfaces_dir: Option<PathBuf>,
    /// Directory used by Astarte-Message-Hub to retain configuration and other persistent data.
    #[arg(short, long, env = "MSGHUB_STORE_DIR")]
    pub store_dir: Option<PathBuf>,
}

/// Options to pass to the [Astarte device SDK](`astarte_device_sdk`).
#[derive(Args, Debug)]
#[command(next_help_heading = "Astarte Options")]
pub struct AstarteOptions {
    /// Keep alive interval in seconds.
    #[arg(
        value_name = "SECONDS",
        long = "keep-alive",
        env = "MSGHUB_ASTARTE_KEEP_ALIVE"
    )]
    pub keep_alive_secs: Option<u64>,
    /// Connection timeout in seconds.
    ///
    /// Should be less than the keep alive interval.
    #[arg(
        long = "timeout",
        value_name = "SECONDS",
        env = "MSGHUB_ASTARTE_TIMEOUT"
    )]
    pub timeout_secs: Option<u64>,
    /// Whether to ignore SSL errors when connecting to Astarte.
    #[arg(long, env = "MSGHUB_ASTARTE_INSECURE")]
    pub ignore_ssl: Option<bool>,
}

impl From<AstarteOptions> for Option<DeviceSdkOptions> {
    fn from(value: AstarteOptions) -> Self {
        let AstarteOptions {
            keep_alive_secs,
            timeout_secs,
            ignore_ssl,
        } = value;

        let has_value = keep_alive_secs.is_some() || timeout_secs.is_some() || ignore_ssl.is_some();

        if !has_value {
            None
        } else {
            Some(DeviceSdkOptions {
                keep_alive_secs,
                timeout_secs,
                ignore_ssl,
                volatile: None,
                store: None,
            })
        }
    }
}
