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

use std::{net::IpAddr, path::PathBuf};

use astarte_message_hub::config::{Config, DeviceSdkOptions};
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
    /// Path to the Astarte Message Hub configuration file.
    #[arg(short, long)]
    pub config: Option<PathBuf>,
    /// Options for the gRPC connection.
    #[command(flatten)]
    pub grpc: GrpcOptions,
    /// Options for the HTTP connection.
    #[command(flatten)]
    pub http: HttpOptions,
    /// Options for the device configuration and credentials.
    #[command(flatten)]
    pub device: DeviceOptions,
    /// Options to pass to the [Astarte device SDK](`astarte_device_sdk`).
    #[command(flatten)]
    pub astarte: AstarteOptions,
}

impl Cli {
    pub fn merge(self, config: &mut Config) {
        self.grpc.merge(config);
        self.device.merge(config);
        self.astarte.merge(&mut config.astarte);
    }
}

/// Options for the HTTP connection.
///
/// It's used for the dynamic configuration.
#[derive(Args, Debug)]
#[command(next_help_heading = "HTTP Options")]
pub struct HttpOptions {
    /// Address the HTTP connection will bind to (e.g. 127.0.0.1).
    #[arg(long, env = "MSGHUB_HTTP_HOST")]
    pub http_host: Option<IpAddr>,
    /// Port the HTTP connection will bind to (e.g 40041)
    #[arg(long, env = "MSGHUB_HTTP_PORT")]
    pub http_port: Option<u16>,
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

impl GrpcOptions {
    fn merge(self, config: &mut Config) {
        config.grpc_socket_host.merge(self.host);
        config.grpc_socket_port.merge(self.port);
    }
}

/// Options for the device configuration and credentials.
#[derive(Args, Debug)]
#[command(next_help_heading = "Device Options")]
pub struct DeviceOptions {
    /// The Astarte realm the device belongs to.
    #[arg(long, env = "MSGHUB_REALM")]
    pub realm: Option<String>,
    /// A unique ID for the device.
    #[arg(long, env = "MSGHUB_DEVICE_ID")]
    pub device_id: Option<String>,
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

impl DeviceOptions {
    pub fn merge(self, config: &mut Config) {
        config.realm.merge(self.realm);
        config.device_id.merge(self.device_id);
        config.credentials_secret.merge(self.credentials_secret);
        config.pairing_token.merge(self.pairing_token);
        config.interfaces_directory.merge(self.interfaces_dir);
        config.store_directory.merge(self.store_dir);
    }
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

impl AstarteOptions {
    pub fn merge(self, config: &mut DeviceSdkOptions) {
        config.keep_alive_secs.merge(self.keep_alive_secs);
        config.timeout_secs.merge(self.timeout_secs);
        config.ignore_ssl = self.ignore_ssl.unwrap_or(config.ignore_ssl);
    }
}

trait OverrideOption {
    fn merge(&mut self, value: Self);
}

impl<T> OverrideOption for Option<T> {
    fn merge(&mut self, value: Self) {
        if value.is_some() {
            *self = value;
        }
    }
}
