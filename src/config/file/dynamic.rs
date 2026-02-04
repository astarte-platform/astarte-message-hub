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

//! Options to enable and customize the dynamic configuration.

use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};

use crate::config::{DEFAULT_GRPC_CONFIG_PORT, DEFAULT_HOST, DEFAULT_HTTP_PORT, Override};

/// Configures the HTTP server for the dynamic configuration
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DynamicConfig {
    /// Configure the HTTP server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http: Option<Http>,
    /// Configure the gRPC server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc: Option<Grpc>,
}

impl DynamicConfig {
    pub(crate) fn http_address(&self) -> SocketAddr {
        match &self.http {
            Some(http) => SocketAddr::from((
                http.host.unwrap_or(DEFAULT_HOST),
                http.port.unwrap_or(DEFAULT_HTTP_PORT),
            )),
            None => SocketAddr::from((DEFAULT_HOST, DEFAULT_HTTP_PORT)),
        }
    }

    pub(crate) fn grpc_address(&self) -> SocketAddr {
        match &self.grpc {
            Some(grpc) => SocketAddr::from((
                grpc.host.unwrap_or(DEFAULT_HOST),
                grpc.port.unwrap_or(DEFAULT_GRPC_CONFIG_PORT),
            )),
            None => SocketAddr::from((DEFAULT_HOST, DEFAULT_GRPC_CONFIG_PORT)),
        }
    }

    /// Check if any server is enabled
    pub fn is_enabled(&self) -> bool {
        self.is_http_enabled() || self.is_grpc_enabled()
    }

    /// Check if the HTTP server is enabled
    pub fn is_http_enabled(&self) -> bool {
        self.http
            .as_ref()
            .and_then(|http| http.enabled)
            .unwrap_or_default()
    }

    /// Check if the gRPC server is enabled
    pub fn is_grpc_enabled(&self) -> bool {
        self.grpc
            .as_ref()
            .and_then(|grpc| grpc.enabled)
            .unwrap_or_default()
    }
}

impl Override for DynamicConfig {
    fn merge(&mut self, overrides: Self) {
        let Self { http, grpc } = overrides;

        self.http.merge(http);
        self.grpc.merge(grpc);
    }
}

/// HTTP server config
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Http {
    /// Whether to enable the HTTP dynamic config
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// IP address to listen on for the HTTP server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<IpAddr>,
    /// Port to listen on for the HTTP server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

/// gRPC server config
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Grpc {
    /// Whether to enable the gRPC dynamic config
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// IP address to listen on for the HTTP server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<IpAddr>,
    /// Port to listen on for the HTTP server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

impl Override for Grpc {
    fn merge(&mut self, overrides: Self) {
        let Self {
            enabled,
            host,
            port,
        } = overrides;

        self.enabled.merge(enabled);
        self.host.merge(host);
        self.port.merge(port);
    }
}

// TODO add insta tests
