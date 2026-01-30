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
use std::net::IpAddr;

use crate::config::Override;

/// Configures the HTTP server for the dynamic configuration
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DynamicConfig {
    /// Whether to enable the dynamic configuration, default to false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// IP address to listen on for the HTTP server.
    pub http_host: Option<IpAddr>,
    /// Port to listen on for the HTTP server.
    pub http_port: Option<u16>,
    /// IP address to listen on for the gRPC server.
    pub grpc_host: Option<IpAddr>,
    /// Port to listen on for the gRPC server.
    pub grpc_port: Option<u16>,
}

impl Override for DynamicConfig {
    fn merge(&mut self, overrides: Self) {
        let Self {
            enabled,
            http_host,
            http_port,
            grpc_host,
            grpc_port,
        } = overrides;

        self.enabled.merge(enabled);
        self.http_host.merge(http_host);
        self.http_port.merge(http_port);
        self.grpc_host.merge(grpc_host);
        self.grpc_port.merge(grpc_port);
    }
}

// TODO add insta tests
