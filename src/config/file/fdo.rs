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

//! Options to setup FIDO Device Onboarding

use serde::{Deserialize, Serialize};
use url::Url;

use crate::config::Override;

/// Configures the FDO Onboarding protocol
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename = "astarte")]
pub struct FdoConfig {
    /// Flag to enable the FDO protocol
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Manufacturing URL for Device Initialization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manufacturing_url: Option<Url>,
}

impl Override for FdoConfig {
    fn merge(&mut self, overrides: Self) {
        let Self {
            enabled,
            manufacturing_url,
        } = overrides;

        self.enabled.merge(enabled);
        self.manufacturing_url.merge(manufacturing_url);
    }
}

// TODO add insta tests
