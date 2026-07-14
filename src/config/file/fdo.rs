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
use crate::error::ConfigError;

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

impl FdoConfig {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self.enabled.is_none_or(|enabled| !enabled) {
            return Ok(());
        }

        if self.manufacturing_url.is_none() {
            return Err(ConfigError::MissingField("manufacturing_url"));
        }

        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use crate::tests::with_settings;

    use super::*;

    #[test]
    fn fdo_config_roundtrip() {
        let fdo = FdoConfig {
            enabled: Some(true),
            manufacturing_url: Some("https://example.com".parse().unwrap()),
        };

        let toml = toml::to_string_pretty(&fdo).unwrap();

        let res: FdoConfig = toml::from_str(&toml).unwrap();

        pretty_assertions::assert_eq!(res, fdo);

        with_settings!({
            insta::assert_snapshot!(toml);
        });
    }

    #[rstest::rstest]
    #[case(FdoConfig {
        enabled: Some(true),
        manufacturing_url: Some("https://example.com".parse().unwrap()),
    })]
    #[case(FdoConfig {
        enabled: Some(false),
        manufacturing_url: Some("https://example.com".parse().unwrap()),
    })]
    #[case(FdoConfig {
        enabled: None,
        manufacturing_url: Some("https://example.com".parse().unwrap()),
    })]
    #[case(FdoConfig {
        enabled: Some(false),
        manufacturing_url: None,
    })]
    #[case(FdoConfig {
        enabled: None,
        manufacturing_url: None,
    })]
    fn should_validate_ok(#[case] case: FdoConfig) {
        case.validate().unwrap();
    }

    #[rstest::rstest]
    #[case(FdoConfig {
        enabled: Some(true),
        manufacturing_url: None,
    })]
    fn should_validate_invalid(#[case] case: FdoConfig) {
        case.validate().unwrap_err();
    }
}
