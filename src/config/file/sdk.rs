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

//! Configuration for the Astarte Device Sdk

use std::num::NonZeroUsize;

use astarte_device_sdk::store::sqlite::Size;
use serde::{Deserialize, Serialize};

use crate::config::Override;

/// Options to pass to the [Astarte device SDK](`astarte_device_sdk`).
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename = "astarte")]
pub struct DeviceSdkOptions {
    /// Keep alive interval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_alive_secs: Option<u64>,
    /// Connection timeout.
    ///
    /// Should be less than the keep alive interval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    /// Whether to ignore SSL errors when connecting to Astarte.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignore_ssl: Option<bool>,
    /// Astarte device SDK volatile store options.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volatile: Option<DeviceSdkVolatileOptions>,
    /// Astarte device SDK persistent store options.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store: Option<DeviceSdkStoreOptions>,
}

impl Override for DeviceSdkOptions {
    fn merge(&mut self, overrides: Self) {
        let Self {
            keep_alive_secs,
            timeout_secs,
            ignore_ssl,
            volatile,
            store,
        } = overrides;

        self.keep_alive_secs.merge(keep_alive_secs);
        self.timeout_secs.merge(timeout_secs);
        self.ignore_ssl.merge(ignore_ssl);
        self.volatile.merge(volatile);
        self.store.merge(store);
    }
}

/// Options to pass to the [Astarte device SDK](`astarte_device_sdk`).
/// These configure the files stored on the disk by the library.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DeviceSdkStoreOptions {
    /// Maximum allowed size of the astarte database
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_db_size: Option<Size>,
    /// Maximum allowed size of the astarte database journal
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_db_journal_size: Option<Size>,
    /// Maximum number of items stored in the database backed retention
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retention_items: Option<NonZeroUsize>,
}

impl Override for DeviceSdkStoreOptions {
    fn merge(&mut self, overrides: Self) {
        let Self {
            max_db_size,
            max_db_journal_size,
            max_retention_items,
        } = overrides;

        self.max_db_size.merge(max_db_size);
        self.max_db_journal_size.merge(max_db_journal_size);
        self.max_retention_items.merge(max_retention_items);
    }
}

/// Options to pass to the [Astarte device SDK](`astarte_device_sdk`).
/// These configure the volatile message storage.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DeviceSdkVolatileOptions {
    /// Maximum number of items stored in the volatile retention
    /// Once the limit is reached old items will be removed first in a fifo manner
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retention_items: Option<NonZeroUsize>,
}

impl Override for DeviceSdkVolatileOptions {
    fn merge(&mut self, overrides: Self) {
        let Self {
            max_retention_items,
        } = overrides;

        self.max_retention_items.merge(max_retention_items);
    }
}

#[cfg(test)]
mod tests {
    // use pretty_assertions::assert_eq;

    // use crate::config::file::Config;

    // use super::*;

    // TODO: cargo insta
    // #[test]
    // fn test_read_options_from_toml_store() {
    //     const TOML_FILE: &str = r#"
    //         [astarte]
    //         ignore_ssl = true

    //         [astarte.store]
    //         max_db_size = { value = 5, unit = "mib" }
    //         max_db_journal_size = { value = 5, unit = "kb" }
    //         max_retention_items = 10
    //     "#;

    //     let res = toml::from_str::<Config>(TOML_FILE);
    //     let options = res.expect("Parsing of TOML file failed");
    //     assert!(options.astarte.ignore_ssl);
    //     assert_eq!(
    //         options.astarte.store.max_db_size,
    //         Some(Size::MiB(5.try_into().unwrap()))
    //     );
    //     assert_eq!(
    //         options.astarte.store.max_db_journal_size,
    //         Some(Size::Kb(5.try_into().unwrap()))
    //     );
    //     assert_eq!(
    //         options.astarte.store.max_retention_items,
    //         Some(10.try_into().unwrap())
    //     )
    // }

    // #[test]
    // fn test_read_options_from_toml_size_error_case_sensitive() {
    //     const TOML_FILE: &str = r#"
    //         [astarte.store]
    //         max_db_size = { value = 10, unit = "MiB" }
    //     "#;

    //     let res = toml::from_str::<Config>(TOML_FILE);
    //     assert!(res.is_err());
    // }

    // #[test]
    // fn test_read_options_from_toml_size_error_tb() {
    //     const TOML_FILE: &str = r#"
    //         [astarte]
    //         ignore_ssl = true
    //         [astarte.store]
    //         max_db_size = { value = 10, unit = "mib" }
    //         max_db_journal_size = { value = 5, unit = "tb" }
    //     "#;

    //     let res = toml::from_str::<Config>(TOML_FILE);
    //     assert!(res.is_err());
    // }

    // #[test]
    // fn test_read_options_from_toml_store_error_0() {
    //     const TOML_FILE: &str = r#"
    //         [astarte.store]
    //         max_retention_items = 0
    //     "#;

    //     let res = toml::from_str::<Config>(TOML_FILE);
    //     assert!(res.is_err());
    // }

    // #[test]
    // fn test_read_options_from_toml_volatile_error_0() {
    //     const TOML_FILE: &str = r#"
    //         [astarte.volatile]
    //         max_retention_items = 0
    //     "#;

    //     let res = toml::from_str::<Config>(TOML_FILE);
    //     assert!(res.is_err());
    // }
}
