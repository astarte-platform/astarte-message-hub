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

//! Manages the store directory.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use eyre::Context;
use tracing::{debug, error, info};

use crate::config::file::{CONFIG_FILE_NAME, Config, LEGACY_CONFIG_FILE_NAME};

/// Manages the files in the store directory
#[derive(Debug, Clone)]
pub struct StoreDir {
    pub(crate) store_dir: Arc<Path>,
}

impl StoreDir {
    /// Create the store
    pub async fn create(store_dir: PathBuf) -> eyre::Result<Self> {
        let this = StoreDir {
            store_dir: Arc::from(store_dir),
        };

        this.move_legacy_files().await?;

        Ok(this)
    }

    async fn move_legacy_files(&self) -> eyre::Result<()> {
        let old_config = self.store_dir.join(LEGACY_CONFIG_FILE_NAME);

        if tokio::fs::try_exists(&old_config).await? {
            debug!("found old dynamic configuraiotn file");

            let mut store_config = self.get_dynamic_config_dir().await?;

            store_config.push(CONFIG_FILE_NAME);

            tokio::fs::rename(&old_config, &store_config)
                .await
                .wrap_err("couldn't move old configuration file")?;

            info!("legacy configuration file moved")
        }

        Ok(())
    }

    /// Returns the store directory creating it if it doesn't exists
    pub async fn get_store_dir(&self) -> Result<&Path, eyre::Error> {
        tokio::fs::create_dir_all(&self.store_dir)
            .await
            .wrap_err("couldn't create stored configuration directory")?;

        Ok(&self.store_dir)
    }

    /// Returns the stored configs directory creating it if it doesn't exists
    pub async fn get_dynamic_config_dir(&self) -> Result<PathBuf, eyre::Error> {
        let store_config = self.store_dir.join("config");

        tokio::fs::create_dir_all(&store_config)
            .await
            .wrap_err("couldn't create stored configuration directory")?;

        Ok(store_config)
    }

    /// Returns the stored configs directory creating it if it doesn't exists
    pub async fn get_interfaces_cache_dir(&self) -> Option<PathBuf> {
        let interfaces = self.store_dir.join("interfaces");

        if let Err(error) = tokio::fs::create_dir_all(&interfaces).await {
            error!(%error, "couldn't create interfaces cache directory");
        }

        Some(interfaces)
    }

    /// Stores ad config file with the given name.
    ///
    /// The name must not have an extension.
    pub async fn store_config(&self, config: &Config, file_name_no_ext: &str) {
        debug_assert!(!file_name_no_ext.ends_with(".json"));
        debug_assert!(!file_name_no_ext.ends_with(".toml"));

        let mut config_file = match self.get_dynamic_config_dir().await {
            Ok(dir) => dir,
            Err(error) => {
                error!(%error, "couldn't get stored config directory");

                return;
            }
        };

        config_file.push(format!("{file_name_no_ext}.toml"));

        let contents = match toml::to_string(&config) {
            Ok(contents) => contents,
            Err(error) => {
                error!(%error, "couldn't serialize configuration file");

                return;
            }
        };

        if let Err(error) = tokio::fs::write(&config_file, contents).await {
            error!(%error, "couln't write configuration file")
        }
    }

    pub(crate) fn dynamic_config_file(&self, file_name_no_ext: &str) -> PathBuf {
        let mut config_file = self.store_dir.join("config");

        config_file.push(format!("{file_name_no_ext}.toml"));

        config_file
    }
}
