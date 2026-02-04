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

//! Loads and stores the configuration files.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

use eyre::{Context, OptionExt};
use futures::TryStreamExt;
use tokio_stream::wrappers::ReadDirStream;
use tracing::{debug, info};

use crate::config::Override;
use crate::store::StoreDir;

use super::file::Config;
use super::file::dynamic::DynamicConfig;
use super::{CustomConfig, MessageHubOptions, hardware_id};

const CONFIG_FILE: &str = "config.toml";
const CONFIG_D: &str = "config.d";

/// Cache for the configuration files after being loaded.
#[derive(Debug, Default)]
pub struct ConfigRepository {
    /// Cli overrides
    overrides: Config,
    /// Config merging all the configuration files
    file_config: Config,
    /// Drop-in from the config.d directories
    config_d: BTreeSet<ConfigEntry>,
    /// Dynamic configurations
    dynamic: BTreeSet<ConfigEntry>,
}

impl ConfigRepository {
    pub(crate) fn with_overrides(overrides: Config) -> Self {
        Self {
            overrides,
            ..Default::default()
        }
    }

    /// Checks if there are any dynamic configs
    pub fn has_dynamic(&self) -> bool {
        !self.dynamic.is_empty()
    }

    /// Returns the store directory
    pub(crate) fn get_store_directory(&self) -> Option<&Path> {
        let mut store_dir = self.file_config.store_directory.as_deref();

        for entry in &self.config_d {
            store_dir.merge(entry.config.store_directory.as_deref())
        }

        store_dir.merge(self.overrides.store_directory.as_deref());

        store_dir
    }

    /// Returns the dynamic config
    pub fn get_dynamic_config(&self) -> Option<&DynamicConfig> {
        let mut dynamic_config = self.file_config.dynamic_config.as_ref();

        for entry in &self.config_d {
            dynamic_config.merge(entry.config.dynamic_config.as_ref())
        }

        dynamic_config.merge(self.overrides.dynamic_config.as_ref());

        dynamic_config
    }

    /// Reads the configurations files.
    ///
    /// The order of the configurations files is as follows:
    ///
    /// 1. System default configuration file (e.g. /etc/message-hub/config.toml)
    /// 2. User default configuration file (e.g. $HOME/.config/message-hub/config.toml)
    /// 3. All the drop-ins configuration from various locations sorted alphabetical with the
    ///    following stable order:
    ///    1. System directory: /etc/message-hub/config.d/*.toml
    ///    1. User directory:  $HOME/.config/message-hub/config.d/*.toml
    pub async fn read_configs(&mut self, custom: &Option<CustomConfig>) -> eyre::Result<()> {
        match custom {
            Some(CustomConfig::File(config_file)) => {
                debug!(path = %config_file.display(), "using custom config file");

                let custom = Config::read(config_file)
                    .await?
                    .ok_or_eyre("couldn't read custom config file")?;

                self.file_config = custom;
            }
            Some(CustomConfig::Dir(config_dir)) => {
                debug!(dir = %config_dir.display(), "using custom config directory");

                self.read_config_dir(config_dir).await?;
            }
            None => {
                // TODO: window support
                #[cfg(unix)]
                {
                    debug!("reading system config dir");

                    const SYSTEM_CONFIG: &str = "/etc/message-hub/";

                    self.read_config_dir(Path::new(SYSTEM_CONFIG)).await?;
                }

                if let Some(user_config_dir) = dirs::config_dir() {
                    debug!("reading user config dir");

                    // $HOME/.config/message-hub
                    let user_config_dir = user_config_dir.join("message-hub");

                    self.read_config_dir(&user_config_dir).await?;
                }
            }
        }

        Ok(())
    }

    /// Reads the configs in the dynamic configuration directory
    pub async fn read_dynamic(&mut self, store_dir: &StoreDir) -> eyre::Result<()> {
        let dir = store_dir.get_dynamic_config_dir().await?;

        Self::append_configs(&mut self.dynamic, &dir).await?;

        Ok(())
    }

    /// Appends the configuration files to the vector
    async fn append_configs(configs: &mut BTreeSet<ConfigEntry>, path: &Path) -> eyre::Result<()> {
        let dir = match tokio::fs::read_dir(path).await {
            Ok(dir) => dir,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(eyre::Report::new(err).wrap_err("couldn't read config.d directory"));
            }
        };

        ReadDirStream::new(dir)
            .map_err(|error| {
                eyre::Report::new(error).wrap_err("couldn't read configuration directory")
            })
            .try_filter_map(|entry| async move {
                let path = entry.path();

                if path.extension().is_none_or(|ext| ext != "toml") {
                    return Ok(None);
                }

                let file_type = entry.file_type().await.wrap_err("couldn't get file type")?;

                if !file_type.is_file() {
                    return Ok(None);
                }

                let Some(config) = Config::read(&path).await? else {
                    return Ok(None);
                };

                Ok(Some(ConfigEntry { path, config }))
            })
            .try_for_each(|entry| {
                configs.insert(entry);

                futures::future::ok(())
            })
            .await?;

        Ok(())
    }

    /// Reads a config.toml and config.d files in the directory.
    async fn read_config_dir(&mut self, dir: &Path) -> eyre::Result<()> {
        self.file_config
            .read_and_merge(dir.join(CONFIG_FILE))
            .await?;

        Self::append_configs(&mut self.config_d, &dir.join(CONFIG_D)).await?;

        Ok(())
    }

    /// Merges all configuration entries into a single one.
    pub(crate) fn merge_configs(&mut self) -> Config {
        self.iter_config_entries().map(|c| &c.config).cloned().fold(
            self.file_config.clone(),
            |mut acc, config_d| {
                acc.merge(config_d);

                acc
            },
        )
    }

    /// Attempts to convert the current configuration into a [`MessageHubOptions`] instance.
    pub async fn try_into_options(
        &mut self,
        store_dir: &StoreDir,
    ) -> eyre::Result<MessageHubOptions> {
        let mut config = self.merge_configs().clone();

        config.legacy_credential_secret(store_dir).await?;

        if !config.has_device_id() {
            info!("missing device id, querying dbus for hardware id");

            let device_id = hardware_id().await?;

            config.device_id = Some(device_id);
        }

        MessageHubOptions::try_from(config).wrap_err("couldn't build message hub options")
    }

    fn iter_config_entries(&self) -> impl Iterator<Item = &ConfigEntry> {
        let mut files = self.config_d.iter().peekable();
        let mut dynamic = self.dynamic.iter().peekable();

        std::iter::from_fn(move || {
            let f = files.peek();
            let d = dynamic.peek();

            match (f, d) {
                (None, None) => None,
                (None, Some(_d)) => dynamic.next(),
                (Some(_f), None) => files.next(),
                (Some(f), Some(d)) => match f.cmp(d) {
                    Ordering::Less => files.next(),
                    Ordering::Equal => files.next(),
                    Ordering::Greater => dynamic.next(),
                },
            }
        })
    }

    /// Add a dynamic config
    pub fn add_dynamic(&mut self, entry: ConfigEntry) {
        self.dynamic.insert(entry);
    }
}

/// Entry in a config.d directory.
// TODO: use lexicographical sorting for numbers
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    pub(crate) path: PathBuf,
    pub(crate) config: Config,
}

impl ConfigEntry {
    pub(crate) fn new(path: PathBuf, config: Config) -> Self {
        Self { path, config }
    }
}

impl PartialOrd for ConfigEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ConfigEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.path.file_name().cmp(&other.path.file_name())
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn list_config_files() -> eyre::Result<()> {
        let dir = TempDir::with_prefix("list_config_files")?;

        tokio::fs::write(dir.path().join("config.toml"), "").await?;

        let config_d = dir.path().join("config.d");
        tokio::fs::create_dir(&config_d).await?;

        tokio::fs::write(dir.path().join("config.toml"), "").await?;
        tokio::fs::write(config_d.join("10-low.toml"), "").await?;
        tokio::fs::write(config_d.join("50-medium.toml"), "").await?;
        tokio::fs::write(config_d.join("99-high.toml"), "").await?;

        let store = dir.path().join("storage");
        let storage_config = store.join("config");

        tokio::fs::create_dir_all(&storage_config).await?;

        tokio::fs::write(storage_config.join("60-dynamic.toml"), "").await?;

        let mut loader = ConfigRepository::default();
        let store_dir = StoreDir::create(store).await.unwrap();

        let custom_dir = CustomConfig::Dir(dir.path().to_path_buf());
        loader.read_configs(&Some(custom_dir)).await.unwrap();
        loader.read_dynamic(&store_dir).await.unwrap();

        let exp = [
            "config.d/10-low.toml",
            "config.d/50-medium.toml",
            "storage/config/60-dynamic.toml",
            "config.d/99-high.toml",
        ]
        .map(|path| dir.path().join(path));

        let files: Vec<PathBuf> = loader
            .iter_config_entries()
            .map(|c| &c.path)
            .cloned()
            .collect();

        assert_eq!(files, exp);

        Ok(())
    }
}
