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

//! Permits the caching of the Device Introspection

use std::path::PathBuf;

use astarte_device_sdk::Interface;
use log::{debug, error};

/// Caching for the device introspection.
pub(crate) struct Introspection {
    interface_dir: PathBuf,
}

impl Introspection {
    pub(crate) fn new(interface_dir: impl Into<PathBuf>) -> Self {
        Self {
            interface_dir: interface_dir.into(),
        }
    }

    pub(crate) async fn store(&self, interface: &Interface) {
        debug!("caching {}", interface.interface_name());

        let file = self.interface_file(interface.interface_name());

        let contents = match serde_json::to_vec(interface) {
            Ok(i) => i,
            Err(err) => {
                error!(
                    "couldn't serialize interface {}: {err}",
                    interface.interface_name()
                );

                return;
            }
        };

        if let Err(err) = tokio::fs::write(&file, contents).await {
            error!("couldn't write to {}: {err}", file.display())
        }
    }

    fn interface_file(&self, interface_name: &str) -> PathBuf {
        self.interface_dir.join(format!("{}.json", interface_name))
    }

    pub(crate) async fn store_many<'a, I>(&self, interfaces: I)
    where
        I: IntoIterator<Item = &'a Interface>,
    {
        for interface in interfaces {
            self.store(interface).await;
        }
    }

    pub(crate) async fn remove<S>(&self, interface_name: S)
    where
        S: AsRef<str>,
    {
        debug!("removind interface {}", interface_name.as_ref());

        let file = self.interface_file(interface_name.as_ref());

        if let Err(err) = tokio::fs::remove_file(&file).await {
            error!("couldn't remove interface file {}: {err}", file.display());
        }
    }

    pub(crate) async fn remove_many<S>(&self, interface_names: &[S])
    where
        S: AsRef<str>,
    {
        for interface_name in interface_names {
            self.remove(interface_name).await;
        }
    }
}
