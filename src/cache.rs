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

use astarte_device_sdk::astarte_interfaces::Interface;
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
        self.interface_dir.join(format!("{interface_name}.json"))
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

#[cfg(test)]
mod tests {
    use std::{io, path::Path, str::FromStr};

    use tempfile::TempDir;
    use tokio::fs;

    use super::*;

    const DEVICE_PROPERTY: &str = include_str!(
        "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceProperty.json"
    );
    const DEVICE_AGGREGATE: &str = include_str!(
        "../e2e-test/interfaces/org.astarte-platform.rust.e2etest.DeviceAggregate.json"
    );

    #[tokio::test]
    async fn should_store() {
        let dir = TempDir::new().unwrap();

        let intro = Introspection::new(dir.path());

        let interface = Interface::from_str(DEVICE_PROPERTY).unwrap();

        intro.store(&interface).await;

        let cached = fs::read_to_string(
            dir.path()
                .join("org.astarte-platform.rust.e2etest.DeviceProperty.json"),
        )
        .await
        .expect("failed to read cached interface file");

        let res = Interface::from_str(&cached).unwrap();

        assert_eq!(res, interface);
    }

    #[tokio::test]
    async fn store_should_not_error() {
        let dir = Path::new("/foo/bar/none-existing");

        let intro = Introspection::new(dir);

        let interface = Interface::from_str(DEVICE_PROPERTY).unwrap();

        intro.store(&interface).await;
    }

    #[tokio::test]
    async fn should_store_many() {
        let dir = TempDir::new().unwrap();

        let intro = Introspection::new(dir.path());

        let prop = Interface::from_str(DEVICE_PROPERTY).unwrap();
        let agg = Interface::from_str(DEVICE_AGGREGATE).unwrap();

        let exp = [prop.clone(), agg.clone()];

        intro.store_many(&exp).await;

        let cached = fs::read_to_string(
            dir.path()
                .join("org.astarte-platform.rust.e2etest.DeviceProperty.json"),
        )
        .await
        .expect("failed to read cached interface file");

        let res = Interface::from_str(&cached).unwrap();

        assert_eq!(res, prop);

        let cached = fs::read_to_string(
            dir.path()
                .join("org.astarte-platform.rust.e2etest.DeviceAggregate.json"),
        )
        .await
        .expect("failed to read cached interface file");

        let res = Interface::from_str(&cached).unwrap();

        assert_eq!(res, agg);
    }

    #[tokio::test]
    async fn store_many_should_not_error() {
        let dir = Path::new("/foo/bar/none-existing");

        let intro = Introspection::new(dir);

        let prop = Interface::from_str(DEVICE_PROPERTY).unwrap();
        let agg = Interface::from_str(DEVICE_AGGREGATE).unwrap();

        let exp = [prop.clone(), agg.clone()];

        intro.store_many(&exp).await;
    }

    #[tokio::test]
    async fn should_remove() {
        let dir = TempDir::new().unwrap();

        let intro = Introspection::new(dir.path());

        let prop = Interface::from_str(DEVICE_PROPERTY).unwrap();
        let agg = Interface::from_str(DEVICE_AGGREGATE).unwrap();

        let exp = [prop.clone(), agg.clone()];

        intro.store_many(&exp).await;

        let cached = dir
            .path()
            .join("org.astarte-platform.rust.e2etest.DeviceProperty.json");

        assert!(cached.is_file());

        intro.remove(prop.interface_name()).await;

        let err = tokio::fs::read(cached).await.unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn remove_should_not_error() {
        let dir = Path::new("/foo/bar/none-existing");

        let intro = Introspection::new(dir);

        let prop = Interface::from_str(DEVICE_PROPERTY).unwrap();
        let agg = Interface::from_str(DEVICE_AGGREGATE).unwrap();

        let exp = [prop.clone(), agg.clone()];

        intro.store_many(&exp).await;
    }

    #[tokio::test]
    async fn should_remove_many() {
        let dir = TempDir::new().unwrap();

        let intro = Introspection::new(dir.path());

        let prop = Interface::from_str(DEVICE_PROPERTY).unwrap();
        let agg = Interface::from_str(DEVICE_AGGREGATE).unwrap();

        let exp = [prop.clone(), agg.clone()];

        intro.store_many(&exp).await;

        let cached = dir
            .path()
            .join("org.astarte-platform.rust.e2etest.DeviceProperty.json");

        assert!(cached.is_file());

        intro.remove_many(&[prop.interface_name()]).await;

        let err = tokio::fs::read(cached).await.unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn remove_many_should_not_error() {
        let dir = Path::new("/foo/bar/none-existing");

        let intro = Introspection::new(dir);

        let prop = Interface::from_str(DEVICE_PROPERTY).unwrap();
        let agg = Interface::from_str(DEVICE_AGGREGATE).unwrap();

        let exp = [prop.clone(), agg.clone()];

        intro.store_many(&exp).await;

        intro.remove_many(&[prop.interface_name()]).await;
    }
}
