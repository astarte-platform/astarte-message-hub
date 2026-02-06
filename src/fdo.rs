// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use astarte_device_fdo::Ctx;
use astarte_device_fdo::client::http::InitialClient;
use astarte_device_fdo::di::Di;
use astarte_device_fdo::srv_info::{AstarteMod, AstarteModBuilder};
use astarte_device_fdo::storage::FileStorage;
use astarte_device_fdo::to1::To1;
use astarte_device_fdo::to2::To2;
use astarte_device_sdk::pairing::fdo::FdoConfig;
use eyre::OptionExt;
use log::info;

pub async fn fdo<'a>(
    dir: PathBuf,
    tls: rustls::ClientConfig,
    manufactoring_url: astarte_device_fdo::url::Url,
    model_no: &'a str,
    serial_no: &'a str,
) -> eyre::Result<AstarteMod<'static>> {
    let builder = FdoConfig::build(model_no, serial_no).set_storage(path);
}
