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

use astarte_device_fdo::client::Client;
use astarte_device_fdo::di::Di;
use astarte_device_fdo::srv_info::AstarteMod;
use astarte_device_fdo::storage::FileStorage;
use astarte_device_fdo::to1::To1;
use astarte_device_fdo::to2::To2;
use astarte_device_fdo::Ctx;
use log::info;

pub async fn fdo<'a>(
    dir: PathBuf,
    tls: rustls::ClientConfig,
    manufactoring_url: astarte_device_fdo::url::Url,
    model_no: &'a str,
    serial_no: &'a str,
) -> eyre::Result<AstarteMod<'static>> {
    let mut storage = FileStorage::open(dir).await?;

    cfg_if::cfg_if! {
        if #[cfg(feature = "tpm")] {
            let mut crypto = astarte_device_fdo::crypto::tpm::Tpm::create(&storage).await?;
        }else {
            let mut crypto =  astarte_device_fdo::crypto::software::SoftwareCrypto::create(storage.clone()).await?;
        }
    }

    let mut ctx = Ctx::new(&mut crypto, &mut storage, tls.clone());

    let client = Client::create(manufactoring_url, tls)?;

    let di = Di::create(&mut ctx, client, model_no, serial_no).await?;

    let cred = di.create_credentials(&mut ctx).await?;

    if !cred.dc_active {
        info!("device change TO already run to completion");

        let dv = To2::read_existing(&mut ctx).await?;

        info!(
            "Astarte mod already stored with device_id: {}",
            dv.device_id
        );

        return Ok(dv);
    }

    let to1 = To1::new(&cred);

    let rv = to1.rv_owner(&mut ctx).await?;

    let to2 = To2::create(cred, rv, serial_no)?;

    let amod = to2.to2_change(&mut ctx).await?;

    info!("Astarte mod received with device_id: {}", amod.device_id);

    Ok(amod)
}
