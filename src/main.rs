/*
 * This file is part of Astarte.
 *
 * Copyright 2022 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
//! Contains the main application runner for the Astarte message hub.

use astarte_device_sdk::options::AstarteOptions;
use astarte_device_sdk::registration::register_device;
use astarte_device_sdk::AstarteDeviceSdk;
use astarte_message_hub::config::MessageHubOptions;
use astarte_message_hub::error::AstarteMessageHubError;

#[tokio::main]
async fn main() -> Result<(), AstarteMessageHubError> {
    env_logger::init();

    let options = MessageHubOptions::get().await?;
    let astarte_options = astarte_map_options(&options).await;
    let _astarte_device_sdk = AstarteDeviceSdk::new(&astarte_options).await?;

    //TODO add MessageHubServer

    Ok(())
}

pub async fn astarte_map_options(opts: &MessageHubOptions) -> AstarteOptions {
    let credentials_secret = match &opts.credentials_secret {
        None => register_device(
            opts.pairing_token.as_ref().unwrap(),
            opts.pairing_url.as_ref().unwrap(),
            opts.realm.as_ref().unwrap(),
            opts.device_id.as_ref().unwrap(),
        )
        .await
        .unwrap(),
        Some(secret) => secret.clone(),
    };
    AstarteOptions::new(
        opts.realm.as_ref().unwrap(),
        opts.device_id.as_ref().unwrap(),
        &credentials_secret,
        opts.pairing_url.as_ref().unwrap(),
    )
}
