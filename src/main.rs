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
use astarte_device_sdk::options::AstarteOptions;
use astarte_device_sdk::registration;
use astarte_device_sdk::AstarteDeviceSdk;

use astarte_message_hub::config::MessageHubOptions;
use astarte_message_hub::error::AstarteMessageHubError;

#[tokio::main]
async fn main() -> Result<(), AstarteMessageHubError> {
    env_logger::init();

    let options = MessageHubOptions::get().await?;
    let _astarte_sdk = initialize_astarte_device_sdk(options).await?;

    //TODO add MessageHubServer and add AstarteHandler::new() on top of AstarteSDK

    Ok(())
}

async fn initialize_astarte_device_sdk(
    mut msg_hub_opts: MessageHubOptions,
) -> Result<AstarteDeviceSdk, AstarteMessageHubError> {
    // If no credential secret is present, register a new device using the Astarte device SDK
    if msg_hub_opts.credentials_secret.is_none() {
        msg_hub_opts.credentials_secret = Some(
            registration::register_device(
                msg_hub_opts.pairing_token.as_ref().unwrap(),
                &msg_hub_opts.pairing_url,
                &msg_hub_opts.realm,
                &msg_hub_opts.device_id,
            )
            .await
            .map_err(|err| AstarteMessageHubError::FatalError(err.to_string()))?,
        );
    }
    // Create the configuration options for the device and then instantiate a new device
    let mut device_sdk_opts = AstarteOptions::new(
        &msg_hub_opts.realm,
        &msg_hub_opts.device_id,
        msg_hub_opts.credentials_secret.as_ref().unwrap(),
        &msg_hub_opts.pairing_url,
    );
    if msg_hub_opts.astarte_ignore_ssl {
        device_sdk_opts = device_sdk_opts.ignore_ssl_errors();
    }
    if let Some(int_dir) = &msg_hub_opts.interfaces_directory {
        device_sdk_opts = device_sdk_opts.interface_directory(int_dir)?;
    }
    Ok(AstarteDeviceSdk::new(&device_sdk_opts).await?)
}
