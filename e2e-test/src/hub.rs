/*
 * This file is part of Astarte.
 *
 * Copyright 2023 SECO Mind Srl
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

use std::net::Ipv6Addr;

use log::info;

use astarte_message_hub::config::MessageHubOptions;
use astarte_message_hub::AstarteMessageHub;
use astarte_message_hub::MessageHubServer;

pub async fn astarte_message_hub_run(
) -> Result<(), astarte_message_hub::error::AstarteMessageHubError> {
    let realm = std::env::var("E2E_REALM").unwrap();
    let device_id = std::env::var("E2E_DEVICE_ID").unwrap();
    let credentials_secret = std::env::var("E2E_CREDENTIALS_SECRET").unwrap();
    let pairing_url = std::env::var("E2E_PAIRING_URL")
        .map_err(|msg| msg.to_string())
        .unwrap();

    let astarte_message_hub_options = MessageHubOptions {
        realm,
        device_id: Some(device_id),
        credentials_secret: Some(credentials_secret),
        pairing_url,
        pairing_token: None,
        interfaces_directory: None,
        astarte_ignore_ssl: true,
        grpc_socket_port: 50051,
        store_directory: Default::default(),
    };

    let mut device_sdk_opts = astarte_device_sdk::options::AstarteOptions::new(
        &astarte_message_hub_options.realm,
        &astarte_message_hub_options.device_id.unwrap(),
        &astarte_message_hub_options.credentials_secret.unwrap(),
        &astarte_message_hub_options.pairing_url,
    );

    if let Some(int_dir) = &astarte_message_hub_options.interfaces_directory {
        device_sdk_opts = device_sdk_opts.interface_directory(&int_dir.to_string_lossy())?;
    }

    if astarte_message_hub_options.astarte_ignore_ssl {
        device_sdk_opts = device_sdk_opts.ignore_ssl_errors();
    }

    let device_sdk = astarte_device_sdk::AstarteDeviceSdk::new(device_sdk_opts).await?;
    info!("Connection to Astarte established.");

    let handler = astarte_message_hub::AstarteHandler::new(device_sdk);
    let message_hub = AstarteMessageHub::new(handler);

    let addrs = (
        Ipv6Addr::LOCALHOST,
        astarte_message_hub_options.grpc_socket_port,
    )
        .into();

    tonic::transport::Server::builder()
        .add_service(MessageHubServer::new(message_hub))
        .serve(addrs)
        .await?;

    Ok(())
}
