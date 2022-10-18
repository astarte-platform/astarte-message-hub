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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use log::info;
use tokio::sync::RwLock;
use tonic::transport::Server;

use astarte_message_hub::AstarteMessageHub;
use astarte_message_hub::MessageHubServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let addr: SocketAddr = "[::1]:10000".parse().unwrap();

    let astarte_message_hub = AstarteMessageHub {
        nodes: Arc::new(RwLock::new(HashMap::new())),
    };

    let astarte_message_server = MessageHubServer::new(astarte_message_hub);
    Server::builder()
        .add_service(astarte_message_server)
        .serve(addr)
        .await?;
    info!("Astarte Message Hub listening on: {}", addr);

    Ok(())
}
