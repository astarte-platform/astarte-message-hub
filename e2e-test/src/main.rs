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

use std::panic;

mod hub;
mod mock_data_datastream;
mod node;
mod utils;

#[tokio::main]
async fn main() -> Result<(), astarte_message_hub::error::AstarteMessageHubError> {
    env_logger::init();
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        println!("Test failed");
        orig_hook(panic_info);
        std::process::exit(1);
    }));

    //Waiting for Astarte Cluster to be ready...
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    tokio::spawn(async {
        //Waiting for Astarte Cluster to be ready...
        tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
        node::run().await;
    });

    hub::astarte_message_hub_run().await?;

    Ok(())
}
