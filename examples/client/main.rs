/*
 * This file is part of Astarte.
 *
 * Copyright 2023 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

//! Astarte Message Hub client example, will send the uptime every 3 seconds to Astarte.

use std::time;

use clap::Parser;

use astarte_message_hub::proto_message_hub::astarte_message::Payload;
use astarte_message_hub::proto_message_hub::message_hub_client::MessageHubClient;
use astarte_message_hub::proto_message_hub::AstarteMessage;
use astarte_message_hub::proto_message_hub::Node;
use log::info;

/// Create a ProtoBuf client for the Astarte message hub.
#[derive(Parser, Debug)]
#[clap(version, about)]
struct Cli {
    /// UUID to be used when registering the client as an Astarte message hub node.
    uuid: String,

    /// Stop after sending COUNT messages.
    #[clap(short, long)]
    count: Option<u64>,

    /// Milliseconds to wait between messages.
    #[clap(short, long, default_value = "3000")]
    time: u64,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Cli::parse();

    let mut client = MessageHubClient::connect("http://[::1]:50051")
        .await
        .unwrap();

    let interface_jsons = [
        include_str!(
            "./interfaces/org.astarte-platform.rust.examples.datastream.DeviceDatastream.json"
        ),
        include_str!(
            "./interfaces/org.astarte-platform.rust.examples.datastream.ServerDatastream.json"
        ),
    ];

    let node = Node::new(&args.uuid, &interface_jsons);

    let mut stream = client.attach(node.clone()).await.unwrap().into_inner();

    // Start a separate task to handle incoming data
    let reply_handle = tokio::spawn(async move {
        info!("Waiting for messages from the message hub.");

        while let Some(astarte_message) = stream.message().await.unwrap() {
            println!("Received AstarteMessage = {:?}", astarte_message);
        }

        info!("Done receiving messages, closing the connection.");
    });

    // Start a separate task to publish data
    let send_handle = tokio::spawn(async move {
        let now = time::SystemTime::now();
        let mut count = 0;
        // Consistent interval of 3 seconds
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(args.time));

        while args.count.is_none() || Some(count) < args.count {
            // Wait a little
            interval.tick().await;

            println!("Publishing the uptime through the message hub.");

            let elapsed = now.elapsed().unwrap().as_secs();

            let elapsed_str = format!("Uptime for node {}: {}", args.uuid, elapsed);
            let msg = AstarteMessage {
                interface_name: "org.astarte-platform.rust.examples.datastream.DeviceDatastream"
                    .to_string(),
                path: "/uptime".to_string(),
                timestamp: None,
                payload: Some(Payload::AstarteData(elapsed_str.into())),
            };
            client.send(msg).await.unwrap();

            count += 1;
        }

        info!("Done sending messages, closing the connection.");
        client.detach(node).await.expect("Detach failed");
    });

    let res = tokio::join!(reply_handle, send_handle);

    match res {
        (Ok(_), Ok(_)) => (),
        (Err(e), Ok(_)) | (Ok(_), Err(e)) => panic!("Error: {}", e),
        (Err(e1), Err(e2)) => panic!("Error:\n\t{}\n\t{}", e1, e2),
    }
}
