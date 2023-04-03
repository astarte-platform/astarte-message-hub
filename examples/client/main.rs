/*
 * This file is part of Astarte.
 *
 * Copyright 2021 SECO Mind Srl
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
use std::env;
use std::time;

use clap::Parser;

use astarte_message_hub::proto_message_hub::astarte_message::Payload;
use astarte_message_hub::proto_message_hub::message_hub_client::MessageHubClient;
use astarte_message_hub::proto_message_hub::AstarteMessage;
use astarte_message_hub::proto_message_hub::Node;


/// Create a ProtoBuf client for the Astarte message hub.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// UUID to be used when registering the client as an Astarte message hub node.
    #[clap(value_parser)]
    uuid: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();

    let now = time::SystemTime::now();

    let mut client = MessageHubClient::connect("http://[::1]:50051")
        .await
        .unwrap();

    let interfaces_folder = env::current_dir()
        .unwrap()
        .as_path()
        .join("examples")
        .join("client")
        .join("interfaces");
    let device_interface_file = interfaces_folder
        .join("org.astarte-platform.rust.examples.datastream.DeviceDatastream.json");
    let server_interface_file = interfaces_folder
        .join("org.astarte-platform.rust.examples.datastream.ServerDatastream.json");
    let interfaces_json = vec![
        std::fs::read(device_interface_file).unwrap(),
        std::fs::read(server_interface_file).unwrap(),
    ];

    let node = Node {
        uuid: args.uuid.clone(),
        interface_jsons: interfaces_json,
    };

    let mut stream = client
        .attach(tonic::Request::new(node))
        .await
        .unwrap()
        .into_inner();

    // Start a separate task to handle incoming data
    let reply_handle = tokio::task::spawn(async move {
        loop {
            if let Some(astarte_message) = stream.message().await.unwrap() {
                println!("Received AstarteMessage = {:?}", astarte_message);
            }
        }
    });

    // Start a separate task to publish data
    let send_handle = tokio::task::spawn(async move {
        loop {
            println!("Publishing the uptime through the message hub.");
            let elapsed = now.elapsed().unwrap().as_secs() as i64;
            let elapsed_str = format!("Uptime for node {}: {}", args.uuid, elapsed);
            let msg = AstarteMessage {
                interface_name: "org.astarte-platform.rust.examples.datastream.DeviceDatastream"
                    .to_string(),
                path: "/uptime".to_string(),
                timestamp: None,
                payload: Some(Payload::AstarteData(elapsed_str.into())),
            };
            client.send(msg).await.unwrap();
            // Wait a little
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    });

    loop {
        if reply_handle.is_finished() || send_handle.is_finished() {
            break;
        }
    }
}
