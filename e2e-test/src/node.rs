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

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use astarte_device_sdk::types::AstarteType;
use astarte_device_sdk::AstarteDeviceDataEvent;
use colored::Colorize;
use log::{debug, info};
use serde_json::Value;
use tokio::time;
use tonic::transport::Channel;

use astarte_message_hub::proto_message_hub::astarte_message::Payload;
use astarte_message_hub::proto_message_hub::message_hub_client::MessageHubClient;
use astarte_message_hub::proto_message_hub::{AstarteMessage, Node};

use crate::mock_data_datastream::MockDataDatastream;
use crate::mock_data_property::MockDataProperty;

const NODE_UUID: &str = "f726577b-7084-4559-9a1d-788c6ec663bf";

#[derive(Clone)]
struct TestCfg {
    realm: String,
    device_id: String,
    api_url: String,
    interfaces_fld: PathBuf,
    interface_datastream_so: String,
    interface_datastream_do: String,
    interface_property_so: String,
    interface_property_do: String,
    appengine_token: String,
}

impl TestCfg {
    pub fn init() -> Result<Self, String> {
        let realm = env::var("E2E_REALM").map_err(|msg| msg.to_string())?;
        let device_id = env::var("E2E_DEVICE_ID").map_err(|msg| msg.to_string())?;
        let api_url = env::var("E2E_API_URL").map_err(|msg| msg.to_string())?;
        let appengine_token = env::var("E2E_TOKEN").map_err(|e| e.to_string())?;

        let interfaces_fld = env::current_dir()
            .map_err(|msg| msg.to_string())?
            .join("e2e-test")
            .join("interfaces");

        let interface_datastream_so =
            "org.astarte-platform.rust.e2etest.ServerDatastream".to_string();
        let interface_datastream_do =
            "org.astarte-platform.rust.e2etest.DeviceDatastream".to_string();
        let interface_property_do = "org.astarte-platform.rust.e2etest.DeviceProperty".to_string();
        let interface_property_so = "org.astarte-platform.rust.e2etest.ServerProperty".to_string();

        Ok(TestCfg {
            realm,
            device_id,
            api_url,
            interfaces_fld,
            interface_datastream_so,
            interface_datastream_do,
            interface_property_so,
            interface_property_do,
            appengine_token,
        })
    }
}

pub async fn run() {
    let test_cfg = TestCfg::init().expect("Failed configuration initialization");

    let mut client = MessageHubClient::connect("http://[::1]:50051")
        .await
        .unwrap();

    let interface_jsons = [
        include_str!("../interfaces/org.astarte-platform.rust.e2etest.DeviceDatastream.json"),
        include_str!("../interfaces/org.astarte-platform.rust.e2etest.ServerDatastream.json"),
        include_str!("../interfaces/org.astarte-platform.rust.e2etest.DeviceProperty.json"),
        include_str!("../interfaces/org.astarte-platform.rust.e2etest.ServerProperty.json"),
    ];

    let node = Node::new(&NODE_UUID, &interface_jsons);

    let mut stream = client.attach(node.clone()).await.unwrap().into_inner();

    let test_cfg_cpy = test_cfg.clone();
    let rx_data_ind_datastream = Arc::new(Mutex::new(HashMap::new()));
    let rx_data_ind_prop = Arc::new(Mutex::new((String::new(), HashMap::new())));

    let rx_data_ind_datastream_cpy = rx_data_ind_datastream.clone();
    let rx_data_ind_prop_cpy = rx_data_ind_prop.clone();

    // Start a separate task to handle incoming data
    let reply_handle = tokio::spawn(async move {
        info!("Waiting for messages from the message hub.");

        while let Some(astarte_message) = stream.message().await.unwrap() {
            println!("Received AstarteMessage = {:?}", astarte_message);

            if astarte_message.interface_name == test_cfg.interface_datastream_so {
                let astarte_device_data_event: AstarteDeviceDataEvent =
                    astarte_message.try_into().unwrap();
                if let astarte_device_sdk::Aggregation::Individual(var) =
                    astarte_device_data_event.data
                {
                    let mut rx_data = rx_data_ind_datastream.lock().unwrap();
                    let mut key = astarte_device_data_event.path.clone();
                    key.remove(0);
                    rx_data.insert(key, var);
                } else {
                    panic!("Received unexpected message!");
                }
            } else if astarte_message.interface_name == test_cfg.interface_property_so {
                let astarte_device_data_event: AstarteDeviceDataEvent =
                    astarte_message.try_into().unwrap();
                if let astarte_device_sdk::Aggregation::Individual(var) =
                    astarte_device_data_event.data.clone()
                {
                    let mut rx_data = rx_data_ind_prop.lock().unwrap();
                    let mut path = astarte_device_data_event.path.clone();
                    path.remove(0); // Remove first forward slash
                    let panic_msg =
                        format!("Incorrect path in message {:?}", &astarte_device_data_event);
                    let (sensor_n, key) = path.split_once('/').expect(&panic_msg);
                    rx_data.0 = sensor_n.to_string();
                    rx_data.1.insert(key.to_string(), var);
                } else {
                    panic!("Received unexpected message!");
                }
            } else {
                panic!("Received unexpected message!");
            }
        }

        info!("Done receiving messages, closing the connection.");
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

    test_datastream_device_to_server(&mut client, &test_cfg_cpy)
        .await
        .unwrap();
    test_datastream_server_to_device(&test_cfg_cpy, &rx_data_ind_datastream_cpy)
        .await
        .unwrap();
    // Run properties tests
    test_property_device_to_server(&mut client, &test_cfg_cpy)
        .await
        .unwrap();
    test_property_server_to_device(&test_cfg_cpy, &rx_data_ind_prop_cpy)
        .await
        .unwrap();

    client.detach(node).await.expect("Detach failed");
    println!("Client detached");

    match tokio::join!(reply_handle) {
        (Ok(_),) => (),
        (Err(e),) => panic!("Error: {}", e),
    }
}

async fn test_datastream_device_to_server(
    client: &mut MessageHubClient<Channel>,
    test_cfg: &TestCfg,
) -> Result<(), String> {
    let mock_data = MockDataDatastream::init();
    let tx_data = mock_data.get_device_to_server_data_as_astarte();

    // Send all the mock test data
    let msg = "\nSending device owned datastreams from device to server.".cyan();
    println!("{msg}");

    for (key, value) in tx_data.clone() {
        let astarte_message_payload = Payload::try_from(value).map_err(|e| e.to_string())?;
        let astarte_message = AstarteMessage {
            interface_name: test_cfg.interface_datastream_do.to_string(),
            path: format!("/{key}"),
            timestamp: None,
            payload: Some(astarte_message_payload),
        };

        client
            .send(astarte_message)
            .await
            .map_err(|e| e.to_string())?;
        time::sleep(Duration::from_millis(5)).await;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Get the stored data using http requests
    println!("{}", "\nChecking data stored on the server.".cyan());
    let http_get_response = http_get_intf(test_cfg, &test_cfg.interface_datastream_do).await?;

    // Check if the sent and received data match
    let data_json: Value = serde_json::from_str(&http_get_response)
        .map_err(|_| "Reply from server is a bad json.".to_string())?;

    let rx_data = MockDataDatastream::init()
        .fill_device_to_server_data_from_json(&data_json)?
        .get_device_to_server_data_as_astarte();

    if tx_data != rx_data {
        Err([
            "Mismatch between server and device.",
            &format!("Expected data: {tx_data:?}. Server data: {rx_data:?}."),
        ]
        .join(" "))
    } else {
        Ok(())
    }
}

/// Run the end to end tests from server to device for individual datastreams.
///
/// # Arguments
/// - *test_cfg*: struct containing configuration settings for the tests.
/// - *rx_data*: shared memory containing the received datastreams.
/// A different process will poll the device and then store the matching received messages
/// in this shared memory location.
async fn test_datastream_server_to_device(
    test_cfg: &TestCfg,
    rx_data: &Arc<Mutex<HashMap<String, AstarteType>>>,
) -> Result<(), String> {
    let mock_data = MockDataDatastream::init();

    // Send the data using http requests
    debug!("Sending server owned datastreams from server to device.");
    for (key, value) in mock_data.get_server_to_device_data_as_json() {
        http_post_to_intf(test_cfg, &test_cfg.interface_datastream_so, &key, value).await?;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Lock the shared data and check if everything sent has been correctly received

    debug!("Checking data received by the device.");
    let rx_data_rw_acc = rx_data
        .lock()
        .map_err(|e| format!("Failed to lock the shared data. {e}"))?;

    let exp_data = mock_data.get_server_to_device_data_as_astarte();
    if exp_data != *rx_data_rw_acc {
        Err([
            "Mismatch between expected and received data.",
            &format!("Expected data: {exp_data:?}. Server data: {rx_data_rw_acc:?}."),
        ]
        .join(" "))
    } else {
        Ok(())
    }
}

/// Run the end to end tests from device to server for properties.
///
/// # Arguments
/// - *device*: the Astarte SDK instance to use for the test.
/// - *test_cfg*: struct containing configuration settings for the tests.
async fn test_property_device_to_server(
    client: &mut MessageHubClient<Channel>,
    test_cfg: &TestCfg,
) -> Result<(), String> {
    let mock_data = MockDataProperty::init();
    let tx_data = mock_data.get_device_to_server_data_as_astarte();
    let sensor_number = 1;

    // Send all the mock test data
    debug!("Set device owned property (will be also sent to server).");
    for (key, value) in tx_data.clone() {
        let astarte_message_payload = Payload::try_from(value).map_err(|e| e.to_string())?;
        let astarte_message = AstarteMessage {
            interface_name: test_cfg.interface_property_do.to_string(),
            path: format!("/{sensor_number}/{key}"),
            timestamp: None,
            payload: Some(astarte_message_payload),
        };

        client
            .send(astarte_message)
            .await
            .map_err(|e| e.to_string())?;
        time::sleep(Duration::from_millis(5)).await;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Get the stored data using http requests
    debug!("Checking data stored on the server.");
    let http_get_response = http_get_intf(test_cfg, &test_cfg.interface_property_do).await?;

    // Check if the sent and received data match
    let data_json: Value = serde_json::from_str(&http_get_response)
        .map_err(|_| "Reply from server is a bad json.".to_string())?;

    let rx_data = MockDataProperty::init()
        .fill_device_to_server_data_from_json(&data_json, sensor_number)?
        .get_device_to_server_data_as_astarte();

    if tx_data != rx_data {
        return Err([
            "Mismatch between server and device.",
            &format!("Expected data: {tx_data:?}. Server data: {rx_data:?}."),
        ]
        .join(" "));
    }

    // Unset one specific property
    debug!("Unset all the device owned property (will be also sent to server).");
    for (key, _) in tx_data.clone() {
        let astarte_message_payload =
            Payload::try_from(AstarteType::Unset).map_err(|e| e.to_string())?;
        let astarte_message = AstarteMessage {
            interface_name: test_cfg.interface_property_do.to_string(),
            path: format!("/{sensor_number}/{key}"),
            timestamp: None,
            payload: Some(astarte_message_payload),
        };

        client
            .send(astarte_message)
            .await
            .map_err(|e| e.to_string())?;
        time::sleep(Duration::from_millis(5)).await;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Get the stored data using http requests
    debug!("Checking data stored on the server.");
    let http_get_response = http_get_intf(test_cfg, &test_cfg.interface_property_do).await?;

    if http_get_response != "{\"data\":{}}" {
        Err([
            "Mismatch between server and device.",
            &format!("Expected data: {{\"data\":{{}}}}. Server data: {http_get_response:?}."),
        ]
        .join(" "))
    } else {
        Ok(())
    }
}

/// Run the end to end tests from server to device for properties.
///
/// # Arguments
/// - *test_cfg*: struct containing configuration settings for the tests.
/// - *rx_data*: shared memory containing the received properties.
/// A different process will poll the device and then store the matching received messages
/// in this shared memory location.
async fn test_property_server_to_device(
    test_cfg: &TestCfg,
    rx_data: &Arc<Mutex<(String, HashMap<String, AstarteType>)>>,
) -> Result<(), String> {
    let mock_data = MockDataProperty::init();
    let sensor_number: i8 = 42;

    // Send the data using http requests
    debug!("Sending server owned properties from server to device.");
    for (key, value) in mock_data.get_server_to_device_data_as_json() {
        http_post_to_intf(
            test_cfg,
            &test_cfg.interface_property_so,
            &format!("{sensor_number}/{key}"),
            value,
        )
        .await?;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Lock the shared data and check if everything sent has been correctly received
    {
        debug!("Checking data received by the device.");
        let rx_data_rw_acc = rx_data
            .lock()
            .map_err(|e| format!("Failed to lock the shared data. {e}"))?;

        let exp_data = mock_data.get_server_to_device_data_as_astarte();
        if (sensor_number.to_string() != rx_data_rw_acc.0) || (exp_data != rx_data_rw_acc.1) {
            return Err([
                "Mismatch between expected and received data.",
                &format!("Expected data: {exp_data:?}. Server data: {rx_data_rw_acc:?}."),
            ]
            .join(" "));
        }
    }

    // Unset all the properties
    debug!("Unsetting all the server owned properties (will be also sent to device).");
    for (key, _) in mock_data.get_server_to_device_data_as_json() {
        http_delete_to_intf(
            test_cfg,
            &test_cfg.interface_property_so,
            &format!("{sensor_number}/{key}"),
        )
        .await?;
    }

    time::sleep(Duration::from_secs(1)).await;

    // Lock the shared data and check if everything sent has been correctly received
    {
        debug!("Checking data received by the device.");
        let rx_data_rw_acc = rx_data
            .lock()
            .map_err(|e| format!("Failed to lock the shared data. {e}"))?;

        if (sensor_number.to_string() != rx_data_rw_acc.0)
            || rx_data_rw_acc
                .1
                .iter()
                .any(|(_, value)| value != &AstarteType::Unset)
        {
            return Err(format!(
                "Uncorrect received data. Server data: {rx_data_rw_acc:?}."
            ));
        }
    }
    Ok(())
}

/// Perform an HTTP GET request to an Astarte interface.
///
/// # Arguments
/// - *test_cfg*: struct containing configuration settings for the request.
/// - *interface*: interface for which to perform the GET request.
async fn http_get_intf(test_cfg: &TestCfg, interface: &str) -> Result<String, String> {
    let get_cmd = format!(
        "{}/v1/{}/devices/{}/interfaces/{}",
        test_cfg.api_url, test_cfg.realm, test_cfg.device_id, interface
    );
    println!("Sending HTTP GET request: {get_cmd}");
    reqwest::Client::new()
        .get(get_cmd)
        .header(
            "Authorization",
            "Bearer ".to_string() + &test_cfg.appengine_token,
        )
        .send()
        .await
        .map_err(|e| format!("HTTP GET failure: {e}"))?
        .text()
        .await
        .map_err(|e| format!("Failure in parsing the HTTP GET result: {e}"))
}

/// Perform an HTTP POST request to an Astarte interface.
///
/// # Arguments
/// - *test_cfg*: struct containing configuration settings for the request.
/// - *interface*: interface on which to perform the POST request.
/// - *path*: path for the endpoint on which the data should be written.
/// - *value_json*: value to be sent, already formatted as a json string.
async fn http_post_to_intf(
    test_cfg: &TestCfg,
    interface: &str,
    path: &str,
    value_json: String,
) -> Result<(), String> {
    let post_cmd = format!(
        "{}/v1/{}/devices/{}/interfaces/{}/{}",
        test_cfg.api_url, test_cfg.realm, test_cfg.device_id, interface, path
    );
    debug!("Sending HTTP POST request: {post_cmd} {value_json}");
    let response = reqwest::Client::new()
        .post(post_cmd)
        .header(
            "Authorization",
            "Bearer ".to_string() + &test_cfg.appengine_token,
        )
        .header("Content-Type", "application/json")
        .body(value_json.clone())
        .send()
        .await
        .map_err(|e| format!("HTTP POST failure: {e}"))?;
    if response.status() != reqwest::StatusCode::OK {
        let response_text = response
            .text()
            .await
            .map_err(|e| format!("Failure in parsing the HTTP POST result: {e}"))?;
        return Err(format!(
            "Failure in POST command. Server response: {response_text}"
        ));
    }
    Ok(())
}

/// Perform an HTTP DELETE request to an Astarte interface.
///
/// # Arguments
/// - *test_cfg*: struct containing configuration settings for the request.
/// - *interface*: interface on which to perform the DELETE request.
/// - *path*: path for the endpoint for which the data should be deleted.
async fn http_delete_to_intf(
    test_cfg: &TestCfg,
    interface: &str,
    path: &str,
) -> Result<(), String> {
    let post_cmd = format!(
        "{}/v1/{}/devices/{}/interfaces/{}/{}",
        test_cfg.api_url, test_cfg.realm, test_cfg.device_id, interface, path
    );
    debug!("Sending HTTP DELETE request: {post_cmd}");
    let response = reqwest::Client::new()
        .delete(post_cmd)
        .header(
            "Authorization",
            "Bearer ".to_string() + &test_cfg.appengine_token,
        )
        .send()
        .await
        .map_err(|e| format!("HTTP DELETE failure: {e}"))?;
    if response.status() != reqwest::StatusCode::NO_CONTENT {
        let response_text = response
            .text()
            .await
            .map_err(|e| format!("Failure in parsing the HTTP DELETE result: {e}"))?;
        return Err(format!(
            "Failure in DELETE command. Server response: {response_text}"
        ));
    }
    Ok(())
}
