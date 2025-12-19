/*
 * This file is part of Astarte.
 *
 * Copyright 2025 SECO Mind Srl
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

use std::time::Duration;

use astarte_device_sdk::{
    store::{PropertyStore, StoreCapabilities},
    transport::mqtt::Mqtt,
    DeviceClient,
};
use chrono::Utc;
use log::debug;

pub(crate) async fn check_cert_expiry<S>(client: DeviceClient<Mqtt<S>>) -> Result<(), eyre::Report>
where
    S: PropertyStore + StoreCapabilities,
{
    // NOTE interval of 10 days to check certificate expiry
    const CHECK_CERT_INTERVAL: Duration = Duration::from_secs(60 * 60 * 24 * 10);

    let mut interval = tokio::time::interval(CHECK_CERT_INTERVAL);

    loop {
        let check_datetime = Utc::now() + CHECK_CERT_INTERVAL;

        let cert_check = client.is_valid_at(check_datetime).await;

        if let Some(cert_check_res) = cert_check {
            if cert_check_res {
                debug!(
                    "Astarte client certificate will be valid at {}",
                    check_datetime
                );
            } else {
                debug!(
                    "Astarte client certificate will be invalid at {}",
                    check_datetime
                );
            }
        } else {
            debug!("Could not check certificate validity");
        }

        interval.tick().await;
    }
}
