// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use astarte_device_sdk::{types::AstarteType, AstarteAggregate, Error};
use serde::Deserialize;

use crate::utils::{base64_decode, timestamp_from_rfc3339, Timestamp};

/// List of all the interfaces
pub static INTERFACES: &[&str] = &[
    DEVICE_AGGREGATE,
    DEVICE_DATASTREAM,
    DEVICE_PROPERTY,
    SERVER_AGGREGATE,
    SERVER_DATASTREAM,
    SERVER_PROPERTY,
];

pub static INTERFACE_NAMES: &[&str] = &[
    DEVICE_AGGREGATE_NAME,
    DEVICE_DATASTREAM_NAME,
    DEVICE_PROPERTY_NAME,
    SERVER_AGGREGATE_NAME,
    SERVER_DATASTREAM_NAME,
    SERVER_PROPERTY_NAME,
];

pub const ENDPOINTS: &[&str] = &[
    "double_endpoint",
    "integer_endpoint",
    "boolean_endpoint",
    "longinteger_endpoint",
    "string_endpoint",
    "binaryblob_endpoint",
    "datetime_endpoint",
    "doublearray_endpoint",
    "integerarray_endpoint",
    "booleanarray_endpoint",
    "longintegerarray_endpoint",
    "stringarray_endpoint",
    "binaryblobarray_endpoint",
    "datetimearray_endpoint",
];

pub const DEVICE_AGGREGATE: &str =
    include_str!("../interfaces/org.astarte-platform.rust.e2etest.DeviceAggregate.json");
pub const DEVICE_AGGREGATE_NAME: &str = "org.astarte-platform.rust.e2etest.DeviceAggregate";

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DeviceAggregate(pub Data);

impl DeviceAggregate {
    pub const fn name() -> &'static str {
        DEVICE_AGGREGATE_NAME
    }

    pub const fn interface() -> &'static str {
        DEVICE_AGGREGATE
    }

    pub const fn path() -> &'static str {
        "/sendor_1"
    }
}

impl Default for DeviceAggregate {
    fn default() -> Self {
        DeviceAggregate(Data::default())
    }
}

impl AstarteAggregate for DeviceAggregate {
    fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error> {
        self.0.astarte_aggregate()
    }
}

pub const DEVICE_DATASTREAM: &str =
    include_str!("../interfaces/org.astarte-platform.rust.e2etest.DeviceDatastream.json");
pub const DEVICE_DATASTREAM_NAME: &str = "org.astarte-platform.rust.e2etest.DeviceDatastream";

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DeviceDatastream(pub Data);

impl DeviceDatastream {
    pub const fn name() -> &'static str {
        DEVICE_DATASTREAM_NAME
    }

    pub const fn interface() -> &'static str {
        DEVICE_DATASTREAM
    }
}

impl Default for DeviceDatastream {
    fn default() -> Self {
        DeviceDatastream(Data::default())
    }
}

impl AstarteAggregate for DeviceDatastream {
    fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error> {
        self.0.astarte_aggregate()
    }
}

pub const DEVICE_PROPERTY: &str =
    include_str!("../interfaces/org.astarte-platform.rust.e2etest.DeviceProperty.json");
pub const DEVICE_PROPERTY_NAME: &str = "org.astarte-platform.rust.e2etest.DeviceProperty";

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DeviceProperty(pub Data);

impl DeviceProperty {
    pub const fn name() -> &'static str {
        DEVICE_PROPERTY
    }

    pub const fn interface() -> &'static str {
        DEVICE_PROPERTY_NAME
    }
}

impl Default for DeviceProperty {
    fn default() -> Self {
        DeviceProperty(Data::default())
    }
}

impl AstarteAggregate for DeviceProperty {
    fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error> {
        self.0.astarte_aggregate()
    }
}

pub const SERVER_AGGREGATE: &str =
    include_str!("../interfaces/org.astarte-platform.rust.e2etest.ServerAggregate.json");
pub const SERVER_AGGREGATE_NAME: &str = "org.astarte-platform.rust.e2etest.ServerAggregate";

pub const SERVER_DATASTREAM: &str =
    include_str!("../interfaces/org.astarte-platform.rust.e2etest.ServerDatastream.json");
pub const SERVER_DATASTREAM_NAME: &str = "org.astarte-platform.rust.e2etest.ServerDatastream";

pub const SERVER_PROPERTY: &str =
    include_str!("../interfaces/org.astarte-platform.rust.e2etest.ServerProperty.json");
pub const SERVER_PROPERTY_NAME: &str = "org.astarte-platform.rust.e2etest.ServerProperty";

#[derive(Debug, Clone, PartialEq, AstarteAggregate, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Data {
    double_endpoint: f64,
    integer_endpoint: i32,
    boolean_endpoint: bool,
    #[serde(deserialize_with = "crate::utils::des::deserialize_longinteger")]
    longinteger_endpoint: i64,
    string_endpoint: String,
    #[serde(deserialize_with = "crate::utils::des::deserialize_blob")]
    binaryblob_endpoint: Vec<u8>,
    datetime_endpoint: Timestamp,
    doublearray_endpoint: Vec<f64>,
    integerarray_endpoint: Vec<i32>,
    booleanarray_endpoint: Vec<bool>,
    #[serde(deserialize_with = "crate::utils::des::deserialize_longinteger_vec")]
    longintegerarray_endpoint: Vec<i64>,
    stringarray_endpoint: Vec<String>,
    #[serde(deserialize_with = "crate::utils::des::deserialize_blob_vec")]
    binaryblobarray_endpoint: Vec<Vec<u8>>,
    datetimearray_endpoint: Vec<Timestamp>,
}

impl Default for Data {
    fn default() -> Self {
        Data {
            double_endpoint: 4.34,
            integer_endpoint: 1,
            boolean_endpoint: true,
            longinteger_endpoint: 45543543534,
            string_endpoint: "Hello".to_string(),
            binaryblob_endpoint: base64_decode("aGVsbG8=").unwrap(),
            datetime_endpoint: timestamp_from_rfc3339("2021-09-29T17:46:48.000Z").unwrap(),
            doublearray_endpoint: Vec::from([43.5, 10.5, 11.9]),
            integerarray_endpoint: Vec::from([-4, 123, -2222, 30]),
            booleanarray_endpoint: Vec::from([true, false]),
            longintegerarray_endpoint: Vec::from([53267895478, 53267895428, 53267895118]),
            stringarray_endpoint: Vec::from(["Test ".to_string(), "String".to_string()]),
            binaryblobarray_endpoint: ["aGVsbG8=", "aGVsbG8="]
                .map(|s| base64_decode(s).unwrap())
                .to_vec(),
            datetimearray_endpoint: ["2021-10-23T17:46:48.000Z", "2021-11-11T17:46:48.000Z"]
                .map(|s| timestamp_from_rfc3339(s).unwrap())
                .to_vec(),
        }
    }
}
