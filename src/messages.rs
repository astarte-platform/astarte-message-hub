// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Conversions between the Astarte and gRPC types.
//!
//! This module is used to not enable the `message-hub` feature in the sdk, since we only use the
//! conversions and not the actual connection.

use astarte_device_sdk::store::StoredProp;
use astarte_device_sdk::{AstarteData, DeviceEvent};
use astarte_interfaces::schema::Ownership;
use astarte_message_hub_proto::astarte_message::Payload;
use astarte_message_hub_proto::prost_types::Timestamp;
use astarte_message_hub_proto::{
    AstarteData as ProtoDataWrapper, Property, astarte_data::AstarteData as ProtoData,
};
use astarte_message_hub_proto::{AstarteDatastreamIndividual, AstartePropertyIndividual};
use astarte_message_hub_proto::{AstarteDatastreamObject, AstarteMessage};
use chrono::{DateTime, Utc};
use tracing::error;

use crate::error::AstarteMessageHubError;

pub(crate) fn convert_event_to_message(event: DeviceEvent) -> AstarteMessage {
    let payload = match event.data {
        astarte_device_sdk::Value::Individual { data, timestamp } => {
            let data = AstarteDatastreamIndividual {
                data: Some(convert_data_into_proto(data)),
                timestamp: Some(convert_chrono_to_timestamp(timestamp)),
            };

            Payload::DatastreamIndividual(data)
        }
        astarte_device_sdk::Value::Object { data, timestamp } => {
            let data = data
                .into_key_values()
                .map(|(k, v)| (k, convert_data_into_proto(v)))
                .collect();

            Payload::DatastreamObject(AstarteDatastreamObject {
                data,
                timestamp: Some(convert_chrono_to_timestamp(timestamp)),
            })
        }
        astarte_device_sdk::Value::Property(astarte_data) => {
            let payload = AstartePropertyIndividual {
                data: astarte_data.map(convert_data_into_proto),
            };

            Payload::PropertyIndividual(payload)
        }
    };

    AstarteMessage {
        interface_name: event.interface,
        path: event.path,
        payload: Some(payload),
    }
}

/// Map a list of stored properties to the respective protobuf version
///
/// Unset values will result in a conversion error.
pub(crate) fn map_stored_properties_to_proto(
    props: Vec<StoredProp>,
) -> astarte_message_hub_proto::StoredProperties {
    let properties = props
        .into_iter()
        .map(|prop| {
            let ownership = match prop.ownership {
                Ownership::Device => astarte_message_hub_proto::Ownership::Device,
                Ownership::Server => astarte_message_hub_proto::Ownership::Server,
            };

            let data = convert_data_into_proto(prop.value);

            Property {
                interface_name: prop.interface,
                path: prop.path,
                version_major: prop.interface_major,
                ownership: ownership.into(),
                data: Some(data),
            }
        })
        .collect();

    astarte_message_hub_proto::StoredProperties { properties }
}

/// Converts astarte [`AstarteData`] into proto [`ProtoDataWrapper`]
pub(crate) fn convert_data_into_proto(data: AstarteData) -> ProtoDataWrapper {
    let astarte_data = match data {
        AstarteData::Double(value) => ProtoData::Double(*value),
        AstarteData::Integer(value) => ProtoData::Integer(value),
        AstarteData::Boolean(value) => ProtoData::Boolean(value),
        AstarteData::LongInteger(value) => ProtoData::LongInteger(value),
        AstarteData::String(value) => ProtoData::String(value),
        AstarteData::BinaryBlob(value) => ProtoData::BinaryBlob(value),
        AstarteData::DateTime(value) => ProtoData::DateTime(convert_chrono_to_timestamp(value)),
        AstarteData::DoubleArray(values) => {
            ProtoData::DoubleArray(astarte_message_hub_proto::AstarteDoubleArray {
                values: values.into_iter().map(Into::into).collect(),
            })
        }
        AstarteData::IntegerArray(values) => {
            ProtoData::IntegerArray(astarte_message_hub_proto::AstarteIntegerArray { values })
        }
        AstarteData::BooleanArray(values) => {
            ProtoData::BooleanArray(astarte_message_hub_proto::AstarteBooleanArray { values })
        }
        AstarteData::LongIntegerArray(values) => {
            ProtoData::LongIntegerArray(astarte_message_hub_proto::AstarteLongIntegerArray {
                values,
            })
        }
        AstarteData::StringArray(values) => {
            ProtoData::StringArray(astarte_message_hub_proto::AstarteStringArray { values })
        }
        AstarteData::BinaryBlobArray(values) => {
            ProtoData::BinaryBlobArray(astarte_message_hub_proto::AstarteBinaryBlobArray { values })
        }
        AstarteData::DateTimeArray(values) => {
            let values = values
                .into_iter()
                .map(convert_chrono_to_timestamp)
                .collect();

            ProtoData::DateTimeArray(astarte_message_hub_proto::AstarteDateTimeArray { values })
        }
    };

    ProtoDataWrapper {
        astarte_data: Some(astarte_data),
    }
}

/// Converts astarte [`AstarteData`] into proto [`ProtoDataWrapper`]
pub(crate) fn convert_proto_into_data(
    data: ProtoDataWrapper,
) -> Result<AstarteData, AstarteMessageHubError> {
    let astarte_data = data
        .astarte_data
        .ok_or(AstarteMessageHubError::Conversion {
            ctx: "missing astarte data filed",
        })?;

    match astarte_data {
        ProtoData::DateTime(v) => convert_timestamp_to_chrono(v).map(AstarteData::DateTime),
        ProtoData::Double(v) => AstarteData::try_from(v).map_err(|error| {
            error!(%error, "couldn't convert double");

            AstarteMessageHubError::Conversion {
                ctx: "invalid double",
            }
        }),
        ProtoData::Integer(v) => Ok(AstarteData::Integer(v)),
        ProtoData::Boolean(v) => Ok(AstarteData::Boolean(v)),
        ProtoData::LongInteger(v) => Ok(AstarteData::LongInteger(v)),
        ProtoData::String(v) => Ok(AstarteData::String(v)),
        ProtoData::BinaryBlob(v) => Ok(AstarteData::BinaryBlob(v)),
        ProtoData::DoubleArray(arr) => AstarteData::try_from(arr.values).map_err(|error| {
            error!(%error, "couldn't convert double");

            AstarteMessageHubError::Conversion {
                ctx: "invalid double",
            }
        }),
        ProtoData::IntegerArray(arr) => Ok(AstarteData::IntegerArray(arr.values)),
        ProtoData::BooleanArray(arr) => Ok(AstarteData::BooleanArray(arr.values)),
        ProtoData::LongIntegerArray(arr) => Ok(AstarteData::LongIntegerArray(arr.values)),
        ProtoData::StringArray(arr) => Ok(AstarteData::StringArray(arr.values)),
        ProtoData::BinaryBlobArray(arr) => Ok(AstarteData::BinaryBlobArray(arr.values)),
        ProtoData::DateTimeArray(arr) => arr
            .values
            .into_iter()
            .map(convert_timestamp_to_chrono)
            .collect::<Result<Vec<DateTime<Utc>>, AstarteMessageHubError>>()
            .map(AstarteData::DateTimeArray),
    }
}

/// Convert between timestamp types.
///
/// This tries to handle leap seconds the best it can.
pub(crate) fn convert_chrono_to_timestamp(timestamp: chrono::DateTime<Utc>) -> Timestamp {
    let seconds = timestamp.timestamp();
    let nanos = timestamp
        .timestamp_subsec_nanos()
        .try_into()
        .unwrap_or(i32::MAX);

    Timestamp { seconds, nanos }.normalized()
}
/// Convert between timestamp types.
pub(crate) fn convert_timestamp_to_chrono(
    timestamp: Timestamp,
) -> Result<chrono::DateTime<Utc>, AstarteMessageHubError> {
    let Timestamp { seconds, nanos } = timestamp.normalized();

    let nanos = u32::try_from(nanos).unwrap_or(0);

    DateTime::from_timestamp(seconds, nanos).ok_or(AstarteMessageHubError::Conversion {
        ctx: "invalid timestamp",
    })
}
