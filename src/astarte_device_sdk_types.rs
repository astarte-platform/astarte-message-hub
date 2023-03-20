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
//! Contains conversion traits to convert the Astarte types in the protobuf format to the
//! Astarte types from the Astarte device SDK.

use std::collections::HashMap;

use chrono::DateTime;

use crate::error::AstarteMessageHubError;
use crate::proto_message_hub;

/// This macro can be used to implement the TryFrom trait for the AstarteType from one or more of
/// the protobuf types.
macro_rules! impl_individual_data_to_astarte_type_conversion_traits {
    (scalar $($typ:ident, $astartedatatype:ident),*; vector $($arraytyp:ident, $astartearraydatatype:ident),*) => {
        impl TryFrom<proto_message_hub::astarte_data_type_individual::IndividualData> for astarte_device_sdk::types::AstarteType {
            type Error = AstarteMessageHubError;
            fn try_from(
                d: proto_message_hub::astarte_data_type_individual::IndividualData
            ) -> Result<Self, Self::Error> {
                use crate::proto_message_hub::astarte_data_type_individual::IndividualData;
                use astarte_device_sdk::types::AstarteType;

                match d {
                    $(
                    IndividualData::$typ(val) => {
                        Ok(AstarteType::$astartedatatype(val.try_into()?))
                    }
                    )?
                    $(
                    IndividualData::$arraytyp(val) => {
                        Ok(AstarteType::$astartearraydatatype(val.values.try_into()?))
                    }
                    )?
                    proto_message_hub::astarte_data_type_individual::IndividualData::AstarteDateTimeArray(val) => {
                        let mut times: Vec<DateTime<chrono::Utc>> = vec![];
                        for time in val.values.iter() {
                            times.push(time.clone().try_into()?);
                        }
                        Ok(astarte_device_sdk::types::AstarteType::DateTimeArray(times))
                    }
                }
            }
        }
    }
}

impl_individual_data_to_astarte_type_conversion_traits!(
    scalar
    AstarteDouble, Double,
    AstarteInteger,  Integer,
    AstarteBoolean, Boolean,
    AstarteLongInteger,LongInteger,
    AstarteString, String,
    AstarteBinaryBlob, BinaryBlob,
    AstarteDateTime, DateTime ;
    vector
    AstarteDoubleArray, DoubleArray,
    AstarteIntegerArray, IntegerArray,
    AstarteBooleanArray, BooleanArray,
    AstarteLongIntegerArray, LongIntegerArray,
    AstarteStringArray, StringArray,
    AstarteBinaryBlobArray, BinaryBlobArray
);

impl TryFrom<proto_message_hub::AstarteMessage> for astarte_device_sdk::AstarteDeviceDataEvent {
    type Error = AstarteMessageHubError;

    fn try_from(astarte_message: proto_message_hub::AstarteMessage) -> Result<Self, Self::Error> {
        let astarte_sdk_aggregation = match astarte_message
            .payload
            .ok_or(AstarteMessageHubError::ConversionError)?
        {
            proto_message_hub::astarte_message::Payload::AstarteData(astarte_data_type) => {
                match astarte_data_type
                    .data
                    .ok_or(AstarteMessageHubError::ConversionError)?
                {
                    proto_message_hub::astarte_data_type::Data::AstarteIndividual(
                        astarte_individual,
                    ) => {
                        let astarte_type: astarte_device_sdk::types::AstarteType =
                            astarte_individual
                                .individual_data
                                .ok_or(AstarteMessageHubError::ConversionError)?
                                .try_into()?;
                        astarte_device_sdk::Aggregation::Individual(astarte_type)
                    }
                    proto_message_hub::astarte_data_type::Data::AstarteObject(astarte_object) => {
                        let astarte_sdk_aggregation =
                            crate::types::map_values_to_astarte_type(astarte_object.object_data)?;
                        astarte_device_sdk::Aggregation::Object(astarte_sdk_aggregation)
                    }
                }
            }
            proto_message_hub::astarte_message::Payload::AstarteUnset(_) => {
                astarte_device_sdk::Aggregation::Individual(
                    astarte_device_sdk::types::AstarteType::Unset,
                )
            }
        };

        Ok(astarte_device_sdk::AstarteDeviceDataEvent {
            interface: astarte_message.interface_name,
            path: astarte_message.path,
            data: astarte_sdk_aggregation,
        })
    }
}

/// This function can be used to convert a map of (String, astarte_device_sdk::types::AstarteType) into a
/// map of (String,  AstarteDataTypeIndividual).
pub fn map_values_to_astarte_data_type_individual(
    value: HashMap<String, astarte_device_sdk::types::AstarteType>,
) -> Result<HashMap<String, proto_message_hub::AstarteDataTypeIndividual>, AstarteMessageHubError> {
    let mut map: HashMap<String, proto_message_hub::AstarteDataTypeIndividual> = Default::default();
    for (key, astarte_type) in value.into_iter() {
        map.insert(key, astarte_type.try_into()?);
    }
    Ok(map)
}

impl TryFrom<crate::types::InterfaceJson> for astarte_device_sdk::Interface {
    type Error = AstarteMessageHubError;

    fn try_from(interface: crate::types::InterfaceJson) -> Result<Self, Self::Error> {
        use astarte_device_sdk::AstarteError;
        use astarte_device_sdk::Interface;
        use std::str::FromStr;

        let interface_str = String::from_utf8_lossy(&interface.0);
        Interface::from_str(interface_str.as_ref())
            .map_err(|err| AstarteMessageHubError::AstarteError(AstarteError::InterfaceError(err)))
    }
}

#[cfg(test)]
mod test {
    use crate::astarte_device_sdk_types::map_values_to_astarte_data_type_individual;
    use astarte_device_sdk::types::AstarteType;
    use astarte_device_sdk::{Aggregation, AstarteDeviceDataEvent};
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;

    use crate::proto_message_hub::astarte_data_type_individual::IndividualData;
    use crate::proto_message_hub::astarte_message::Payload;
    use crate::proto_message_hub::AstarteMessage;

    #[test]
    fn proto_astarte_double_into_astarte_device_sdk_type_success() {
        let value: f64 = 15.5;
        let expected_double_value = IndividualData::AstarteDouble(value);
        let astarte_type: AstarteType = expected_double_value.try_into().unwrap();

        if let AstarteType::Double(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_integer_into_astarte_device_sdk_type_success() {
        let value: i32 = 15;
        let expected_integer_value = IndividualData::AstarteInteger(value);
        let astarte_type: AstarteType = expected_integer_value.try_into().unwrap();

        if let AstarteType::Integer(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_boolean_into_astarte_device_sdk_type_success() {
        let value: bool = true;
        let expected_boolean_value = IndividualData::AstarteBoolean(value);
        let astarte_type: AstarteType = expected_boolean_value.try_into().unwrap();

        if let AstarteType::Boolean(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_long_integer_into_astarte_device_sdk_type_success() {
        let value: i64 = 154;
        let expected_long_integer_value = IndividualData::AstarteLongInteger(value);
        let astarte_type: AstarteType = expected_long_integer_value.try_into().unwrap();

        if let AstarteType::LongInteger(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_string_into_astarte_device_sdk_type_success() {
        let value: String = "test".to_owned();
        let expected_string_value = IndividualData::AstarteString(value.clone());
        let astarte_type: AstarteType = expected_string_value.try_into().unwrap();

        if let AstarteType::String(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_binary_blob_into_astarte_device_sdk_type_success() {
        let value: Vec<u8> = vec![10, 34];
        let expected_binary_blob_value = IndividualData::AstarteBinaryBlob(value.clone());
        let astarte_type: AstarteType = expected_binary_blob_value.try_into().unwrap();

        if let AstarteType::BinaryBlob(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_date_time_into_astarte_device_sdk_type_success() {
        let value: DateTime<Utc> = Utc::now();
        let expected_date_time_value = IndividualData::AstarteDateTime(value.into());
        let astarte_type: AstarteType = expected_date_time_value.try_into().unwrap();

        if let AstarteType::DateTime(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_double_array_into_astarte_device_sdk_type_success() {
        let value: Vec<f64> = vec![15.5, 18.7];
        use crate::proto_message_hub::AstarteDoubleArray;
        let expected_double_array_value = IndividualData::AstarteDoubleArray(AstarteDoubleArray {
            values: value.clone(),
        });
        let astarte_type: AstarteType = expected_double_array_value.try_into().unwrap();

        if let AstarteType::DoubleArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_integer_array_into_astarte_device_sdk_type_success() {
        let value: Vec<i32> = vec![15, 18];
        use crate::proto_message_hub::AstarteIntegerArray;
        let expected_integer_array_value =
            IndividualData::AstarteIntegerArray(AstarteIntegerArray {
                values: value.clone(),
            });
        let astarte_type: AstarteType = expected_integer_array_value.try_into().unwrap();

        if let AstarteType::IntegerArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_boolean_array_into_astarte_device_sdk_type_success() {
        let value: Vec<bool> = vec![false, true];
        use crate::proto_message_hub::AstarteBooleanArray;
        let expected_boolean_array_value =
            IndividualData::AstarteBooleanArray(AstarteBooleanArray {
                values: value.clone(),
            });
        let astarte_type: AstarteType = expected_boolean_array_value.try_into().unwrap();

        if let AstarteType::BooleanArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_long_integer_array_into_astarte_device_sdk_type_success() {
        let value: Vec<i64> = vec![1543, 18];
        use crate::proto_message_hub::AstarteLongIntegerArray;
        let expected_long_integer_array_value =
            IndividualData::AstarteLongIntegerArray(AstarteLongIntegerArray {
                values: value.clone(),
            });
        let astarte_type: AstarteType = expected_long_integer_array_value.try_into().unwrap();

        if let AstarteType::LongIntegerArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_string_array_into_astarte_device_sdk_type_success() {
        let value: Vec<String> = vec!["test1".to_owned(), "test2".to_owned()];
        use crate::proto_message_hub::AstarteStringArray;
        let expected_string_array_value = IndividualData::AstarteStringArray(AstarteStringArray {
            values: value.clone(),
        });
        let astarte_type: AstarteType = expected_string_array_value.try_into().unwrap();

        if let AstarteType::StringArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_binary_blob_array_into_astarte_device_sdk_type_success() {
        let value: Vec<Vec<u8>> = vec![vec![11, 201], vec![1, 241]];
        use crate::proto_message_hub::AstarteBinaryBlobArray;
        let expected_binary_blob_array_value =
            IndividualData::AstarteBinaryBlobArray(AstarteBinaryBlobArray {
                values: value.clone(),
            });
        let astarte_type: AstarteType = expected_binary_blob_array_value.try_into().unwrap();

        if let AstarteType::BinaryBlobArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_date_time_array_into_astarte_device_sdk_type_success() {
        use crate::proto_message_hub::AstarteDateTimeArray;
        use pbjson_types::Timestamp;

        let value: Vec<DateTime<Utc>> = vec![Utc::now(), Utc::now()];
        let expected_date_time_array_value =
            IndividualData::AstarteDateTimeArray(AstarteDateTimeArray {
                values: value
                    .clone()
                    .into_iter()
                    .map(|it| it.into())
                    .collect::<Vec<Timestamp>>(),
            });
        let astarte_type: AstarteType = expected_date_time_array_value.try_into().unwrap();

        if let AstarteType::DateTimeArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn convert_astarte_message_to_astarte_device_data_event_individual_success() {
        let expected_data: f64 = 15.5;
        let interface_name = "test.name.json".to_string();
        let interface_path = "test".to_string();

        let astarte_type: AstarteType = expected_data.try_into().unwrap();
        let payload: Payload = astarte_type.try_into().unwrap();

        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: interface_path.clone(),
            timestamp: None,
            payload: Some(payload),
        };

        let astarte_device_data_event: AstarteDeviceDataEvent = astarte_message.try_into().unwrap();

        assert_eq!(interface_name, astarte_device_data_event.interface);
        assert_eq!(interface_path, astarte_device_data_event.path);

        match astarte_device_data_event.data {
            Aggregation::Individual(value) => {
                assert_eq!(value, expected_data)
            }
            Aggregation::Object(_) => {
                panic!()
            }
        }
    }

    #[test]
    fn convert_astarte_message_to_astarte_device_data_event_object_success() {
        use crate::proto_message_hub::AstarteDataTypeIndividual;
        let interface_name = "test.name.json".to_string();
        let interface_path = "test".to_string();

        let expected_data_f64: f64 = 15.5;
        let expected_data_i32: i32 = 15;
        let mut object_map: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        object_map.insert("1".to_string(), expected_data_f64.try_into().unwrap());
        object_map.insert("2".to_string(), expected_data_i32.try_into().unwrap());

        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: interface_path.clone(),
            timestamp: None,
            payload: Some(Payload::AstarteData(object_map.try_into().unwrap())),
        };

        let astarte_device_data_event: AstarteDeviceDataEvent = astarte_message.try_into().unwrap();

        assert_eq!(interface_name, astarte_device_data_event.interface);
        assert_eq!(interface_path, astarte_device_data_event.path);

        match astarte_device_data_event.data {
            Aggregation::Individual(_) => {
                panic!()
            }
            Aggregation::Object(object_map) => {
                assert_eq!(
                    object_map.get("1").unwrap().clone(),
                    AstarteType::try_from(expected_data_f64).unwrap()
                );
                assert_eq!(
                    object_map.get("2").unwrap().clone(),
                    AstarteType::from(expected_data_i32)
                );
            }
        }
    }

    #[test]
    fn convert_astarte_message_to_astarte_device_data_event_unset_success() {
        let interface_name = "test.name.json".to_string();
        let interface_path = "test".to_string();

        let astarte_type: AstarteType = AstarteType::Unset;
        let payload: Payload = astarte_type.try_into().unwrap();

        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: interface_path.clone(),
            timestamp: None,
            payload: Some(payload),
        };

        let astarte_device_data_event: AstarteDeviceDataEvent = astarte_message.try_into().unwrap();

        assert_eq!(interface_name, astarte_device_data_event.interface);
        assert_eq!(interface_path, astarte_device_data_event.path);

        match astarte_device_data_event.data {
            Aggregation::Individual(value) => {
                assert_eq!(AstarteType::Unset, value)
            }
            Aggregation::Object(_) => {
                panic!()
            }
        }
    }

    #[test]
    fn convert_map_values_to_astarte_astarte_data_type_individual_success() {
        let expected_data: f64 = 15.5;
        use std::collections::HashMap;
        let astarte_type_map = HashMap::from([(
            "key1".to_string(),
            astarte_device_sdk::types::AstarteType::Double(expected_data),
        )]);

        let conversion_map_result = map_values_to_astarte_data_type_individual(astarte_type_map);
        assert!(conversion_map_result.is_ok());

        let astarte_individual_map = conversion_map_result.unwrap();

        if let IndividualData::AstarteDouble(double_data) = astarte_individual_map
            .get("key1")
            .unwrap()
            .individual_data
            .clone()
            .unwrap()
        {
            assert_eq!(expected_data, double_data)
        } else {
            panic!()
        }
    }

    #[test]
    fn convert_proto_interface_to_astarte_interface() {
        use astarte_device_sdk::Interface;

        use crate::types::InterfaceJson;

        const SERV_PROPS_IFACE: &str = r#"
        {
            "interface_name": "org.astarte-platform.test.test",
            "version_major": 1,
            "version_minor": 1,
            "type": "properties",
            "ownership": "server",
            "mappings": [
                {
                    "endpoint": "/button",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/uptimeSeconds",
                    "type": "integer",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;

        let interface = InterfaceJson(SERV_PROPS_IFACE.into());

        let astarte_interface: Interface = interface.try_into().unwrap();

        assert_eq!(
            astarte_interface.get_name(),
            "org.astarte-platform.test.test"
        );
        assert_eq!(astarte_interface.get_version_major(), 1);
    }

    #[tokio::test]
    async fn convert_proto_interface_with_special_chars_to_astarte_interface() {
        use astarte_device_sdk::Interface;

        use crate::types::InterfaceJson;

        const IFACE_SPECIAL_CHARS: &str = r#"
        {
            "interface_name": "org.astarte-platform.test.test",
            "version_major": 1,
            "version_minor": 1,
            "type": "properties",
            "ownership": "server",
            "mappings": [
                {
                    "endpoint": "/uptimeSeconds",
                    "type": "integer",
                    "explicit_timestamp": true,
                    "description": "Hello 你好 안녕하세요"
                }
            ]
        }
        "#;

        let interface = InterfaceJson(IFACE_SPECIAL_CHARS.into());

        let astarte_interface: Interface = interface.try_into().unwrap();

        assert_eq!(
            astarte_interface.get_name(),
            "org.astarte-platform.test.test"
        );
        assert_eq!(astarte_interface.get_version_major(), 1);
    }

    #[tokio::test]
    async fn convert_bad_proto_interface_to_astarte_interface() {
        use astarte_device_sdk::Interface;

        use crate::error::AstarteMessageHubError;
        use crate::types::InterfaceJson;

        const IFACE_BAD: &str = r#"{"#;

        let interface = InterfaceJson(IFACE_BAD.into());

        let astarte_interface_bad_result: Result<Interface, AstarteMessageHubError> =
            interface.try_into();

        assert!(astarte_interface_bad_result.is_err());
    }
}
