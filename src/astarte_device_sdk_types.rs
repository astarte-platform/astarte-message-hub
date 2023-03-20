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

use chrono::DateTime;

use astarte_device_sdk::types::AstarteType;

use crate::error::AstarteMessageHubError;
use crate::proto_message_hub::astarte_data_type_individual::IndividualData;

/// This macro can be used to implement the TryFrom trait for the AstarteType from one or more of
/// the protobuf types.
macro_rules! impl_individual_data_to_astarte_type_conversion_traits {
    (scalar $($typ:ident, $astartedatatype:ident),*; vector $($arraytyp:ident, $astartearraydatatype:ident),*) => {
        impl TryFrom<IndividualData> for AstarteType {
            type Error = AstarteMessageHubError;
            fn try_from(d: IndividualData) -> Result<Self, Self::Error> {
                match d {
                    $(
                    IndividualData::$typ(val) => Ok(AstarteType::$astartedatatype(val.try_into()?)),
                    )?
                    $(
                    IndividualData::$arraytyp(val) => Ok(AstarteType::$astartearraydatatype(val.values.try_into()?)),
                    )?
                    IndividualData::AstarteDateTimeArray(val) => {
                        let mut times: Vec<DateTime<chrono::Utc>> = vec![];
                        for time in val.values.iter() {
                            times.push(time.clone().try_into()?);
                        }
                        Ok(AstarteType::DateTimeArray(times))
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

#[cfg(test)]
mod test {
    use astarte_device_sdk::types::AstarteType;
    use chrono::{DateTime, Utc};

    use crate::proto_message_hub::astarte_data_type_individual::IndividualData;

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
}
