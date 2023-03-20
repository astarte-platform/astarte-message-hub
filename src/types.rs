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

use crate::error::AstarteMessageHubError;
use astarte_device_sdk::types::AstarteType;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::proto_message_hub;

/// This macro can be used to implement the from trait for an AstarteDataTypeIndividual from a
/// generic type that is not an array.
macro_rules! impl_type_conversion_traits {
    ( {$( ($typ:ty, $astartedatatype:ident) ,)*}) => {

        $(
               impl From<$typ> for proto_message_hub::AstarteDataTypeIndividual {
                    fn from(d: $typ) -> Self {
                        use proto_message_hub::AstarteDataTypeIndividual;
                        use proto_message_hub::astarte_data_type_individual::IndividualData;

                        AstarteDataTypeIndividual {
                            individual_data: Some(IndividualData::$astartedatatype(d.into())),
                        }
                    }
                }

                impl From<&$typ> for proto_message_hub::AstarteDataTypeIndividual {
                    fn from(d: &$typ) -> Self {
                        use proto_message_hub::AstarteDataTypeIndividual;
                        use proto_message_hub::astarte_data_type_individual::IndividualData;

                        AstarteDataTypeIndividual {
                            individual_data: Some(IndividualData::$astartedatatype(d.clone().into())),
                        }
                    }
                }
        )*
    };
}

/// This macro can be used to implement the from trait for an AstarteDataTypeIndividual from a
/// generic type that is an array.
macro_rules! impl_array_type_conversion_traits {
    ( {$( ($typ:ty, $astartedatatype:ident) ,)*}) => {

        $(
               impl From<$typ> for proto_message_hub::AstarteDataTypeIndividual {
                    fn from(values: $typ) -> Self {
                        use proto_message_hub::AstarteDataTypeIndividual;
                        use proto_message_hub::astarte_data_type_individual::IndividualData;

                        AstarteDataTypeIndividual {
                            individual_data: Some(
                                IndividualData::$astartedatatype(
                                    proto_message_hub::$astartedatatype { values },
                                ),
                            ),
                        }
                    }
                }
        )*
    };
}

impl_type_conversion_traits!({
    (f64, AstarteDouble),
    (i32, AstarteInteger),
    (bool, AstarteBoolean),
    (i64, AstarteLongInteger),
    (&str, AstarteString),
    (String, AstarteString),
    (Vec<u8>, AstarteBinaryBlob),
});

impl_array_type_conversion_traits!({
    (Vec<f64>, AstarteDoubleArray),
    (Vec<i32>, AstarteIntegerArray),
    (Vec<bool>, AstarteBooleanArray),
    (Vec<i64>, AstarteLongIntegerArray),
    (Vec<String>, AstarteStringArray),
    (Vec<Vec<u8>>, AstarteBinaryBlobArray),
});

/// This struct can be used to store the content of a `.json` file.
#[derive(Clone)]
pub struct InterfaceJson(pub Vec<u8>);

impl From<DateTime<Utc>> for proto_message_hub::AstarteDataTypeIndividual {
    fn from(value: DateTime<Utc>) -> Self {
        use proto_message_hub::astarte_data_type_individual::IndividualData;
        use proto_message_hub::AstarteDataTypeIndividual;

        AstarteDataTypeIndividual {
            individual_data: Some(IndividualData::AstarteDateTime(value.into())),
        }
    }
}

impl From<Vec<DateTime<Utc>>> for proto_message_hub::AstarteDataTypeIndividual {
    fn from(values: Vec<DateTime<Utc>>) -> Self {
        use pbjson_types::Timestamp;
        use proto_message_hub::astarte_data_type_individual::IndividualData;
        use proto_message_hub::AstarteDataTypeIndividual;
        use proto_message_hub::AstarteDateTimeArray;

        AstarteDataTypeIndividual {
            individual_data: Some(IndividualData::AstarteDateTimeArray(AstarteDateTimeArray {
                values: values
                    .into_iter()
                    .map(|x| x.into())
                    .collect::<Vec<Timestamp>>(),
            })),
        }
    }
}

impl<T> From<T> for proto_message_hub::AstarteDataType
where
    T: Into<proto_message_hub::AstarteDataTypeIndividual>,
{
    fn from(value: T) -> Self {
        use proto_message_hub::astarte_data_type::Data;
        use proto_message_hub::AstarteDataType;

        AstarteDataType {
            data: Some(Data::AstarteIndividual(value.into())),
        }
    }
}

impl From<HashMap<String, proto_message_hub::AstarteDataTypeIndividual>>
    for proto_message_hub::AstarteDataType
{
    fn from(value: HashMap<String, proto_message_hub::AstarteDataTypeIndividual>) -> Self {
        use proto_message_hub::astarte_data_type::Data;
        use proto_message_hub::AstarteDataType;
        use proto_message_hub::AstarteDataTypeObject;

        AstarteDataType {
            data: Some(Data::AstarteObject(AstarteDataTypeObject {
                object_data: value,
            })),
        }
    }
}

/// Implements the TryFrom trait for the AstarteDataTypeIndividual for any AstarteType.
macro_rules! impl_astarte_type_to_individual_data_conversion_traits {
    ($($typ:ident),*) => {
        impl TryFrom<astarte_device_sdk::types::AstarteType> for proto_message_hub::AstarteDataTypeIndividual {
            type Error = AstarteMessageHubError;
            fn try_from(d: astarte_device_sdk::types::AstarteType) -> Result<Self, Self::Error> {
                use crate::types::AstarteMessageHubError::ConversionError;
                use astarte_device_sdk::types::AstarteType;

                match d {
                    $(
                    AstarteType::$typ(val) => Ok(val.into()),
                    )*
                    AstarteType::Unset => Err(ConversionError)
                }
            }
        }
    }
}

impl_astarte_type_to_individual_data_conversion_traits!(
    Double,
    Integer,
    Boolean,
    LongInteger,
    String,
    BinaryBlob,
    DateTime,
    DoubleArray,
    IntegerArray,
    BooleanArray,
    LongIntegerArray,
    StringArray,
    BinaryBlobArray,
    DateTimeArray
);

impl TryFrom<astarte_device_sdk::AstarteDeviceDataEvent> for proto_message_hub::AstarteMessage {
    type Error = AstarteMessageHubError;

    fn try_from(value: astarte_device_sdk::AstarteDeviceDataEvent) -> Result<Self, Self::Error> {
        use crate::proto_message_hub::astarte_data_type::Data;
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteDataType;
        use crate::proto_message_hub::AstarteDataTypeIndividual;
        use crate::proto_message_hub::AstarteMessage;
        use crate::proto_message_hub::AstarteUnset;
        use astarte_device_sdk::Aggregation;

        let payload: Payload = match value.data {
            Aggregation::Individual(astarte_type) => {
                if let AstarteType::Unset = astarte_type {
                    Payload::AstarteUnset(AstarteUnset {})
                } else {
                    let individual_type: AstarteDataTypeIndividual = astarte_type.try_into()?;
                    Payload::AstarteData(AstarteDataType {
                        data: Some(Data::AstarteIndividual(individual_type)),
                    })
                }
            }
            Aggregation::Object(astarte_map) => {
                let astarte_data: AstarteDataType = astarte_map
                    .into_iter()
                    .map(|(k, v)| (k, v.try_into().unwrap()))
                    .collect::<HashMap<String, AstarteDataTypeIndividual>>()
                    .into();

                Payload::AstarteData(astarte_data)
            }
        };

        Ok(AstarteMessage {
            interface_name: value.interface.clone(),
            path: value.path.clone(),
            timestamp: None,
            payload: Some(payload),
        })
    }
}

/// This function can be used to convert a map of (String, AstarteDataTypeIndividual) into a
/// map of (String, astarte_device_sdk::types::AstarteType).
/// It can be useful when a method accept an astarte_device_sdk::AstarteAggregate.
pub fn map_values_to_astarte_type(
    value: HashMap<String, proto_message_hub::AstarteDataTypeIndividual>,
) -> Result<HashMap<String, astarte_device_sdk::types::AstarteType>, AstarteMessageHubError> {
    let mut map: HashMap<String, AstarteType> = Default::default();
    for (key, astarte_data) in value.into_iter() {
        map.insert(
            key,
            astarte_data
                .individual_data
                .ok_or(AstarteMessageHubError::ConversionError)?
                .try_into()?,
        );
    }
    Ok(map)
}

impl TryFrom<astarte_device_sdk::types::AstarteType>
    for proto_message_hub::astarte_message::Payload
{
    type Error = AstarteMessageHubError;

    fn try_from(
        astarte_device_sdk_type: astarte_device_sdk::types::AstarteType,
    ) -> Result<Self, Self::Error> {
        use crate::proto_message_hub::astarte_data_type::Data::AstarteIndividual;
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteDataType;
        use crate::proto_message_hub::AstarteDataTypeIndividual;

        let payload = if let astarte_device_sdk::types::AstarteType::Unset = astarte_device_sdk_type
        {
            Payload::AstarteUnset(proto_message_hub::AstarteUnset {})
        } else {
            let individual_type: AstarteDataTypeIndividual = astarte_device_sdk_type.try_into()?;
            Payload::AstarteData(AstarteDataType {
                data: Some(AstarteIndividual(individual_type)),
            })
        };
        Ok(payload)
    }
}

impl TryFrom<HashMap<String, AstarteType>> for proto_message_hub::astarte_message::Payload {
    type Error = AstarteMessageHubError;

    fn try_from(value: HashMap<String, AstarteType>) -> Result<Self, Self::Error> {
        use crate::proto_message_hub::astarte_data_type::Data::AstarteObject;
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteDataType;
        use crate::proto_message_hub::AstarteDataTypeObject;

        let astarte_individual_type_map =
            crate::astarte_device_sdk_types::map_values_to_astarte_data_type_individual(value)?;
        let payload = Payload::AstarteData(AstarteDataType {
            data: Some(AstarteObject(AstarteDataTypeObject {
                object_data: astarte_individual_type_map,
            })),
        });
        Ok(payload)
    }
}

#[cfg(test)]
mod test {
    use crate::error::AstarteMessageHubError;
    use crate::proto_message_hub;
    use crate::proto_message_hub::astarte_data_type_individual::IndividualData;
    use crate::proto_message_hub::astarte_message::Payload;
    use crate::proto_message_hub::AstarteDataType;
    use crate::proto_message_hub::AstarteDataTypeIndividual;
    use crate::proto_message_hub::AstarteMessage;
    use astarte_device_sdk::types::AstarteType;
    use astarte_device_sdk::{Aggregation, AstarteDeviceDataEvent};
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;

    #[test]
    fn from_double_to_astarte_individual_type_success() {
        let expected_double_value: f64 = 15.5;
        let d_astarte_individual_type: AstarteDataTypeIndividual =
            AstarteDataTypeIndividual::from(expected_double_value);

        let double_value = d_astarte_individual_type.individual_data.unwrap();

        assert_eq!(
            IndividualData::AstarteDouble(expected_double_value),
            double_value
        );
    }

    #[test]
    fn from_double_by_ref_to_astarte_individual_type_success() {
        let expected_double_value: f64 = 15.5;
        let d_astarte_individual_type: AstarteDataTypeIndividual =
            AstarteDataTypeIndividual::from(&expected_double_value);

        let double_value = d_astarte_individual_type.individual_data.unwrap();

        assert_eq!(
            IndividualData::AstarteDouble(expected_double_value),
            double_value
        );
    }

    #[test]
    fn double_into_astarte_individual_type_success() {
        let expected_double_value: f64 = 15.5;
        let d_astarte_individual_type: AstarteDataTypeIndividual = expected_double_value.into();

        let double_value = d_astarte_individual_type.individual_data.unwrap();

        assert_eq!(
            IndividualData::AstarteDouble(expected_double_value),
            double_value
        );
    }

    #[test]
    fn integer_into_astarte_individual_type_success() {
        let expected_integer_value: i32 = 15;
        let i32_individual_data_type: AstarteDataTypeIndividual = expected_integer_value.into();

        let i32_value = i32_individual_data_type.individual_data.unwrap();

        assert_eq!(
            IndividualData::AstarteInteger(expected_integer_value),
            i32_value
        );
    }

    #[test]
    fn bool_into_astarte_individual_type_success() {
        let expected_bool_value: bool = true;
        let bool_individual_data_type: AstarteDataTypeIndividual = expected_bool_value.into();

        let bool_value = bool_individual_data_type.individual_data.unwrap();

        assert_eq!(
            IndividualData::AstarteBoolean(expected_bool_value),
            bool_value
        );
    }

    #[test]
    fn u8_array_into_individual_type_success() {
        let expected_vec_u8_value: Vec<u8> = vec![10, 44];
        let vec_u8_astarte_individual_type: AstarteDataTypeIndividual =
            expected_vec_u8_value.clone().into();

        let vec_u8_values = vec_u8_astarte_individual_type.individual_data.unwrap();

        assert_eq!(
            IndividualData::AstarteBinaryBlob(expected_vec_u8_value),
            vec_u8_values
        );
    }

    #[test]
    fn double_array_into_individual_type_success() {
        let expected_vec_double_value: Vec<f64> = vec![10.54, 44.99];
        let vec_double_individual_type: AstarteDataTypeIndividual =
            expected_vec_double_value.clone().into();

        let vec_double_values = vec_double_individual_type.individual_data.unwrap();

        assert_eq!(
            IndividualData::AstarteDoubleArray(proto_message_hub::AstarteDoubleArray {
                values: expected_vec_double_value
            }),
            vec_double_values
        );
    }

    #[test]
    fn string_array_into_individual_type_success() {
        let expected_vec_string_value: Vec<String> = vec!["test1".to_owned(), "test2".to_owned()];
        let vec_string_individual_type: AstarteDataTypeIndividual =
            expected_vec_string_value.clone().into();

        let vec_string_values = vec_string_individual_type.individual_data.unwrap();

        assert_eq!(
            IndividualData::AstarteStringArray(proto_message_hub::AstarteStringArray {
                values: expected_vec_string_value
            }),
            vec_string_values
        );
    }

    #[test]
    fn double_into_astarte_data_type_success() {
        let expected_double_value: f64 = 15.5;
        let d_astarte_data_type: AstarteDataType = expected_double_value.into();

        let double_value = d_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteDouble(expected_double_value),
            double_value
        );
    }

    #[test]
    fn integer_into_astarte_data_type_success() {
        let expected_integer_value: i32 = 15;
        let i32_astarte_data_type: AstarteDataType = expected_integer_value.into();

        let i32_value = i32_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteInteger(expected_integer_value),
            i32_value
        );
    }

    #[test]
    fn bool_into_astarte_data_type_success() {
        let expected_bool_value: bool = true;
        let bool_astarte_data_type: AstarteDataType = expected_bool_value.into();

        let bool_value = bool_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteBoolean(expected_bool_value),
            bool_value
        );
    }

    #[test]
    fn longinteger_into_astarte_data_type_success() {
        let expected_longinteger_value: i64 = 15;
        let i64_astarte_data_type: AstarteDataType = expected_longinteger_value.into();

        let i64_value = i64_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteLongInteger(expected_longinteger_value),
            i64_value
        );
    }

    #[test]
    fn string_into_astarte_data_type_success() {
        let expected_string_value: String = "15".to_owned();
        let string_astarte_data_type: AstarteDataType = expected_string_value.clone().into();

        let string_value = string_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteString(expected_string_value),
            string_value
        );
    }

    #[test]
    fn strings_slice_into_astarte_data_type_success() {
        let expected_string_value = "15".to_string();
        let string_astarte_data_type: AstarteDataType = expected_string_value.as_str().into();

        let string_value = string_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteString(expected_string_value),
            string_value
        );
    }

    #[test]
    fn u8_array_into_astarte_data_type_success() {
        let expected_vec_u8_value: Vec<u8> = vec![10, 44];
        let vec_u8_astarte_data_type: AstarteDataType = expected_vec_u8_value.clone().into();

        let vec_u8_values = vec_u8_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteBinaryBlob(expected_vec_u8_value),
            vec_u8_values
        );
    }

    #[test]
    fn datetime_into_astarte_data_type_success() {
        use chrono::Utc;

        let expected_datetime_value = Utc::now();

        let datetime_astarte_data_type: AstarteDataType = expected_datetime_value.into();

        let data = datetime_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        if let IndividualData::AstarteDateTime(date_time_value) = data {
            let resul_date_time: DateTime<Utc> = date_time_value.try_into().unwrap();
            assert_eq!(expected_datetime_value, resul_date_time);
        } else {
            panic!();
        }
    }

    #[test]
    fn double_array_into_astarte_data_type_success() {
        let expected_vec_double_value: Vec<f64> = vec![10.54, 44.99];
        let vec_double_astarte_data_type: AstarteDataType =
            expected_vec_double_value.clone().into();

        let vec_double_values = vec_double_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteDoubleArray(proto_message_hub::AstarteDoubleArray {
                values: expected_vec_double_value
            }),
            vec_double_values
        );
    }

    #[test]
    fn integer_array_into_astarte_data_type_success() {
        let expected_vec_i32_value: Vec<i32> = vec![10, 44];
        let vec_i32_astarte_data_type: AstarteDataType = expected_vec_i32_value.clone().into();

        let vec_i32_values = vec_i32_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteIntegerArray(proto_message_hub::AstarteIntegerArray {
                values: expected_vec_i32_value
            }),
            vec_i32_values
        );
    }

    #[test]
    fn long_integer_array_into_astarte_data_type_success() {
        let expected_vec_i64_value: Vec<i64> = vec![10, 44];
        let vec_i64_astarte_data_type: AstarteDataType = expected_vec_i64_value.clone().into();

        let vec_i64_values = vec_i64_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteLongIntegerArray(proto_message_hub::AstarteLongIntegerArray {
                values: expected_vec_i64_value
            }),
            vec_i64_values
        );
    }

    #[test]
    fn bool_array_into_astarte_data_type_success() {
        let expected_vec_bool_value: Vec<bool> = vec![false, true];
        let vec_bool_astarte_data_type: AstarteDataType = expected_vec_bool_value.clone().into();

        let vec_bool_values = vec_bool_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteBooleanArray(proto_message_hub::AstarteBooleanArray {
                values: expected_vec_bool_value
            }),
            vec_bool_values
        );
    }

    #[test]
    fn string_array_into_astarte_data_type_success() {
        let expected_vec_string_value: Vec<String> = vec!["test1".to_owned(), "test2".to_owned()];
        let vec_string_astarte_data_type: AstarteDataType =
            expected_vec_string_value.clone().into();

        let vec_string_values = vec_string_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteStringArray(proto_message_hub::AstarteStringArray {
                values: expected_vec_string_value
            }),
            vec_string_values
        );
    }

    #[test]
    fn binary_blob_array_into_astarte_data_type_success() {
        let expected_vec_binary_blob_value: Vec<Vec<u8>> = vec![vec![12, 245], vec![78, 11, 128]];
        let vec_binary_blob_astarte_data_type: AstarteDataType =
            expected_vec_binary_blob_value.clone().into();

        let vec_binary_blob_values = vec_binary_blob_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteBinaryBlobArray(proto_message_hub::AstarteBinaryBlobArray {
                values: expected_vec_binary_blob_value
            }),
            vec_binary_blob_values
        );
    }

    #[test]
    fn datetime_array_into_astarte_type_individual_success() {
        use chrono::{DateTime, Utc};

        let expected_vec_datetime_value = vec![Utc::now(), Utc::now()];
        let vec_datetime_astarte_individual_type: AstarteDataTypeIndividual =
            expected_vec_datetime_value.clone().into();

        if let IndividualData::AstarteDateTimeArray(vec_datetime_value) =
            vec_datetime_astarte_individual_type
                .individual_data
                .unwrap()
        {
            for i in 0..expected_vec_datetime_value.len() {
                let date_time: DateTime<Utc> = vec_datetime_value
                    .values
                    .get(i)
                    .unwrap()
                    .clone()
                    .try_into()
                    .unwrap();

                assert_eq!(expected_vec_datetime_value.get(i).unwrap(), &date_time);
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn datetime_array_into_astarte_data_type_success() {
        use chrono::{DateTime, Utc};

        let expected_vec_datetime_value = vec![Utc::now(), Utc::now()];
        let vec_datetime_astarte_data_type: AstarteDataType =
            expected_vec_datetime_value.clone().into();

        let data = vec_datetime_astarte_data_type
            .take_individual()
            .and_then(|data| data.individual_data)
            .unwrap();

        if let IndividualData::AstarteDateTimeArray(vec_datetime_value) = data {
            for i in 0..expected_vec_datetime_value.len() {
                let date_time: DateTime<Utc> = vec_datetime_value
                    .values
                    .get(i)
                    .unwrap()
                    .clone()
                    .try_into()
                    .unwrap();

                assert_eq!(expected_vec_datetime_value.get(i).unwrap(), &date_time);
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn map_into_astarte_data_type_success() {
        use crate::proto_message_hub::AstarteDataTypeIndividual;

        let expected_i32 = 5;
        let expected_f64 = 5.12;
        let mut map_val: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        map_val.insert("i32".to_owned(), expected_i32.into());
        map_val.insert("f64".to_owned(), expected_f64.into());

        let astarte_data_object: AstarteDataType = map_val.into();

        let data = astarte_data_object.object().unwrap();

        let i32_value = data
            .object_data
            .get("i32")
            .and_then(|data| data.individual_data.clone())
            .unwrap();

        assert_eq!(IndividualData::AstarteInteger(expected_i32), i32_value);

        let f64_value = data
            .object_data
            .get("f64")
            .and_then(|data| data.individual_data.clone())
            .unwrap();

        assert_eq!(IndividualData::AstarteDouble(expected_f64), f64_value);
    }

    #[test]
    fn convert_astarte_type_unset_give_conversion_error() {
        let expected_data = AstarteType::Unset;

        let result: Result<AstarteDataTypeIndividual, AstarteMessageHubError> =
            expected_data.try_into();

        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            AstarteMessageHubError::ConversionError
        ));
    }

    #[test]
    fn convert_astarte_device_data_event_unset_to_astarte_message() {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteUnset;
        let expected_data = AstarteType::Unset;

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);
        assert_eq!(
            Payload::AstarteUnset(AstarteUnset {}),
            astarte_message.payload.unwrap()
        );
    }

    fn get_individual_data_from_payload(
        payload: Payload,
    ) -> Result<AstarteType, AstarteMessageHubError> {
        payload
            .take_data()
            .and_then(AstarteDataType::take_individual)
            .and_then(|data| data.individual_data)
            .ok_or(AstarteMessageHubError::ConversionError)?
            .try_into()
    }

    #[test]
    fn convert_astarte_device_data_event_individual_f64_to_astarte_message() {
        let expected_data = AstarteType::Double(10.1);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_i32_to_astarte_message() {
        let expected_data = AstarteType::Integer(10);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_bool_to_astarte_message() {
        let expected_data = AstarteType::Boolean(true);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_i64_to_astarte_message() {
        let expected_data = AstarteType::LongInteger(45);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_string_to_astarte_message() {
        let expected_data = AstarteType::String("test".to_owned());

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_bytes_to_astarte_message() {
        let expected_data = AstarteType::BinaryBlob(vec![12, 48]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_date_time_to_astarte_message() {
        let expected_data = AstarteType::DateTime(Utc::now());

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_f64_array_to_astarte_message() {
        let expected_data = AstarteType::DoubleArray(vec![13.5, 487.35]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_i32_array_to_astarte_message() {
        let expected_data = AstarteType::IntegerArray(vec![78, 45]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_bool_array_to_astarte_message() {
        let expected_data = AstarteType::BooleanArray(vec![true, false, true]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_i64_array_to_astarte_message() {
        let expected_data = AstarteType::LongIntegerArray(vec![658, 77845, 4444]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_string_array_to_astarte_message() {
        let expected_data =
            AstarteType::StringArray(vec!["test1".to_owned(), "test_098".to_string()]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_bytes_array_to_astarte_message() {
        let expected_data = AstarteType::BinaryBlobArray(vec![vec![12, 48], vec![47, 55], vec![9]]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_date_time_array_to_astarte_message() {
        let expected_data = AstarteType::DateTimeArray(vec![Utc::now(), Utc::now()]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_object_to_astarte_message() {
        let expected_map = HashMap::from([
            ("Mercury".to_owned(), AstarteType::Double(0.4)),
            ("Venus".to_owned(), AstarteType::Double(0.7)),
            ("Earth".to_owned(), AstarteType::Double(1.0)),
            ("Mars".to_owned(), AstarteType::Double(1.5)),
        ]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Object(expected_map.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let astarte_object = astarte_message
            .take_data()
            .and_then(|data| data.take_object())
            .unwrap();

        let object_data = astarte_object.object_data;
        for (k, v) in expected_map.into_iter() {
            let astarte_type: AstarteType = object_data
                .get(&k)
                .and_then(|data| data.individual_data.as_ref())
                .and_then(|data| data.clone().try_into().ok())
                .unwrap();

            assert_eq!(v, astarte_type);
        }
    }

    #[test]
    fn convert_astarte_device_data_event_object2_to_astarte_message() {
        let expected_map = HashMap::from([
            ("M".to_owned(), AstarteType::Double(0.4)),
            (
                "V".to_owned(),
                AstarteType::StringArray(vec!["test1".to_owned(), "test2".to_owned()]),
            ),
            ("R".to_owned(), AstarteType::Integer(112)),
            ("a".to_owned(), AstarteType::Boolean(false)),
        ]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Object(expected_map.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let object_data = astarte_message
            .take_data()
            .and_then(|data| data.take_object())
            .unwrap()
            .object_data;

        for (k, v) in expected_map.into_iter() {
            let astarte_type: AstarteType = object_data
                .get(&k)
                .and_then(|data| data.individual_data.as_ref())
                .and_then(|data| data.clone().try_into().ok())
                .unwrap();

            assert_eq!(v, astarte_type);
        }
    }

    #[test]
    fn from_sdk_astarte_type_to_astarte_message_payload_success() {
        let expected_double_value: f64 = 15.5;
        let astarte_sdk_type_double = AstarteType::Double(expected_double_value);

        let payload: Payload = astarte_sdk_type_double.try_into().unwrap();

        let double_value = payload
            .take_data()
            .and_then(AstarteDataType::take_individual)
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteDouble(expected_double_value),
            double_value
        );
    }

    #[test]
    fn from_sdk_astarte_aggregate_to_astarte_message_payload_success() {
        use crate::proto_message_hub::astarte_message::Payload;

        let expected_data: f64 = 15.5;
        use std::collections::HashMap;
        let astarte_type_map = HashMap::from([(
            "key1".to_string(),
            astarte_device_sdk::types::AstarteType::Double(expected_data),
        )]);

        let payload_result: Result<Payload, AstarteMessageHubError> = astarte_type_map.try_into();
        assert!(payload_result.is_ok());

        let double_data = payload_result
            .ok()
            .as_mut()
            .and_then(Payload::data_mut)
            .and_then(AstarteDataType::object_mut)
            .and_then(|data| data.object_data.remove("key1"))
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(IndividualData::AstarteDouble(expected_data), double_data);
    }
}
