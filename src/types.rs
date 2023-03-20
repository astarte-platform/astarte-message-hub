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
use chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::proto_message_hub::astarte_data_type::Data::{AstarteIndividual, AstarteObject};
use crate::proto_message_hub::astarte_data_type_individual::IndividualData;
use crate::proto_message_hub::{
    AstarteBinaryBlobArray, AstarteBooleanArray, AstarteDataType, AstarteDataTypeIndividual,
    AstarteDataTypeObject, AstarteDateTimeArray, AstarteDoubleArray, AstarteIntegerArray,
    AstarteLongIntegerArray, AstarteMessage, AstarteStringArray,
};

macro_rules! impl_type_conversion_traits {
    ( {$( ($typ:ty, $astartedatatype:ident) ,)*}) => {

        $(
               impl From<$typ> for AstarteDataTypeIndividual {
                    fn from(d: $typ) -> Self {
                        AstarteDataTypeIndividual {
                            individual_data: Some(IndividualData::$astartedatatype(d.into())),
                        }
                    }
                }

                impl From<&$typ> for AstarteDataTypeIndividual {
                    fn from(d: &$typ) -> Self {
                        AstarteDataTypeIndividual {
                            individual_data: Some(IndividualData::$astartedatatype(d.clone().into())),
                        }
                    }
                }
        )*
    };
}

macro_rules! impl_array_type_conversion_traits {
    ( {$( ($typ:ty, $astartedatatype:ident) ,)*}) => {

        $(
               impl From<$typ> for AstarteDataTypeIndividual {
                    fn from(values: $typ) -> Self {
                        AstarteDataTypeIndividual {
                            individual_data: Some(IndividualData::$astartedatatype($astartedatatype{values}))
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

#[derive(Clone)]
pub struct InterfaceJson(pub Vec<u8>);

impl From<DateTime<Utc>> for AstarteDataTypeIndividual {
    fn from(value: DateTime<Utc>) -> Self {
        AstarteDataTypeIndividual {
            individual_data: Some(IndividualData::AstarteDateTime(value.into())),
        }
    }
}

impl From<Vec<DateTime<Utc>>> for AstarteDataTypeIndividual {
    fn from(values: Vec<DateTime<Utc>>) -> Self {
        use pbjson_types::Timestamp;

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

impl<T> From<T> for AstarteDataType
where
    T: Into<AstarteDataTypeIndividual>,
{
    fn from(value: T) -> Self {
        AstarteDataType {
            data: Some(AstarteIndividual(value.into())),
        }
    }
}

impl From<HashMap<String, AstarteDataTypeIndividual>> for AstarteDataType {
    fn from(value: HashMap<String, AstarteDataTypeIndividual>) -> Self {
        AstarteDataType {
            data: Some(AstarteObject(AstarteDataTypeObject { object_data: value })),
        }
    }
}

macro_rules! impl_astarte_type_to_individual_data_conversion_traits {
    ($($typ:ident),*) => {
        impl TryFrom<astarte_device_sdk::types::AstarteType> for AstarteDataTypeIndividual {
            type Error = AstarteMessageHubError;
            fn try_from(d: astarte_device_sdk::types::AstarteType) -> Result<Self, Self::Error> {
                use crate::types::AstarteMessageHubError::ConversionError;

                match d {
                    $(
                    astarte_device_sdk::types::AstarteType::$typ(val) => Ok(val.into()),
                    )*
                    astarte_device_sdk::types::AstarteType::Unset => Err(ConversionError)
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

impl TryFrom<astarte_device_sdk::AstarteDeviceDataEvent> for AstarteMessage {
    type Error = AstarteMessageHubError;

    fn try_from(value: astarte_device_sdk::AstarteDeviceDataEvent) -> Result<Self, Self::Error> {
        use crate::proto_message_hub::astarte_message::Payload;
        use crate::proto_message_hub::AstarteUnset;

        let payload: Payload = match value.data {
            astarte_device_sdk::Aggregation::Individual(astarte_type) => {
                if let astarte_device_sdk::types::AstarteType::Unset = astarte_type {
                    Payload::AstarteUnset(AstarteUnset {})
                } else {
                    let individual_type: AstarteDataTypeIndividual = astarte_type.try_into()?;
                    Payload::AstarteData(AstarteDataType {
                        data: Some(AstarteIndividual(individual_type)),
                    })
                }
            }
            astarte_device_sdk::Aggregation::Object(astarte_map) => {
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

impl TryFrom<InterfaceJson> for astarte_device_sdk::Interface {
    type Error = AstarteMessageHubError;

    fn try_from(interface: InterfaceJson) -> Result<Self, Self::Error> {
        use std::str::FromStr;

        let interface_str = String::from_utf8_lossy(&interface.0);
        astarte_device_sdk::Interface::from_str(interface_str.as_ref()).map_err(|err| {
            AstarteMessageHubError::AstarteError(astarte_device_sdk::AstarteError::InterfaceError(
                err,
            ))
        })
    }
}

/// This function can be used to convert a map of (String, AstarteDataTypeIndividual) into a
/// map of (String, astarte_device_sdk::types::AstarteType).
/// It can be useful when a method accept an astarte_device_sdk::AstarteAggregate.
pub fn map_values_to_astarte_type(
    value: HashMap<String, AstarteDataTypeIndividual>,
) -> Result<HashMap<String, astarte_device_sdk::types::AstarteType>, AstarteMessageHubError> {
    let mut map: HashMap<String, astarte_device_sdk::types::AstarteType> = Default::default();
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

#[cfg(test)]
mod test {
    use crate::error::AstarteMessageHubError;
    use astarte_device_sdk::types::AstarteType;
    use astarte_device_sdk::{Aggregation, AstarteDeviceDataEvent, Interface};
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;

    use crate::proto_message_hub::astarte_data_type::Data::AstarteIndividual;
    use crate::proto_message_hub::astarte_data_type_individual::IndividualData;
    use crate::proto_message_hub::astarte_message::Payload;
    use crate::proto_message_hub::AstarteDataType;
    use crate::proto_message_hub::AstarteDataTypeIndividual;
    use crate::proto_message_hub::AstarteMessage;
    use crate::types::InterfaceJson;

    #[test]
    fn from_double_to_astarte_individual_type_success() {
        let expected_double_value: f64 = 15.5;
        let d_astarte_individual_type: AstarteDataTypeIndividual =
            AstarteDataTypeIndividual::from(expected_double_value);

        if let IndividualData::AstarteDouble(double_value) =
            d_astarte_individual_type.individual_data.unwrap()
        {
            assert_eq!(expected_double_value, double_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn from_double_by_ref_to_astarte_individual_type_success() {
        let expected_double_value: f64 = 15.5;
        let d_astarte_individual_type: AstarteDataTypeIndividual =
            AstarteDataTypeIndividual::from(&expected_double_value);

        if let IndividualData::AstarteDouble(double_value) =
            d_astarte_individual_type.individual_data.unwrap()
        {
            assert_eq!(expected_double_value, double_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn double_into_astarte_individual_type_success() {
        let expected_double_value: f64 = 15.5;
        let d_astarte_individual_type: AstarteDataTypeIndividual = expected_double_value.into();

        if let IndividualData::AstarteDouble(double_value) =
            d_astarte_individual_type.individual_data.unwrap()
        {
            assert_eq!(expected_double_value, double_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn integer_into_astarte_individual_type_success() {
        let expected_integer_value: i32 = 15;
        let i32_individual_data_type: AstarteDataTypeIndividual = expected_integer_value.into();

        if let IndividualData::AstarteInteger(i32_value) =
            i32_individual_data_type.individual_data.unwrap()
        {
            assert_eq!(expected_integer_value, i32_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn bool_into_astarte_individual_type_success() {
        let expected_bool_value: bool = true;
        let bool_individual_data_type: AstarteDataTypeIndividual = expected_bool_value.into();

        if let IndividualData::AstarteBoolean(bool_value) =
            bool_individual_data_type.individual_data.unwrap()
        {
            assert_eq!(expected_bool_value, bool_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn u8_array_into_individual_type_success() {
        let expected_vec_u8_value: Vec<u8> = vec![10, 44];
        let vec_u8_astarte_individual_type: AstarteDataTypeIndividual =
            expected_vec_u8_value.clone().into();

        if let IndividualData::AstarteBinaryBlob(vec_u8_values) =
            vec_u8_astarte_individual_type.individual_data.unwrap()
        {
            assert_eq!(expected_vec_u8_value, vec_u8_values);
        } else {
            panic!();
        }
    }

    #[test]
    fn double_array_into_individual_type_success() {
        let expected_vec_double_value: Vec<f64> = vec![10.54, 44.99];
        let vec_double_individual_type: AstarteDataTypeIndividual =
            expected_vec_double_value.clone().into();

        if let IndividualData::AstarteDoubleArray(vec_double_values) =
            vec_double_individual_type.individual_data.unwrap()
        {
            assert_eq!(expected_vec_double_value, vec_double_values.values);
        } else {
            panic!();
        }
    }

    #[test]
    fn string_array_into_individual_type_success() {
        let expected_vec_string_value: Vec<String> = vec!["test1".to_owned(), "test2".to_owned()];
        let vec_string_individual_type: AstarteDataTypeIndividual =
            expected_vec_string_value.clone().into();

        if let IndividualData::AstarteStringArray(vec_string_values) =
            vec_string_individual_type.individual_data.unwrap()
        {
            assert_eq!(expected_vec_string_value, vec_string_values.values);
        } else {
            panic!();
        }
    }

    #[test]
    fn double_into_astarte_data_type_success() {
        let expected_double_value: f64 = 15.5;
        let d_astarte_data_type: AstarteDataType = expected_double_value.into();

        if let AstarteIndividual(data) = d_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteDouble(double_value) = data.individual_data.unwrap() {
                assert_eq!(expected_double_value, double_value);
            } else {
                panic!();
            }
        } else {
            panic!()
        }
    }

    #[test]
    fn integer_into_astarte_data_type_success() {
        let expected_integer_value: i32 = 15;
        let i32_astarte_data_type: AstarteDataType = expected_integer_value.into();

        if let AstarteIndividual(data) = i32_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteInteger(i32_value) = data.individual_data.unwrap() {
                assert_eq!(expected_integer_value, i32_value);
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn bool_into_astarte_data_type_success() {
        let expected_bool_value: bool = true;
        let bool_astarte_data_type: AstarteDataType = expected_bool_value.into();

        if let AstarteIndividual(data) = bool_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteBoolean(bool_value) = data.individual_data.unwrap() {
                assert_eq!(expected_bool_value, bool_value);
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn longinteger_into_astarte_data_type_success() {
        let expected_longinteger_value: i64 = 15;
        let i64_astarte_data_type: AstarteDataType = expected_longinteger_value.into();

        if let AstarteIndividual(data) = i64_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteLongInteger(i64_value) = data.individual_data.unwrap() {
                assert_eq!(expected_longinteger_value, i64_value);
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn string_into_astarte_data_type_success() {
        let expected_string_value: String = "15".to_owned();
        let string_astarte_data_type: AstarteDataType = expected_string_value.clone().into();

        if let AstarteIndividual(data) = string_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteString(string_value) = data.individual_data.unwrap() {
                assert_eq!(expected_string_value, string_value);
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn strings_slice_into_astarte_data_type_success() {
        let expected_string_value: &str = "15";
        let string_astarte_data_type: AstarteDataType = expected_string_value.into();

        if let AstarteIndividual(data) = string_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteString(string_value) = data.individual_data.unwrap() {
                assert_eq!(expected_string_value, string_value);
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn u8_array_into_astarte_data_type_success() {
        let expected_vec_u8_value: Vec<u8> = vec![10, 44];
        let vec_u8_astarte_data_type: AstarteDataType = expected_vec_u8_value.clone().into();

        if let AstarteIndividual(data) = vec_u8_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteBinaryBlob(vec_u8_values) = data.individual_data.unwrap()
            {
                assert_eq!(expected_vec_u8_value, vec_u8_values);
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn datetime_into_astarte_data_type_success() {
        use chrono::Utc;

        let expected_datetime_value = Utc::now();

        let datetime_astarte_data_type: AstarteDataType = expected_datetime_value.into();

        if let AstarteIndividual(data) = datetime_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteDateTime(date_time_value) = data.individual_data.unwrap()
            {
                let resul_date_time: DateTime<Utc> = date_time_value.try_into().unwrap();
                assert_eq!(expected_datetime_value, resul_date_time);
            } else {
                panic!();
            }
        } else {
            panic!()
        }
    }

    #[test]
    fn double_array_into_astarte_data_type_success() {
        let expected_vec_double_value: Vec<f64> = vec![10.54, 44.99];
        let vec_double_astarte_data_type: AstarteDataType =
            expected_vec_double_value.clone().into();

        if let AstarteIndividual(data) = vec_double_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteDoubleArray(vec_double_values) =
                data.individual_data.unwrap()
            {
                assert_eq!(expected_vec_double_value, vec_double_values.values);
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn integer_array_into_astarte_data_type_success() {
        let expected_vec_i32_value: Vec<i32> = vec![10, 44];
        let vec_i32_astarte_data_type: AstarteDataType = expected_vec_i32_value.clone().into();

        if let AstarteIndividual(data) = vec_i32_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteIntegerArray(vec_i32_values) =
                data.individual_data.unwrap()
            {
                assert_eq!(expected_vec_i32_value, vec_i32_values.values);
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn long_integer_array_into_astarte_data_type_success() {
        let expected_vec_i64_value: Vec<i64> = vec![10, 44];
        let vec_i64_astarte_data_type: AstarteDataType = expected_vec_i64_value.clone().into();

        if let AstarteIndividual(data) = vec_i64_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteLongIntegerArray(vec_i64_values) =
                data.individual_data.unwrap()
            {
                assert_eq!(expected_vec_i64_value, vec_i64_values.values);
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn bool_array_into_astarte_data_type_success() {
        let expected_vec_bool_value: Vec<bool> = vec![false, true];
        let vec_bool_astarte_data_type: AstarteDataType = expected_vec_bool_value.clone().into();

        if let AstarteIndividual(data) = vec_bool_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteBooleanArray(vec_bool_values) =
                data.individual_data.unwrap()
            {
                assert_eq!(expected_vec_bool_value, vec_bool_values.values);
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn string_array_into_astarte_data_type_success() {
        let expected_vec_string_value: Vec<String> = vec!["test1".to_owned(), "test2".to_owned()];
        let vec_string_astarte_data_type: AstarteDataType =
            expected_vec_string_value.clone().into();

        if let AstarteIndividual(data) = vec_string_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteStringArray(vec_string_values) =
                data.individual_data.unwrap()
            {
                assert_eq!(expected_vec_string_value, vec_string_values.values);
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn binary_blob_array_into_astarte_data_type_success() {
        let expected_vec_binary_blob_value: Vec<Vec<u8>> = vec![vec![12, 245], vec![78, 11, 128]];
        let vec_binary_blob_astarte_data_type: AstarteDataType =
            expected_vec_binary_blob_value.clone().into();

        if let AstarteIndividual(data) = vec_binary_blob_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteBinaryBlobArray(vec_binary_blob_values) =
                data.individual_data.unwrap()
            {
                assert_eq!(
                    expected_vec_binary_blob_value,
                    vec_binary_blob_values.values
                );
            } else {
                panic!();
            }
        } else {
            panic!();
        }
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

        if let AstarteIndividual(data) = vec_datetime_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteDateTimeArray(vec_datetime_value) =
                data.individual_data.unwrap()
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
        } else {
            panic!();
        }
    }

    #[test]
    fn map_into_astarte_data_type_success() {
        use crate::proto_message_hub::astarte_data_type::Data::AstarteObject;
        use crate::proto_message_hub::AstarteDataTypeIndividual;

        let expected_i32 = 5;
        let expected_f64 = 5.12;
        let mut map_val: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        map_val.insert("i32".to_owned(), expected_i32.into());
        map_val.insert("f64".to_owned(), expected_f64.into());

        let astarte_data_object: AstarteDataType = map_val.into();

        if let AstarteObject(data) = astarte_data_object.data.unwrap() {
            if let IndividualData::AstarteInteger(i32_value) = data
                .object_data
                .get("i32")
                .unwrap()
                .clone()
                .individual_data
                .unwrap()
            {
                assert_eq!(expected_i32, i32_value);
            } else {
                panic!();
            }
            if let IndividualData::AstarteDouble(f64_value) = data
                .object_data
                .get("f64")
                .unwrap()
                .clone()
                .individual_data
                .unwrap()
            {
                assert_eq!(expected_f64, f64_value);
            } else {
                panic!();
            }
        } else {
            panic!();
        }
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

    fn get_individual_data_from_payload(payload: Payload) -> AstarteType {
        if let Payload::AstarteData(astarte_data) = payload {
            if let AstarteIndividual(astarte_individual_data) = astarte_data.data.unwrap() {
                astarte_individual_data
                    .individual_data
                    .unwrap()
                    .try_into()
                    .unwrap()
            } else {
                panic!()
            }
        } else {
            panic!()
        }
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        assert_eq!(astarte_device_data_event.path, astarte_message.path);
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
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
        let astarte_type = get_individual_data_from_payload(astarte_message.payload.unwrap());
        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_object_to_astarte_message() {
        use crate::proto_message_hub::astarte_data_type::Data::AstarteObject;

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

        if let Payload::AstarteData(astarte_data) = astarte_message.payload.unwrap() {
            if let AstarteObject(astarte_object) = astarte_data.data.unwrap() {
                let object_data = astarte_object.object_data;
                for (k, v) in expected_map.into_iter() {
                    let astarte_type: AstarteType = object_data
                        .get(&k)
                        .unwrap()
                        .individual_data
                        .clone()
                        .unwrap()
                        .try_into()
                        .unwrap();
                    assert_eq!(v, astarte_type);
                }
            } else {
                panic!()
            }
        } else {
            panic!()
        }
    }

    #[test]
    fn convert_astarte_device_data_event_object2_to_astarte_message() {
        use crate::proto_message_hub::astarte_data_type::Data::AstarteObject;

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

        if let Payload::AstarteData(astarte_data) = astarte_message.payload.unwrap() {
            if let AstarteObject(astarte_object) = astarte_data.data.unwrap() {
                let object_data = astarte_object.object_data;
                for (k, v) in expected_map.into_iter() {
                    let astarte_type: AstarteType = object_data
                        .get(&k)
                        .unwrap()
                        .individual_data
                        .clone()
                        .unwrap()
                        .try_into()
                        .unwrap();
                    assert_eq!(v, astarte_type);
                }
            } else {
                panic!()
            }
        } else {
            panic!()
        }
    }

    #[test]
    fn convert_proto_interface_to_astarte_interface() {
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
        const IFACE_BAD: &str = r#"{"#;

        let interface = InterfaceJson(IFACE_BAD.into());

        let astarte_interface_bad_result: Result<Interface, AstarteMessageHubError> =
            interface.try_into();

        assert!(astarte_interface_bad_result.is_err());
    }
}
