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

use std::collections::HashMap;
use std::time::SystemTime;

use chrono::{DateTime, Utc};

use crate::proto_message_hub::astarte_data_type::Data::{AstarteIndividual, AstarteObject};
use crate::proto_message_hub::astarte_data_type_individual::IndividualData;
use crate::proto_message_hub::{
    AstarteBinaryBlobArray, AstarteBooleanArray, AstarteDataType, AstarteDataTypeIndividual,
    AstarteDataTypeObject, AstarteDateTimeArray, AstarteDoubleArray, AstarteIntegerArray,
    AstarteLongIntegerArray, AstarteStringArray,
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

impl From<DateTime<Utc>> for AstarteDataTypeIndividual {
    fn from(value: DateTime<Utc>) -> Self {
        let system_time: SystemTime = value.into();

        AstarteDataTypeIndividual {
            individual_data: Some(IndividualData::AstarteDateTime(system_time.into())),
        }
    }
}

impl From<Vec<DateTime<Utc>>> for AstarteDataTypeIndividual {
    fn from(values: Vec<DateTime<Utc>>) -> Self {
        use prost_types::Timestamp;
        AstarteDataTypeIndividual {
            individual_data: Some(IndividualData::AstarteDateTimeArray(AstarteDateTimeArray {
                values: values
                    .iter()
                    .map(|x| {
                        let system_time: SystemTime = x.clone().into();
                        system_time.into()
                    })
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

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::proto_message_hub::astarte_data_type::Data::AstarteIndividual;
    use crate::proto_message_hub::astarte_data_type_individual::IndividualData;
    use crate::proto_message_hub::AstarteDataType;
    use crate::proto_message_hub::AstarteDataTypeIndividual;

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
        use std::time::SystemTime;

        let expected_datetime_value = Utc::now();

        let datetime_astarte_data_type: AstarteDataType = expected_datetime_value.clone().into();

        if let AstarteIndividual(data) = datetime_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteDateTime(date_time_value) = data.individual_data.unwrap()
            {
                let expected_sys_time: SystemTime = expected_datetime_value.into();
                let sys_time: SystemTime = date_time_value.try_into().unwrap();
                assert_eq!(expected_sys_time, sys_time);
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
        use std::time::SystemTime;

        let expected_vec_datetime_value = vec![Utc::now(), Utc::now()];
        let vec_datetime_astarte_individual_type: AstarteDataTypeIndividual =
            expected_vec_datetime_value.clone().into();

        if let IndividualData::AstarteDateTimeArray(vec_datetime_value) =
            vec_datetime_astarte_individual_type
                .individual_data
                .unwrap()
        {
            for i in 0..expected_vec_datetime_value.len() {
                let system_time: SystemTime = vec_datetime_value
                    .values
                    .get(i)
                    .unwrap()
                    .clone()
                    .try_into()
                    .unwrap();

                let date_time: DateTime<Utc> = system_time.try_into().unwrap();
                assert_eq!(expected_vec_datetime_value.get(i).unwrap(), &date_time);
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn datetime_array_into_astarte_data_type_success() {
        use chrono::{DateTime, Utc};
        use std::time::SystemTime;

        let expected_vec_datetime_value = vec![Utc::now(), Utc::now()];
        let vec_datetime_astarte_data_type: AstarteDataType =
            expected_vec_datetime_value.clone().into();

        if let AstarteIndividual(data) = vec_datetime_astarte_data_type.data.unwrap() {
            if let IndividualData::AstarteDateTimeArray(vec_datetime_value) =
                data.individual_data.unwrap()
            {
                for i in 0..expected_vec_datetime_value.len() {
                    let system_time: SystemTime = vec_datetime_value
                        .values
                        .get(i)
                        .unwrap()
                        .clone()
                        .try_into()
                        .unwrap();

                    let date_time: DateTime<Utc> = system_time.try_into().unwrap();
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
}
