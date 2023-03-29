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

pub use crate::astarte_message_hub::AstarteMessageHub;
pub use crate::proto_message_hub::message_hub_server::MessageHubServer;

mod astarte_device_sdk_types;
mod astarte_message_hub;
pub mod config;
mod data;
pub mod error;
mod types;

#[allow(clippy::all)]
pub mod proto_message_hub {
    use self::astarte_data_type::Data;

    tonic::include_proto!("astarteplatform.msghub");

    impl AstarteMessage {
        pub fn take_data(self) -> Option<AstarteDataType> {
            use self::astarte_message::Payload;

            match self.payload {
                Some(Payload::AstarteData(data)) => Some(data),
                _ => None,
            }
        }

        pub fn take_unset(self) -> Option<AstarteUnset> {
            use self::astarte_message::Payload;

            match self.payload {
                Some(Payload::AstarteUnset(unset)) => Some(unset),
                _ => None,
            }
        }

        pub fn data(&self) -> Option<&AstarteDataType> {
            use self::astarte_message::Payload;

            match self.payload {
                Some(Payload::AstarteData(ref data)) => Some(data),
                _ => None,
            }
        }

        pub fn unset(&self) -> Option<&AstarteUnset> {
            use self::astarte_message::Payload;

            match self.payload {
                Some(Payload::AstarteUnset(ref unset)) => Some(unset),
                _ => None,
            }
        }

        pub fn data_mut(&mut self) -> Option<&mut AstarteDataType> {
            use self::astarte_message::Payload;

            match self.payload {
                Some(Payload::AstarteData(ref mut data)) => Some(data),
                _ => None,
            }
        }

        pub fn unset_mut(&mut self) -> Option<&mut AstarteUnset> {
            use self::astarte_message::Payload;

            match self.payload {
                Some(Payload::AstarteUnset(ref mut unset)) => Some(unset),
                _ => None,
            }
        }
    }

    impl AstarteDataType {
        pub fn individual(&self) -> Option<&AstarteDataTypeIndividual> {
            match self.data {
                Some(Data::AstarteIndividual(ref individual)) => Some(individual),
                _ => None,
            }
        }

        pub fn individual_mut(&mut self) -> Option<&mut AstarteDataTypeIndividual> {
            match self.data {
                Some(Data::AstarteIndividual(ref mut individual)) => Some(individual),
                _ => None,
            }
        }

        pub fn take_individual(self) -> Option<AstarteDataTypeIndividual> {
            match self.data {
                Some(Data::AstarteIndividual(individual)) => Some(individual),
                _ => None,
            }
        }

        pub fn object(&self) -> Option<&AstarteDataTypeObject> {
            match self.data {
                Some(Data::AstarteObject(ref object)) => Some(object),
                _ => None,
            }
        }

        pub fn object_mut(&mut self) -> Option<&mut AstarteDataTypeObject> {
            match self.data {
                Some(Data::AstarteObject(ref mut object)) => Some(object),
                _ => None,
            }
        }

        pub fn take_object(self) -> Option<AstarteDataTypeObject> {
            match self.data {
                Some(Data::AstarteObject(object)) => Some(object),
                _ => None,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::proto_message_hub::{
        astarte_data_type::Data, astarte_data_type_individual::IndividualData,
        astarte_message::Payload, AstarteDataType, AstarteDataTypeIndividual,
        AstarteDataTypeObject, AstarteMessage, AstarteUnset,
    };

    #[test]
    fn test_astarte_message_data() {
        let data = AstarteDataType::default();
        let mut message = AstarteMessage {
            payload: Some(Payload::AstarteData(data.clone())),
            ..Default::default()
        };

        // This test also the ergonomics of the methods
        assert!(message.data().is_some());
        assert!(message.data_mut().is_some());
        assert!(message.clone().take_data().is_some());
        assert!(message.unset().is_none());
        assert!(message.unset_mut().is_none());
        assert!(message.clone().take_unset().is_none());

        let res = message.take_data();

        assert_eq!(res, Some(data));
    }

    #[test]
    fn test_astarte_message_unset() {
        let unset = AstarteUnset::default();
        let mut message = AstarteMessage {
            payload: Some(Payload::AstarteUnset(unset.clone())),
            ..Default::default()
        };

        // This test also the ergonomics of the methods
        assert!(message.data().is_none());
        assert!(message.data_mut().is_none());
        assert!(message.clone().take_data().is_none());
        assert!(message.unset().is_some());
        assert!(message.unset_mut().is_some());
        assert!(message.clone().take_unset().is_some());

        let res = message.take_unset();

        assert_eq!(res, Some(unset));
    }

    #[test]
    fn test_astarte_data_type_object() {
        let individual = AstarteDataTypeIndividual {
            individual_data: Some(IndividualData::AstarteDouble(42.)),
        };
        let mut object_data = HashMap::new();
        object_data.insert("foo".to_string(), individual);

        let object = AstarteDataTypeObject { object_data };
        let mut data = AstarteDataType {
            data: Some(Data::AstarteObject(object.clone())),
        };

        // This test also the ergonomics of the methods
        assert!(data.object().is_some());
        assert!(data.object_mut().is_some());
        assert!(data.clone().take_object().is_some());
        assert!(data.individual().is_none());
        assert!(data.individual_mut().is_none());
        assert!(data.clone().take_individual().is_none());

        let res = data.take_object();

        assert_eq!(res, Some(object));
    }

    #[test]
    fn test_astarte_data_type_individual() {
        let individual = AstarteDataTypeIndividual {
            individual_data: Some(IndividualData::AstarteDouble(42.)),
        };
        let mut data = AstarteDataType {
            data: Some(Data::AstarteIndividual(individual.clone())),
        };

        // This test also the ergonomics of the methods
        assert!(data.object().is_none());
        assert!(data.object_mut().is_none());
        assert!(data.clone().take_object().is_none());
        assert!(data.individual().is_some());
        assert!(data.individual_mut().is_some());
        assert!(data.clone().take_individual().is_some());

        let res = data.take_individual();

        assert_eq!(res, Some(individual));
    }
}
