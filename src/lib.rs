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
}

#[cfg(test)]
mod test {
    use crate::proto_message_hub::{
        astarte_message::Payload, AstarteDataType, AstarteMessage, AstarteUnset,
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
}
