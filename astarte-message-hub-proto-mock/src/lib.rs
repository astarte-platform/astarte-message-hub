// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
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

pub use mockall;

use mockall::mock;

mock! {
    pub Streaming<T: 'static> {
        pub async fn message(&mut self) -> Result<Option<T>, tonic::Status>;
    }

    impl<T> std::fmt::Debug for Streaming<T> {
        fn fmt<'a>(&self, f: &mut std::fmt::Formatter<'a>) -> std::fmt::Result;
    }
}

mock! {
    pub MessageHubClient<T: 'static> {
        pub fn with_interceptor<I>(
            inner: T,
            interceptor: I,
        ) -> MockMessageHubClient<tonic::service::interceptor::InterceptedService<T, I>>
        where
            I: 'static;

        pub async fn attach<R>(
            &mut self,
            request: R,
        ) -> std::result::Result<
            tonic::Response<MockStreaming<astarte_message_hub_proto::MessageHubEvent>>,
            tonic::Status,
        >
        where
            R: tonic::IntoRequest<astarte_message_hub_proto::Node> + 'static;

        pub async fn send<R>(
            &mut self,
            request: R,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>
        where
            R: tonic::IntoRequest<astarte_message_hub_proto::AstarteMessage> + 'static;

        pub async fn detach<R>(
            &mut self,
            request: R,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>
        where
            R: tonic::IntoRequest<()> + 'static;

        pub async fn add_interfaces<R>(
            &mut self,
            request: R,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>
        where
            R: tonic::IntoRequest<astarte_message_hub_proto::InterfacesJson> + 'static;

        pub async fn remove_interfaces<R>(
            &mut self,
            request: R,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>
        where
            R: tonic::IntoRequest<astarte_message_hub_proto::InterfacesName> + 'static;

        pub async fn get_properties<R>(
            &mut self,
            request: R,
        ) -> std::result::Result<
            tonic::Response<astarte_message_hub_proto::StoredProperties>,
            tonic::Status,
        >
        where
            R: tonic::IntoRequest<astarte_message_hub_proto::InterfaceName> + 'static;

        pub async fn get_all_properties<R>(
            &mut self,
            request: R,
        ) -> std::result::Result<
            tonic::Response<astarte_message_hub_proto::StoredProperties>,
            tonic::Status,
        >
        where
            R: tonic::IntoRequest<astarte_message_hub_proto::PropertyFilter> + 'static;

        pub async fn get_property<R>(
            &mut self,
            request: R,
        ) -> std::result::Result<tonic::Response<astarte_message_hub_proto::AstartePropertyIndividual>, tonic::Status>
        where
            R: tonic::IntoRequest<astarte_message_hub_proto::PropertyIdentifier> + 'static;
    }

    impl<T> std::clone::Clone for MessageHubClient<T> {
        fn clone(&self) -> Self;
    }

    impl<T> std::fmt::Debug for MessageHubClient<T> {
        fn fmt<'a>(&self, f: &mut std::fmt::Formatter<'a>) -> std::fmt::Result;
    }
}
