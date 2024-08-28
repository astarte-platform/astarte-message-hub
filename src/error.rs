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
//! Contains the error types used in this crate.

//! Errors for the message hub.

use std::io;
use std::path::PathBuf;

use astarte_device_sdk::interface::error::InterfaceError;
use astarte_device_sdk::introspection::AddInterfaceError;
use astarte_device_sdk::transport::grpc::convert::MessageHubProtoError;
use log::{debug, error};
use tonic::{Code, Status};
use uuid::Uuid;

use crate::astarte::handler::DeviceError;
use crate::config::http::HttpError;
use crate::config::protobuf::ProtobufConfigError;

/// A list specifying general categories of Astarte Message Hub error.
#[derive(thiserror::Error, Debug)]
pub enum AstarteMessageHubError {
    /// Error returned by the Astarte SDK
    #[error(transparent)]
    Astarte(#[from] astarte_device_sdk::error::Error),

    /// Invalid date
    #[error("{0}")]
    AstarteInvalidData(String),

    /// Wrapper for an io error
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Unrecoverable error
    #[error("unrecoverable error ({0})")]
    Fatal(String),

    /// Fail while sending or receiving data
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),

    /// Error returned by Zbus
    #[error(transparent)]
    Zbus(#[from] zbus::Error),

    /// Http server error
    #[error("HTTP server error, {0}")]
    HttpServer(#[from] HttpError),

    /// Protobuf server error
    #[error("Protobuf config error, {0}")]
    ProtobufConfig(#[from] ProtobufConfigError),

    /// Wrapper for integer conversion errors
    #[error("couldn't convert timestamp, {0}")]
    Timestamp(&'static str),

    /// Astarte Message Hub proto error
    #[error("Astarte Message Hub proto error, {0}")]
    Proto(#[from] MessageHubProtoError),

    /// Error returned by  the device
    #[error("error returned by the device")]
    Device(#[from] DeviceError),

    /// Couldn't parse the node ID
    #[error("couldn't parse node id")]
    Uuid(#[from] uuid::Error),

    /// Couldn't find the node id
    #[error("node id not found {0}")]
    NodeId(Uuid),

    /// Failed to parse am Interface
    #[error("failed to parse am Interface")]
    ParseInterface(#[from] InterfaceError),

    /// Failed to add interfaces while building an Astarte device
    #[error("failed to add interfaces while building an Astarte device")]
    AddInterface(#[from] AddInterfaceError),

    /// Couldn't read the configuration
    #[error("coudln't read the configuration")]
    Config(#[from] ConfigError),
}

impl From<AstarteMessageHubError> for Status {
    fn from(value: AstarteMessageHubError) -> Self {
        debug!("error {value:?}");

        let code = match value {
            AstarteMessageHubError::Astarte(_)
            | AstarteMessageHubError::Io(_)
            | AstarteMessageHubError::Fatal(_)
            | AstarteMessageHubError::Config(_)
            | AstarteMessageHubError::Transport(_)
            | AstarteMessageHubError::Zbus(_)
            | AstarteMessageHubError::HttpServer(_)
            | AstarteMessageHubError::ProtobufConfig(_)
            | AstarteMessageHubError::ParseInterface(_)
            | AstarteMessageHubError::AddInterface(_) => Code::Internal,
            AstarteMessageHubError::Device(ref err) => err.into(),
            AstarteMessageHubError::AstarteInvalidData(_)
            | AstarteMessageHubError::Timestamp(_)
            | AstarteMessageHubError::Proto(_)
            | AstarteMessageHubError::Uuid(_)
            | AstarteMessageHubError::NodeId(_) => Code::InvalidArgument,
        };

        Status::new(code, value.to_string())
    }
}

/// Reason why a configuration is invalid.
#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    /// Missing required field in the configuration file
    #[error("{0} field is missing")]
    MissingField(&'static str),
    /// Missing both the pairing token and the credentials secret
    #[error("either the pairing token or credential secret must be provided")]
    Credentials,
    /// The provided interface path is not a directory
    #[error("interface path {0:?} is not a directory")]
    InvalidInterfaceDirectory(Option<PathBuf>),
    /// Couldn't deserialize the configuration file
    #[error("coudln't deserialize the configuration file")]
    Toml(#[from] toml::de::Error),
    /// Couldn't read configuration file
    #[error("couldn't read configuration file")]
    File(#[from] io::Error),
    /// Couldn't read the dynamic generated file.
    #[error("coudldn't read dynamic configuration {0}")]
    Dynamic(PathBuf),
}
