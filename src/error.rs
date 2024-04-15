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

use std::path::PathBuf;

use astarte_device_sdk::transport::grpc::convert::MessageHubProtoError;
use thiserror::Error;

use crate::config::http::HttpError;

/// A list specifying general categories of Astarte Message Hub error.
#[derive(Error, Debug)]
pub enum AstarteMessageHubError {
    /// An infallible error
    #[error(transparent)]
    Infallible(#[from] std::convert::Infallible),

    /// Failed to convert between types
    #[error("unable to convert type")]
    ConversionError,

    /// Error returned by the Astarte SDK
    #[error(transparent)]
    AstarteError(#[from] astarte_device_sdk::error::Error),

    /// Invalid date
    #[error("{0}")]
    AstarteInvalidData(String),

    /// Wrapper for an io error
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    /// Unrecoverable error
    #[error("unrecoverable error ({0})")]
    FatalError(String),

    /// Invalid configuration file
    #[error("configuration file error")]
    ConfigFileError(#[from] toml::de::Error),

    /// Fail while sending or receiving data
    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),

    /// Error returned by Zbus
    #[error(transparent)]
    ZbusError(#[from] zbus::Error),

    /// Http server error
    #[error("HTTP server error, {0}")]
    HttpServer(#[from] HttpError),

    /// Wrapper for integer conversion errors
    #[error("couldn't convert timestamp, {0}")]
    Timestamp(&'static str),

    /// Astarte Message Hub proto error
    #[error("Astarte Message Hub proto error, {0}")]
    Proto(#[from] MessageHubProtoError),
}

/// Reason why a configuration is invalid.
#[derive(Error, Debug)]
pub enum ConfigValidationError {
    /// Missing required field in the configuration file
    #[error("{0} field is missing")]
    MissingField(&'static str),
    /// Missing both the pairing token and the credentials secret
    #[error("either the pairing token or credential secret must be provided")]
    MissingPairingAndCredentials,
    /// The provided interface path is not a directory
    #[error("interface path {0:?} is not a directory")]
    InvalidInterfaceDirectory(Option<PathBuf>),
}
