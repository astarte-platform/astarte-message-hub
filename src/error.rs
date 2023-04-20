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

use std::path::PathBuf;

use thiserror::Error;

/// A list specifying general categories of Astarte Message Hub error.
#[derive(Error, Debug)]
pub enum AstarteMessageHubError {
    #[error(transparent)]
    Infallible(#[from] std::convert::Infallible),

    #[error(transparent)]
    TryFromIntError(#[from] core::num::TryFromIntError),

    #[error("Unable to convert type")]
    ConversionError,

    #[error(transparent)]
    AstarteError(#[from] astarte_device_sdk::AstarteError),

    #[error(transparent)]
    AstarteOptionsError(#[from] astarte_device_sdk::options::AstarteOptionsError),

    #[error("{0}")]
    AstarteInvalidData(String),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error("unrecoverable error ({0})")]
    FatalError(String),

    #[error("configuration file error")]
    ConfigFileError(#[from] toml::de::Error),

    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),
}

/// A macro to simplify the creation of a `Result` with an `AstarteMessageHubError` error type.
#[macro_export(crate)]
macro_rules! ensure {
    ($cond:expr, $err:expr) => {
        if !($cond) {
            return Err($err);
        }
    };
}
/// Reason why a configuration is invalid.
#[derive(Error, Debug)]
pub enum ConfigValidationError {
    #[error("{0} field is missing")]
    MissingField(&'static str),
    #[error("either the pairing token or credential secret must be provided")]
    MissingPairingAndCredentials,
    #[error("interface path {0:?} is not a directory")]
    InvalidInterfaceDirectory(Option<PathBuf>),
}
