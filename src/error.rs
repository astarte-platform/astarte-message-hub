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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum AstarteMessageHubError {
    #[error("")]
    Infallible(#[from] std::convert::Infallible),

    #[error("")]
    TryFromIntError(#[from] core::num::TryFromIntError),

    #[error("Unable to convert type")]
    ConversionError,

    #[error(transparent)]
    AstarteError(#[from] astarte_device_sdk::AstarteError),

    #[error("{0}")]
    AstarteInvalidData(String),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error("unrecoverable error ({0})")]
    FatalError(String),

    #[error("configuration file error")]
    ConfigFileError(#[from] toml::de::Error),
}
