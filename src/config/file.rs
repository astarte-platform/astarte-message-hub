/*
 * This file is part of Edgehog.
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

use crate::{config::MessageHubOptions, error::AstarteMessageHubError};
use std::path::Path;

pub(crate) fn read_options(path: &Path) -> Result<MessageHubOptions, AstarteMessageHubError> {
    let config_raw = std::fs::read_to_string(path)?;
    let config = toml::from_str::<MessageHubOptions>(&config_raw)?;
    Ok(config)
}
