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
use log::info;

pub fn read_options(
    override_config_file_path: Option<String>,
) -> Result<MessageHubOptions, AstarteMessageHubError> {
    let paths = ["message-hub-config.toml", "/etc/message-hub/config.toml"]
        .iter()
        .map(|f| f.to_string());

    let paths = override_config_file_path
        .into_iter()
        .chain(paths)
        .filter(|f| std::path::Path::new(f).exists());

    if let Some(path) = paths.into_iter().next() {
        info!("Found configuration file {path}");
        get_options_from_path(path)
    } else {
        Err(AstarteMessageHubError::FatalError(
            "Configuration file not found".to_string(),
        ))
    }
}

pub(crate) fn get_options_from_path(
    path: String,
) -> Result<MessageHubOptions, AstarteMessageHubError> {
    let config = std::fs::read_to_string(path)?;

    let config = toml::from_str::<MessageHubOptions>(&config)?;

    Ok(config)
}
