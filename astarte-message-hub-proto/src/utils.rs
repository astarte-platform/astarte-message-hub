// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Formatter};

use crate::MessageHubError;

impl std::error::Error for MessageHubError {}

impl Display for MessageHubError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)?;

        for s in &self.source {
            write!(f, ": {s}")?;
        }

        Ok(())
    }
}

impl MessageHubError {
    pub fn new<S>(description: S, source: Vec<String>) -> Self
    where
        S: Into<String>,
    {
        Self {
            description: description.into(),
            source,
        }
    }

    pub fn from_error<E>(error: E) -> Self
    where
        E: std::error::Error,
    {
        let description = error.to_string();
        let mut source_vec = vec![];

        // the cause need to be cast as a &dyn Error
        let mut cause: &dyn std::error::Error = &error;
        while let Some(source) = cause.source() {
            cause = source;
            source_vec.push(source.to_string());
        }

        Self {
            description,
            source: source_vec,
        }
    }
}
