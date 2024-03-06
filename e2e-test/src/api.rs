// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, fmt::Debug, str::FromStr};

use astarte_device_sdk::types::AstarteType;
use chrono::{DateTime, Utc};
use eyre::{ensure, eyre};
use reqwest::Url;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;
use tracing::instrument;

use crate::utils::{base64_decode, read_env, Timestamp};

#[derive(Clone)]
pub struct Api {
    ///  Base url plus the realm and device id
    url: Url,
    token: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiResp<T> {
    pub data: T,
}

#[derive(Deserialize)]
struct WithTimestamp<T> {
    #[serde(flatten)]
    value: T,
    #[serde(rename = "timestamp")]
    _timestamp: Option<DateTime<Utc>>,
}

impl Debug for Api {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Api")
            .field(&"url", &self.url)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Deserialize)]
struct IndividualResp {
    value: Value,
}

impl Api {
    fn url(api_url: &str, realm: &str, device_id: &str) -> eyre::Result<Url> {
        let url = Url::parse(&format!("{api_url}/v1/{realm}/devices/{device_id}"))?;

        Ok(url)
    }

    fn new(url: Url, token: String) -> Self {
        Self { url, token }
    }

    pub fn try_from_env() -> eyre::Result<Self> {
        let realm = read_env("E2E_REALM")?;
        let device_id = read_env("E2E_DEVICE_ID")?;
        let api_url = read_env("E2E_API_URL")?;
        let token = read_env("E2E_TOKEN")?;

        let url = Self::url(&api_url, &realm, &device_id)?;

        Ok(Self::new(url, token))
    }

    #[instrument(skip_all)]
    pub async fn interfaces(&self) -> eyre::Result<Vec<String>> {
        let url = format!("{}/interfaces", self.url);

        let res = reqwest::Client::new()
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?;

        let payload: ApiResp<Vec<String>> = res.json().await?;

        Ok(payload.data)
    }

    pub async fn aggregate_value<T>(&self, interface: &str, path: &str) -> eyre::Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let url = format!("{}/interfaces/{interface}", self.url);

        let res = reqwest::Client::new()
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?;

        let mut payload: ApiResp<HashMap<String, Vec<WithTimestamp<T>>>> = res.json().await?;

        payload
            .data
            .remove(path.trim_matches('/'))
            .map(|v| v.into_iter().map(|v| v.value).collect())
            .ok_or_else(|| eyre!("missing {path} in response"))
    }

    pub async fn check_individual(
        &self,
        interface: &str,
        expected: &HashMap<String, AstarteType>,
    ) -> eyre::Result<()> {
        let url = format!("{}/interfaces/{interface}", self.url);

        let res = reqwest::Client::new()
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?;

        let payload: ApiResp<HashMap<String, IndividualResp>> = res.json().await?;

        for (k, exp) in expected {
            let v = &payload
                .data
                .get(k)
                .ok_or_else(|| eyre!("missing endpoint {k}"))?
                .value;

            let check = match exp {
                AstarteType::Double(exp) => v.as_f64().is_some_and(|v| v == *exp),
                AstarteType::Integer(exp) => v.as_i64().is_some_and(|v| v == i64::from(*exp)),
                AstarteType::Boolean(exp) => v.as_bool().is_some_and(|v| v == *exp),
                AstarteType::LongInteger(exp) => v.as_str().is_some_and(|v| v == exp.to_string()),
                AstarteType::String(exp) => v.as_str().is_some_and(|v| v == exp),
                AstarteType::BinaryBlob(exp) => {
                    if let Some(s) = v.as_str() {
                        let blob = base64_decode(s)?;

                        blob == *exp
                    } else {
                        false
                    }
                }
                AstarteType::DateTime(exp) => {
                    if let Some(s) = v.as_str() {
                        let date_time = Timestamp::from_str(s)?;

                        date_time == *exp
                    } else {
                        false
                    }
                }
                AstarteType::DoubleArray(exp) => {
                    let arr: Vec<f64> = serde_json::from_value(v.clone())?;

                    arr == *exp
                }
                AstarteType::IntegerArray(exp) => {
                    let arr: Vec<i32> = serde_json::from_value(v.clone())?;

                    arr == *exp
                }
                AstarteType::BooleanArray(exp) => {
                    let arr: Vec<bool> = serde_json::from_value(v.clone())?;

                    arr == *exp
                }
                AstarteType::LongIntegerArray(exp) => {
                    let arr: Vec<String> = serde_json::from_value(v.clone())?;
                    let arr = arr
                        .into_iter()
                        .map(|v| v.parse())
                        .collect::<Result<Vec<i64>, _>>()?;

                    arr == *exp
                }
                AstarteType::StringArray(exp) => {
                    let arr: Vec<String> = serde_json::from_value(v.clone())?;

                    arr == *exp
                }
                AstarteType::BinaryBlobArray(exp) => {
                    let arr: Vec<String> = serde_json::from_value(v.clone())?;
                    let arr = arr
                        .into_iter()
                        .map(|v| base64_decode(v))
                        .collect::<Result<Vec<_>, _>>()?;

                    arr == *exp
                }
                AstarteType::DateTimeArray(exp) => {
                    let arr: Vec<String> = serde_json::from_value(v.clone())?;
                    let arr = arr
                        .into_iter()
                        .map(|v| Timestamp::from_str(&v))
                        .collect::<Result<Vec<_>, _>>()?;

                    arr == *exp
                }
                AstarteType::Unset => v.is_null(),
            };

            ensure!(check, "expected for key {k} that {exp:?} was equal to {v}");
        }

        Ok(())
    }
}
