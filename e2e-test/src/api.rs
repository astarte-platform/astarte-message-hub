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

use astarte_device_sdk::{AstarteData, aggregate::AstarteObject};
use color_eyre::{Section, SectionExt, owo_colors::OwoColorize};
use eyre::{ensure, eyre};
use itertools::Itertools;
use reqwest::{Response, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tracing::{debug, instrument, trace};

use crate::utils::{Timestamp, base64_decode, base64_encode, read_env};

#[derive(Clone)]
pub struct Api {
    ///  Base url plus the realm and device id
    url: Url,
    token: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApiData<T> {
    pub data: T,
}

impl<T> ApiData<T> {
    fn new(data: T) -> Self
    where
        T: Serialize,
    {
        Self { data }
    }
}

#[derive(Debug, Deserialize)]
struct WithTimestamp<T> {
    #[serde(flatten)]
    value: T,
    #[serde(rename = "timestamp")]
    _timestamp: Option<Timestamp>,
}

impl Debug for Api {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Api")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

async fn check_response(url: &str, res: Response) -> eyre::Result<Response> {
    let status = res.status();
    if status.is_client_error() || status.is_server_error() {
        let body: Value = res.json().await?;

        Err(eyre!("HTTP status error ({status}) for url {url}")
            .section(format!("The response body is:\n{body:#}").header("Response:".cyan())))
    } else {
        Ok(res)
    }
}

fn check_astarte_value(data: &AstarteData, value: &Value) -> eyre::Result<bool> {
    let check = match data {
        AstarteData::Double(exp) => value.as_f64().is_some_and(|v| v == *exp),
        AstarteData::Integer(exp) => value.as_i64().is_some_and(|v| v == i64::from(*exp)),
        AstarteData::Boolean(exp) => value.as_bool().is_some_and(|v| v == *exp),
        AstarteData::LongInteger(exp) => value.as_str().is_some_and(|v| v == exp.to_string()),
        AstarteData::String(exp) => value.as_str().is_some_and(|v| v == exp),
        AstarteData::BinaryBlob(exp) => value
            .as_str()
            .map(base64_decode)
            .transpose()?
            .is_some_and(|blob| blob == *exp),
        AstarteData::DateTime(exp) => value
            .as_str()
            .map(Timestamp::from_str)
            .transpose()?
            .is_some_and(|date_time| date_time == *exp),
        AstarteData::DoubleArray(exp) => {
            let arr: Vec<f64> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteData::IntegerArray(exp) => {
            let arr: Vec<i32> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteData::BooleanArray(exp) => {
            let arr: Vec<bool> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteData::LongIntegerArray(exp) => {
            let arr: Vec<String> = serde_json::from_value(value.clone())?;
            let arr = arr
                .into_iter()
                .map(|v| v.parse())
                .collect::<Result<Vec<i64>, _>>()?;

            arr == *exp
        }
        AstarteData::StringArray(exp) => {
            let arr: Vec<String> = serde_json::from_value(value.clone())?;

            arr == *exp
        }
        AstarteData::BinaryBlobArray(exp) => {
            let arr: Vec<String> = serde_json::from_value(value.clone())?;
            let arr = arr
                .into_iter()
                .map(base64_decode)
                .collect::<Result<Vec<_>, _>>()?;

            arr == *exp
        }
        AstarteData::DateTimeArray(exp) => {
            let arr: Vec<String> = serde_json::from_value(value.clone())?;
            let arr = arr
                .into_iter()
                .map(|v| Timestamp::from_str(&v))
                .collect::<Result<Vec<_>, _>>()?;

            arr == *exp
        }
    };

    Ok(check)
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

        let payload: ApiData<Vec<String>> = res.json().await?;

        Ok(payload.data)
    }

    pub async fn aggregate_value<T>(&self, interface: &str, path: &str) -> eyre::Result<Vec<T>>
    where
        T: DeserializeOwned + Debug,
    {
        let url = format!("{}/interfaces/{interface}", self.url);

        let res = reqwest::Client::new()
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await?;

        let res = check_response(&url, res).await?;

        let mut payload: ApiData<HashMap<String, Vec<WithTimestamp<T>>>> = res.json().await?;

        payload
            .data
            .remove(path.trim_matches('/'))
            .map(|v| v.into_iter().map(|v| v.value).collect())
            .ok_or_else(|| {
                eyre!("missing {path} in response").note(format!("Full response {payload:#?}"))
            })
    }

    pub async fn check_individual(
        &self,
        interface: &str,
        expected: &AstarteObject,
    ) -> eyre::Result<()> {
        let payload = self.property(interface).await?;

        for (k, exp) in expected.iter() {
            trace!("checking {k}");

            let v = payload
                .get(k)
                .ok_or_else(|| eyre!("missing endpoint {k}"))?;

            let check = check_astarte_value(exp, v)?;

            ensure!(check, "expected for key {k} that {exp:?} was equal to {v}");
        }

        Ok(())
    }

    pub async fn property(&self, interface: &str) -> Result<HashMap<String, Value>, eyre::Error> {
        let url = format!("{}/interfaces/{interface}", self.url);
        let res = reqwest::Client::new()
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await?;
        let res = check_response(&url, res).await?;
        let payload: ApiData<HashMap<String, Value>> = res.json().await?;

        Ok(payload.data)
    }

    pub async fn send_interface<T>(&self, interface: &str, path: &str, data: T) -> eyre::Result<()>
    where
        T: Serialize,
    {
        let url = format!("{}/interfaces/{interface}/{path}", self.url);

        let res = reqwest::Client::new()
            .post(&url)
            .bearer_auth(&self.token)
            .json(&ApiData::new(data))
            .send()
            .await?;

        check_response(&url, res).await?;

        Ok(())
    }

    #[instrument]
    pub async fn send_individual(
        &self,
        interface: &str,
        path: &str,
        data: &AstarteData,
    ) -> eyre::Result<()> {
        let url = format!("{}/interfaces/{interface}/{path}", self.url);

        let value = match data {
            AstarteData::Double(v) => Value::from(**v),
            AstarteData::Integer(v) => Value::from(*v),
            AstarteData::Boolean(v) => Value::from(*v),
            AstarteData::LongInteger(v) => Value::from(*v),
            AstarteData::String(v) => Value::from(v.as_str()),
            AstarteData::BinaryBlob(v) => Value::from(base64_encode(v)),
            AstarteData::DateTime(v) => Value::from(v.to_rfc3339()),
            AstarteData::DoubleArray(v) => Value::from(v.iter().map(|v| **v).collect_vec()),
            AstarteData::IntegerArray(v) => Value::from(v.as_slice()),
            AstarteData::BooleanArray(v) => Value::from(v.as_slice()),
            AstarteData::LongIntegerArray(v) => Value::from(v.as_slice()),
            AstarteData::StringArray(v) => Value::from(v.as_slice()),
            AstarteData::BinaryBlobArray(v) => {
                Value::from(v.iter().map(base64_encode).collect::<Vec<_>>())
            }
            AstarteData::DateTimeArray(v) => {
                Value::from(v.iter().map(|d| d.to_rfc3339()).collect::<Vec<_>>())
            }
        };

        debug!("value {value}");

        let res = reqwest::Client::new()
            .post(&url)
            .bearer_auth(&self.token)
            .json(&ApiData::new(value))
            .send()
            .await?;

        check_response(&url, res).await?;

        Ok(())
    }

    #[instrument]
    pub async fn unset(&self, interface: &str, path: &str) -> eyre::Result<()> {
        let url = format!("{}/interfaces/{interface}/{path}", self.url);

        let res = reqwest::Client::new()
            .delete(&url)
            .bearer_auth(&self.token)
            .send()
            .await?;

        check_response(&url, res).await?;

        Ok(())
    }
}
