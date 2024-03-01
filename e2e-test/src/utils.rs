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

use std::env;

use base64::prelude::*;
use chrono::{DateTime, Utc};
use eyre::Context;
use serde::{
    de::{self, Visitor},
    Deserializer,
};

pub type Timestamp = DateTime<Utc>;

pub fn base64_decode<T>(input: T) -> Result<Vec<u8>, base64::DecodeError>
where
    T: AsRef<[u8]>,
{
    BASE64_STANDARD.decode(input)
}

pub fn timestamp_from_rfc3339(input: &str) -> chrono::ParseResult<Timestamp> {
    DateTime::parse_from_rfc3339(input).map(|d| d.to_utc())
}

pub fn read_env(name: &str) -> eyre::Result<String> {
    env::var(name).wrap_err_with(|| format!("couldn't read environment variable {name}"))
}

pub mod des {
    use super::*;

    struct BlobVisitor;

    impl<'de> Visitor<'de> for BlobVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "expected string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            base64_decode(v).map_err(de::Error::custom)
        }
    }

    struct VecBlobVisitor;

    impl<'de> Visitor<'de> for VecBlobVisitor {
        type Value = Vec<Vec<u8>>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "expected string array")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut items = seq
                .size_hint()
                .map(|size| Vec::with_capacity(size))
                .unwrap_or_default();

            while let Some(v) = seq.next_element::<&str>()? {
                let value = base64_decode(v).map_err(de::Error::custom)?;

                items.push(value);
            }

            Ok(items)
        }
    }

    pub fn deserialize_blob<'de, D>(de: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_str(BlobVisitor)
    }

    pub fn deserialize_blob_vec<'de, D>(de: D) -> Result<Vec<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_seq(VecBlobVisitor)
    }

    struct LongintegerVisitor;

    impl<'de> Visitor<'de> for LongintegerVisitor {
        type Value = i64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "expected string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            v.parse().map_err(de::Error::custom)
        }
    }

    struct VecLongintegerVisitor;

    impl<'de> Visitor<'de> for VecLongintegerVisitor {
        type Value = Vec<i64>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "expected string array")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut items = seq
                .size_hint()
                .map(|size| Vec::with_capacity(size))
                .unwrap_or_default();

            while let Some(v) = seq.next_element::<&str>()? {
                let value = v.parse().map_err(de::Error::custom)?;

                items.push(value);
            }

            Ok(items)
        }
    }

    pub fn deserialize_longinteger<'de, D>(de: D) -> Result<i64, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_str(LongintegerVisitor)
    }

    pub fn deserialize_longinteger_vec<'de, D>(de: D) -> Result<Vec<i64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_seq(VecLongintegerVisitor)
    }
}
