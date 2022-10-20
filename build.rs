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

fn main() {
    let proto_files = &[
        "proto/astarteplatform/msghub/message_hub_service.proto",
        "proto/astarteplatform/msghub/node.proto",
        "proto/astarteplatform/msghub/interface.proto",
        "proto/astarteplatform/msghub/astarte_message.proto",
        "proto/astarteplatform/msghub/astarte_type.proto",
    ];

    tonic_build::configure()
        .compile_well_known_types(true)
        .type_attribute(
            "AstarteDataTypeIndividual.individual_data",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "AstarteBinaryBlob",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "AstarteDateTime",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "AstarteDoubleArray",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "AstarteIntegerArray",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "AstarteBooleanArray",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "AstarteLongIntegerArray",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "AstarteStringArray",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "AstarteBinaryBlobArray",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "AstarteDateTimeArray",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "IndividualData",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "AstarteDataTypeIndividual",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "AstarteDataTypeObject",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile(proto_files, &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
