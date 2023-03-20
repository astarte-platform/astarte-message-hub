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
        "proto/astarteplatform/msghub/astarte_message.proto",
        "proto/astarteplatform/msghub/astarte_type.proto",
        "proto/astarteplatform/msghub/config.proto",
    ];

    tonic_build::configure()
        .compile_well_known_types(true)
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile(proto_files, &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
