# This file is part of Astarte.
#
# Copyright 2024 SECO Mind Srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

FROM rust:1.86-alpine3.20 AS build

RUN mkdir /msg-hub && \
    apk add --no-cache alpine-sdk musl-dev pkgconfig \
    openssl-dev openssl-libs-static \
    sqlite-static sqlite-dev

COPY . /msg-hub

WORKDIR /msg-hub

RUN cargo build --release

FROM alpine:3.21

RUN mkdir -p /usr/share/message-hub/astarte-interfaces \
    /var/lib/message-hub/interfaces

COPY scripts/docker/entrypoint.sh /entrypoint.sh

COPY --from=build /msg-hub/target/release/astarte-message-hub /usr/bin/

CMD ["/entrypoint.sh"]
