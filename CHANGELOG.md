<!--
This file is part of Astarte.

Copyright 2023-2026 SECO Mind Srl

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
-->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Configuration files will read and merged based on their priority [#463]
- Read the files from the `config.d` directories [#463]
- Read the files from the user configuration directory [#463]

### Changed

- Update the MSRV to 1.86 and edition 2024
  [#430](https://github.com/astarte-platform/astarte-message-hub/pull/430)
- The dynamic configuration needs to be enable manually via config, cli or env [#436]
- You can start the HTTP or gRPC config server separately [#436]
- The default store directory is no longer the current working directory but the user data directory
  [#463]

[#436]: https://github.com/astarte-platform/astarte-message-hub/pull/436

## [0.9.2] - 2026-01-29

### Fixed

- Cancel tasks on shutdown signal received.
  [#427](https://github.com/astarte-platform/astarte-message-hub/pull/427)

## [0.9.1] - 2026-01-15

### Changed

- Bump Astarte Device SDK to 0.11.2.

## [0.9.0] - 2025-12-19

### Added

- Add `security-events` feature.
  [#418](https://github.com/astarte-platform/astarte-message-hub/pull/418)

## [0.8.3] - 2025-12-18

### Changed

- Bump Astarte Device SDK to 0.10.5.

## [0.7.6] - 2025-11-14

### Changed

- Bump Astarte Device SDK to 0.9.10.

## [0.8.2] - 2025-10-13

### Fixed

- Crypto provider install.

## [0.8.1] - 2025-09-19

### Changed

- Forward port release v0.7.5.
- Bump Astarte Device SDK to 0.10.4.

## [0.7.5] - 2025-09-19

### Changed

- Bump Astarte Device SDK to 0.9.9.

## [0.8.0] - 2025-07-17

### Added

- Configure the size limit of the database used by the astarte device sdk
  [#372](https://github.com/astarte-platform/astarte-message-hub/pull/372).

### Changed

- Reflect proto changes to retrieve the properties
  [#336](https://github.com/astarte-platform/astarte-message-hub/pull/336).

### Fixed

- Let a node receive only the properties in its introspection
  [#335](https://github.com/astarte-platform/astarte-message-hub/pull/335).

## [0.7.4] - 2025-07-02

### Added

- Introduce feature `vendored`, `bindgen` and `cross` to vendor, use `aws_lc` as default crypto
  provider and cross compile
  [#363](https://github.com/astarte-platform/astarte-message-hub/pull/363).
- Implement methods to retrieve Astarte properties
  [#329](https://github.com/astarte-platform/astarte-message-hub/pull/329).

### Changed

- Bump MSRV to 1.78.0.

## [0.7.3] - 2025-05-29

### Added

- Add the `pairing_url` configuration option to the cmdline `--pairing-url` and environment variable
  `MSGHUB_PAIRING_URL`. [#334](https://github.com/astarte-platform/astarte-message-hub/pull/334)

## [0.7.2] - 2025-03-07

### Changed

- Bump Astarte Device SDK to 0.9.6 release.
  [#320](https://github.com/astarte-platform/astarte-message-hub/pull/320)

## [0.7.1] - 2025-02-28

### Changed

- Bump Astarte Device SDK to 0.9.4 release.

## [0.6.2] - 2025-02-27

### Changed

- Bump Astarte Device SDK to 0.8.5 release.

## [0.7.0] - 2024-11-04

### Added

- Provide support to dynamically add or remove interfaces from a Node introspection
  [#241](https://github.com/astarte-platform/astarte-message-hub/pull/241)
- Create a `Dockerfile` to build the message hub as a container [#268]
- Add `-c` and `--config` flags as an alternative to `toml` for providing the configuration file
  path. [#268]
- Add the `host` and `port` cli configuration options. [#268]
- Add the optional `grpc_socket_host` option for the HTTP, gRPC and File configurations. [#268]
- Handle the `SIGINT` and `SIGTERM` to shutdown the gRPC server. [#268]

### Changed

- Retrieve Node information from metadata to detach a node, now the `detach` rpc is called with
  `Empty` [#251](https://github.com/astarte-platform/astarte-message-hub/pull/251)
- Print a warning when the `-t/--toml` flag is used, while still accepting it, and require it to be
  a well formed path. [#268]
- Make the `grpc_socket_port` optional for the HTTP, gRPC and File configurations. [#268]
- Default to `127.0.0.1:50051` if no host and port is configured. [#268]
- Handle the new return type of the Attach rpc, `MessageHubEvent`, which can either be an error or
  an Astarte message. [#264]
- Retrieve the Node ID information from the grpc metadata also for the Attach rpc. [#264]
- Update Astarte Device Sdk to 0.9.2 release.

[#264]: https://github.com/astarte-platform/astarte-message-hub/pull/264
[#268]: https://github.com/astarte-platform/astarte-message-hub/pull/268

## [0.6.1] - 2024-06-04

## [0.5.4] - 2024-05-24

### Fixed

- Update sdk dependency to fix a purge property bug
  [#341](https://github.com/astarte-platform/astarte-device-sdk-rust/issues/341)

## [0.6.0] - 2024-05-09

### Changed

- Update Astarte Device Sdk to 0.8.1 release.
- Bump MSRV to 1.72.0.
- Introduce Node ID check for gRPC metadata.
- Send all server properties in the node introspection on attach.
  [#244](https://github.com/astarte-platform/astarte-message-hub/pull/244)

## [0.5.3] - 2024-01-31

### Added

- Option to configure the timeout and keep alive interval for the MQTT connection to astarte.

## [0.5.2] - 2023-07-03

### Added

- Add support to receive `device_id` option from dbus.

### Changed

- Make `device_id` options as optional field.

## [0.5.1] - 2023-04-26

## [0.5.0] - 2023-04-21

### Added

- Initial Astarte Message Hub release.
