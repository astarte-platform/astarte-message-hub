# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Implement methods to retrieve Astarte properties
  [#329](https://github.com/astarte-platform/astarte-message-hub/pull/329)

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
