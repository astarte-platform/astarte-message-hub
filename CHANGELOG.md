# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
  `Empty` [#251](https://github.com/astarte-platform/astarte-message-hub/pull/251).
- Print a warning when the `-t/--toml` flag is used, while still accepting it, and require it to be
  a well formed path. [#268]
- Make the `grpc_socket_port` optional for the HTTP, gRPC and File configurations. [#268]
- Default to `127.0.0.1:50051` if no host and port is configured. [#268]

[#268]: https://github.com/astarte-platform/astarte-message-hub/pull/268

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
