# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.6.2] - Unreleased

### Changed

- Bump Astarte Device SDK to 0.8.5 release.

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
