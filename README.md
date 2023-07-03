<!---
  Copyright 2022 SECO Mind Srl

  SPDX-License-Identifier: Apache-2.0
-->

# Astarte Message Hub

[![Crates.io](https://img.shields.io/crates/v/astarte-message-hub)](https://crates.io/crates/astarte-message-hub)
[![docs.rs](https://img.shields.io/docsrs/astarte-message-hub)](https://docs.rs/astarte-message-hub/)
[![CI](https://github.com/astarte-platform/astarte-message-hub/actions/workflows/build.yaml/badge.svg?branch=master)](https://github.com/astarte-platform/astarte-message-hub/actions/workflows/build.yaml?branch=master)
[![codecov](https://codecov.io/gh/astarte-platform/astarte-message-hub/branch/master/graph/badge.svg)](https://app.codecov.io/gh/astarte-message-hub)
[![LICENSE](https://img.shields.io/github/license/astarte-platform/astarte-message-hub)](./LICENSE)

A central service that runs on (Linux) devices for collecting and delivering messages from N apps
using 1 MQTT connection to Astarte.

## Documentation

- [Astarte Message Hub Architecture](https://github.com/astarte-platform/astarte-message-hub/blob/master/docs/ARCHITECTURE.md)
- [Astarte Documentation](https://docs.astarte-platform.org/latest/001-intro_user.html)

## Requirements

- protobuf >= 3.15
- Rust version >= 1.59

## Configuration

The Astarte Message Hub is configured through `message-hub-config.toml` in the current working
directory, otherwise the system wide `/etc/message-hub/config.toml` can be used. In alternative, you
can specify the path to the configuration file with the `-t/--toml` cli option.

The format for the configuration file is the following:

```toml
##
# Required fields
#
realm = "<REALM>"
pairing_url = "<PAIRING_URL>"
grpc_socket_port = 0 # Required, 0 is only a placeholder

##
# Optional fields
#
interfaces_directory = "[INTERFACES_DIRECTORY]"
# Device id, if not provided it will be retrieved from `io.edgehog.Device` dbus-service
device_id = "[DEVICE_ID]"
# Used to register a device and obtain a `credentials_secret`
pairing_token = "[PAIRING_TOKEN]"
# Credential secret, if not provided the `pairing_token` is required
credentials_secret = "[CREDENTIALS_SECRET]"
# Ignore SSL errors, defaults to false
astarte_ignore_ssl = false
# Path to store persistent data, defaults to "./"
store_directory = "<STORE_PAHT>"
```

An example configuration file can be found in the
[examples](https://github.com/astarte-platform/astarte-message-hub/blob/master/examples/message-hub-config.toml)
direction.

## Example

Have a look at the
[examples](https://github.com/astarte-platform/astarte-message-hub/blob/master/examples/) for an
usage example showing how to send and receive data.
