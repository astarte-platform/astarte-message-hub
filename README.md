<!--
This file is part of Astarte.

Copyright 2022-2026 SECO Mind Srl

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

# Astarte Message Hub

[![Crates.io](https://img.shields.io/crates/v/astarte-message-hub)](https://crates.io/crates/astarte-message-hub)
[![docs.rs](https://img.shields.io/docsrs/astarte-message-hub)](https://docs.rs/astarte-message-hub/)
[![CI](https://github.com/astarte-platform/astarte-message-hub/actions/workflows/ci.yaml/badge.svg?branch=master)](https://github.com/astarte-platform/astarte-message-hub/actions/workflows/ci.yaml?branch=master)
[![codecov](https://codecov.io/gh/astarte-platform/astarte-message-hub/branch/master/graph/badge.svg)](https://app.codecov.io/gh/astarte-message-hub)
[![LICENSE](https://img.shields.io/github/license/astarte-platform/astarte-message-hub)](./LICENSE)

A central service that runs on (Linux) devices for collecting and delivering messages from N apps
using 1 MQTT connection to Astarte.

## Documentation

- [Astarte Message Hub Architecture](https://github.com/astarte-platform/astarte-message-hub/blob/master/docs/ARCHITECTURE.md)
- [Astarte Documentation](https://docs.astarte-platform.org/latest/001-intro_user.html)

## Requirements

- protobuf >= 3.15
- Rust version >= 1.78.0

## Configuration

The Astarte Message Hub is configured through `message-hub-config.toml` in the current working
directory, otherwise the system wide `/etc/message-hub/config.toml` can be used. In alternative, you
can specify the path to the configuration file with the `-c/--config` cli option.

The format for the configuration file is the following:

```toml
##
# Required fields
#
realm = "<REALM>"
# Required to communicate with Astarte.
pairing_url = "<PAIRING_URL>"
# Used to register a device and obtain a `credentials_secret`
pairing_token = "[PAIRING_TOKEN]"
# Credential secret, if not provided the `pairing_token` is required
credentials_secret = "[CREDENTIALS_SECRET]"
# Device id, if not provided it will be retrieved from `io.edgehog.Device` dbus-service
device_id = "[DEVICE_ID]"

##
# Optional fields
#
# Directory containing the JSON interfaces
interfaces_directory = "[INTERFACES_DIRECTORY]"

##
# Other fields, with defaults
#
# Path to store persistent data
store_directory = "./"
# Address the gRPC connection will bind to
grpc_socket_host = "127.0.0.1"
grpc_socket_port = 50051

[astarte]
# Ignore SSL errors
ignore_ssl = false
```

An example configuration file can be found in the
[examples](https://github.com/astarte-platform/astarte-message-hub/blob/master/examples/message-hub-config.toml)
direction.

### Configuration in docker

The message-hub is also distributed as a docker image available in the ghcr.
Using the image you can quickly test out and connect nodes to the message hub.
Remember to expose the grpc port (the default is 50051) if you need to accept
external connections.
You can use docker-compose or run it directly:

Using an already registered device id:
```sh
docker run -e MSGHUB_REALM=realm \
  -e MSGHUB_DEVICE_ID=s6USRuvRSv6Te7S2dVVh3Q \
  -e MSGHUB_PAIRING_URL=https://api.eu1.astarte.cloud/pairing \
  -e MSGHUB_CREDENTIALS_SECRET=your_secret \
  -p 50051 ghcr.io/lucaato/astarte-message-hub:snapshot
```
Replace the environment values with the appropriate ones for your device.

Or by using a pairing token:
```sh
docker run -e MSGHUB_REALM=realm \
  -e MSGHUB_DEVICE_ID=s6USRuvRSv6Te7S2dVVh3Q \
  -e MSGHUB_PAIRING_URL=https://api.eu1.astarte.cloud/pairing \
  -e MSGHUB_PAIRING_TOKEN=your_pairing_token \
  -p 50051 ghcr.io/lucaato/astarte-message-hub:snapshot
```
Replace the environment values with the appropriate ones for your device.

### Override the configuration file

You can override the configuration file options by passing arguments to the command line or
exporting the corresponding environment variables. Run the massage hub with `--help` to see all the
options.

## Example

Have a look at the
[examples](https://github.com/astarte-platform/astarte-message-hub/blob/master/examples/) for an
usage example showing how to send and receive data.
