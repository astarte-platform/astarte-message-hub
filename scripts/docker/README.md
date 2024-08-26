<!---
  Copyright 2022 SECO Mind Srl

  SPDX-License-Identifier: Apache-2.0
-->

# Astarte Message Hub Container

This README explains how to build and run the Astarte Message Hub container. Follow the steps below
to set up your environment and get the container running with the appropriate configuration.

## Prerequisites

Ensure you have Docker installed on your machine.

## Build the Container

1. Clone the repository containing the Astarte Message Hub code.
2. Navigate to the root directory of the repository.
3. Run the build script to create the Docker image:
   ```sh
   ./scripts/docker/build.sh
   ```

## Run the Container

To run the container with your configuration file:

1. Ensure you have your configuration file named `config.toml`.
2. Run the Docker container, mounting the configuration file and opening the gRPC port:

```sh
docker run -p 50051:50051 -v /path/to/your/config.toml:/etc/astarte-message-hub/config.toml astarte-message-hub:latest
```

Replace `/path/to/your/config.toml` with the actual path to your configuration file.

### ENV variables

You can configure the Message Hub with environment variables by exporting them (e.g. configuring
them in the
[docker-compose.yaml](https://docs.docker.com/compose/environment-variables/set-environment-variables/))
or via the `--env-file` CLI options:

```sh
docker run -p 50051:50051 --env-file .env astarte-message-hub:latest
```

Consult the `--help` for a full list of environment variable names and options.
