<!--
Copyright 2023 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Get started using the Astarte message hub

The following examples are available to get you started with the Astarte message hub:
- [client](./client/README.md): shows how to build a simple ProtoBuf client to communicate with a
message hub server.

# Common prerequisites

All the examples above have some common prerequisites:
- A compatible version of the [Rust toolchain](https://www.rust-lang.org/tools/install).
- A local instance of Astarte. See
[Astarte in 5 minutes](https://docs.astarte-platform.org/astarte/latest/010-astarte_in_5_minutes.html)
for a quick way to set up Astarte on your machine.
- The [astartectl](https://github.com/astarte-platform/astartectl/releases) tool. We will use
`astartectl` to manage the Astarte instance.

**N.B.** When installing Astarte using *Astarte in 5 minutes* perform all the installation steps
until right before the *installing the interfaces* step.

## Installing the interfaces on Astarte

An interface can be installed by running the following command:

```
astartectl realm-management                       \
    --realm-management-url http://localhost:4000/ \
    --realm-key <REALM>_private.pem               \
    --realm-name <REALM>                          \
    interfaces install <INTERFACE_FILE_PATH>
```
Where `<REALM>` is the name of the realm, and `<INTERFACE_FILE_PATH>` is the path name to the
`.json` file containing the interface description.
We assume you are running this command from the Astarte installation folder. If you would like to
run it from another location provide the full path to the realm key.

Each example contains an `/interfaces` folder. To run that example install all the interfaces
contained in the `.json` files in that folder.

## Registering a new device on Astarte

To register a new device on Astarte, two separate options are possible.
Manual registration using a tool such `astartectl` to manage Astarte or automatic registration
performed by the message hub requiring a pairing JWT.

### Manual registration

To manually register the device on the Astarte instance you can use the following `astartectl`
command:
```
astartectl pairing                       \
    --pairing-url http://localhost:4003/ \
    --realm-key <REALM>_private.pem      \
    --realm-name <REALM>                 \
    agent register <DEVICE_ID>
```
**NB**: The device id should follow a specific format. See the
[astarte documentation](https://docs.astarte-platform.org/latest/010-design_principles.html#device-id)
for more information regarding accepted values.

**NB**: The credential secret is only shown once during the device registration procedure.

### Automatic registration

To enable the automatic registration we will need to generate a parining JWT from our Astarte local
instance. This token will then be used inthe registration procedure performed internally to the
message hub.

The command to generate a Pairing JWT is:
```
astartectl utils gen-jwt --private-key <REALM>_private.pem pairing --expiry 0
```
This will generate a never expiring token. To generate a token with an expiration date, then change
`--expiry 0` to `--expiry <SEC>` with `<SEC>` the number of seconds your token should last.

## Configuring the message hub server

First, we shall configure the message hub server.
The easier way is to create a `message-hub-config.toml` file in the root of this repository.

A basic template for a manual registered device is the following:
```
realm = "realm name"
device_id = "device id"
credentials_secret = "credentials secret"
pairing_url = "http://localhost:4003"
astarte_ignore_ssl = true
protobuf_port = 50051
```
If you are using the automatic registration for your device, substitute the `credentials_secret`
with the `pairing_token` obrained in the previous step.

## Build and run the message hub server

You can start the configured Astarte message hub server using the following command:
```
cargo run
```

## Device connection to Astarte

Once the server has been started it will automatically attempt to connect the device to the local
instance of Astarte.
After the connection has been accomplished, the following log message should be shown:
```
Connection to Astarte established.
```

You can check the device used by the message hub has been correctly registered and is connected to
the Astarte instance using `astartectl`.
To list the all registered devices run:
```
astartectl appengine --appengine-url http://localhost:4002/ \
    --realm-key <REALM>_private.pem --realm-name <REALM>    \
    devices list
```
You can check the status of a specific device with the command:
```
astartectl appengine --appengine-url http://localhost:4002/ \
    --realm-key <REALM>_private.pem --realm-name <REALM>    \
    devices show <DEVICE_ID>
```
