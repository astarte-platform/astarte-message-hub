<!--
This file is part of Astarte.

Copyright 2023, 2026 SECO Mind Srl

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

# Message hub client example

This example shows how a simple rust client can be implemented for communication with the
message hub.

The protobuf client will connect to the message hub server and publish on a device-owned interface
periodically. The published data will be a value corresponding to the uptime of the client,
together with the UUID of the node sending the data.
It will also accept data published on server-owned interfaces and print to screen the received
values.

**N.B.** Make sure you satisfy the [common prerequisites](./../README.md#common-prerequisites) and
have completed the [common configuration](./../README.md#common-configuration) before starting this
example.

## Start the example client(s)

The following command can be used to start a single client.

```
cargo run --example client -- <UUID>
```

Run this command multiple times in separate terminals to start multiple clients. And attach them as
nodes to the message hub server.
For ease of implementation, all the clients will register the same interfaces. While, an unique
`<UUID>` should be passed to each client.

## Check published data from the client(s)

We can now check that the data published by our clients has been correctly received and stored in
the Astarte cluster.

Let's use `get-samples` as follows:

```
astartectl appengine --appengine-url http://api.astarte.localhost/appengine --realm-key <REALM>_private.pem \
    --realm-name <REALM> devices get-samples <DEVICE_ID>                                    \
    org.astarte-platform.rust.examples.datastream.DeviceDatastream <ENDPOINT> -c 10
```

Where `<REALM>` is your realm's name, `<DEVICE_ID>` is the device ID from which the data has
been received and `<ENDPOINT>` is the endpoint we want do observe.

This will print the latest published data from the device that has been stored in the Astarte Cloud
instance.
To print a longer history of the published data change the number in `-c 10` to the history length
of your liking.

## Receive data from the server published with the client

With `send-data` we can publish new values on a server-owned interface of our local Astarte
instance.
The syntax is the following:

```
astartectl appengine --appengine-url http://api.astarte.localhost/appengine            \
    --realm-management-url http://api.astarte.localhost/realmmanagement                \
    --realm-key <REALM>_private.pem --realm-name <REALM> devices send-data <DEVICE_ID> \
    org.astarte-platform.rust.examples.datastream.ServerDatastream <ENDPOINT> <VALUE>
```

Where `<REALM>` is your realm name, `<DEVICE_ID>` is the device ID to send the data to,
`<ENDPOINT_TYPE>` is the Astarte type of the chosen endpoint, `<ENDPOINT>` is the endpoint
to send data to and `<VALUE>` is the value to send.

We can observe how all the clients will print out a message with the content of the incoming data
each time we publish some.
Since all of our clients are subscribed to the same interfaces this is to be expected.
