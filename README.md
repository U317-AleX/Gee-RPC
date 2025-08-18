# Gee-RPC

**Gee-RPC** is a lightweight RPC (Remote Procedure Call) framework.

## Features

* **Basic RPC functionality:** Includes both an RPC server and an RPC client.
* **Server-side service registration:** Allows for easy registration of services on the server.
* **Customizable encoder/decoder:** Supports custom protocols for encoding and decoding data.
* **Flexible connection methods:** Establishes connections by hijacking TCP ports from HTTP, or directly via TCP or Unix domain sockets.
* **Load balancing:** Implements two load balancing algorithms: **Random** and **Round Robin**.
* **Service registry and discovery:** Supports a dedicated service registry for discovery.
