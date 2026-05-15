# Bale Tunnel

A lightweight TCP-over-Bale‑bot tunnel that turns a bale bot into a SOCKS5 proxy.  
Designed for environments with restricted internet access – the client speaks to the Bale API using ordinary HTTPS, while the server (outside the restricted network) connects to the final destination.

## Features

- SOCKS5 server on `127.0.0.1:1080`
- Real TCP connection from the server to the target
- Streaming with dynamic buffering (max chunk size + inactivity flush)
- Smart LZ4 compression (only when beneficial, skip <1KB and incompressible data)
- Retry & timeout handling 
- Clean shutdown via `Ctrl+C`
- Works with any Bale bot 

## Architecture (simplified)

Browser → SOCKS5 (client) → (conn_/u_/d_/end_ files) → Bale Bot API

Target Server ← (TCP) ← Server (polling) 


## Prerequisites

- [Rust](https://rustup.rs/) (edition 2024)
- 2 **Bale bot token** – create a bot via [@botfather](https://ble.ir/botfather) on Bale.
- A **chat ID** (the channel/group where the bot and the client will exchange files).  
  You can obtain it by sending a message to the bot and calling `getUpdates`.
- Two machines / terminals:
  - **Client** (inside restricted network, runs the SOCKS5 proxy)
  - **Server** (outside, with free internet access)

## Installation

```bash
git clone https://github.com/4mir7z/bale-tunnel.git
cd bale-tunnel
# Build both client and server (release)
cargo build --release --workspace
```

## Configuration
Copy the example environment file and edit it.

Set at least:
BALE_CLIENT_BOT_TOKEN 
BALE_SERVER_BOT_TOKEN
BALE_CHAT_ID 

See .env.example for all options.

## Running
On the server (outside):

```bash
cargo run --release -p server
```

On the client (inside):

```bash
cargo run --release -p client
```
Both will start polling the Bale API. Once both are running, you can use the SOCKS5 proxy.

## Using the SOCKS5 proxy
Configure your browser or system to use a SOCKS5 proxy at 127.0.0.1:1080.

Example with curl:
```bash
curl --socks5 127.0.0.1:1080 https://check.torproject.org/
```

## Limitations
- Latency – files are uploaded/downloaded via Bale API, so extra delay (typically 200‑500ms) is added.
- Throughput – limited by Bale’s file upload/download speed (usually 1‑5 MB/s).
- File size – each chunk is up to 1 MiB (configurable). Large transfers produce many API calls.

## Project Structure

```text
bale-tunnel-stream/
├── client/                 # SOCKS5 server + polling loop (receives ack/downstream)
│   ├── src/                # main.rs, config.rs, socks5_server.rs, session_manager.rs, streamer.rs
│   └── Cargo.toml
├── server/                 # Polling loop + TCP connection to final target
│   ├── src/                # main.rs, config.rs, session_manager.rs, stream_handler.rs
│   └── Cargo.toml
├── shared/                 # Common types, filename parsing, compression, header
│   └── src/                # lib.rs, types.rs, protocol.rs, compression.rs
├── .env.example
├── CHANGELOG.md
└── README.md
```

## Building Release Binaries
```bash
# Build both client and server in release mode
cargo build --release --workspace

# The binaries are now at:
#   target/release/bale-tunnel-client
#   target/release/bale-tunnel-server

# Optionally strip them to reduce size
strip target/release/bale-tunnel-client
strip target/release/bale-tunnel-server
```

You can copy them to any machine with the same architecture and run without Rust installed.
