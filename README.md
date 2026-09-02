# Bale Tunnel

A lightweight TCP-over-Bale‑bot tunnel that turns a bale bot into a SOCKS5 proxy.  
Designed for environments with restricted internet access – the client speaks to the Bale API using ordinary HTTPS, while the server (outside the restricted network) connects to the final destination.

## Features

- SOCKS5 server on `127.0.0.1:1080`
- Real TCP connection from the server to the target
- Streaming with dynamic buffering (max chunk size + inactivity flush)
- Smart LZ4 compression (only when beneficial, skip <1KB and incompressible data)
- Retry & timeout handling 
- Typed shared Bot API client (`shared/src/bot_api.rs`) used by both sides
- **Webhook mode** for the server — Bale pushes updates directly to an axum HTTP endpoint for lower latency; falls back to long polling when not configured
- Workspace-wide strict Clippy lints and unit tests for the wire protocol
- Clean shutdown via `Ctrl+C`
- Works with any Bale bot 

## Architecture (simplified)

Browser → SOCKS5 (client) → (conn_/u_/d_/end_ files) → Bale Bot API

Target Server ← (TCP) ← Server

Server receives updates either via **webhook** (Bale POSTs to axum) or **long polling** (`getUpdates`), configured by `WEBHOOK_BASE_URL`.


## Deploy the server to Render

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/bzamr/bale_tunnel)

Clicking the button deploys the **server** using the `render.yaml` Blueprint (free plan, Docker build via `Dockerfile.server`).
During the setup you will be asked for three values:

| Variable | Value |
|---|---|
| `BALE_SERVER_BOT_TOKEN` | server bot token from [@botfather](https://ble.ir/botfather) |
| `BALE_CHAT_ID` | shared channel/group ID |
| `WEBHOOK_BASE_URL` | `https://<your-service-name>.onrender.com` |

The **client (SOCKS5 proxy) still runs locally** — see [Running](#running) and [Using the SOCKS5 proxy](#using-the-socks5-proxy).

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
git clone https://github.com/bzamr/bale_tunnel.git
cd bale_tunnel
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

### Webhook mode (server)
To use webhook mode instead of long polling, set these on the server:
```bash
WEBHOOK_BASE_URL=https://your-server.com:8443  # must be HTTPS, publicly reachable
WEBHOOK_PATH=/webhook                            # default
SERVER_PORT=8080                                  # default (Render sets SERVER_PORT=10000)
```
When `WEBHOOK_BASE_URL` is not set, the server falls back to long polling automatically. For local development, use a tunnel tool like ngrok. For a hosted setup, use the [Deploy to Render](#deploy-the-server-to-render) button — it configures all of this for you.

## Running
On the server (outside):

```bash
cargo run --release -p server
```

On the client (inside):

```bash
cargo run --release -p client
```
Both will start polling the Bale API (or, for the server, receiving webhooks if configured). Once both are running, you can use the SOCKS5 proxy.

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
├── server/                 # Polling loop / webhook server + TCP connection to final target
│   ├── src/                # main.rs, config.rs, session_manager.rs, streamer.rs, webhook.rs
│   └── Cargo.toml
├── shared/                 # Common types, Bot API client, filename parsing, compression, chunk header
│   └── src/                # lib.rs, types.rs, protocol.rs, compression.rs, bot_api.rs
├── Dockerfile.server         # Server container image (multi-stage Rust build)
├── Dockerfile.client          # Client container image (multi-stage Rust build)
├── docker-compose.yml         # server / client / both profiles
├── render.yaml                # Render Blueprint (one-click server deploy)
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
