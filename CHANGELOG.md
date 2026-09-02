# Changelog

All notable changes to this project will be documented in this file.

## [1.3.0] – 2026-09-02

### Added
- **Containerization**: `Dockerfile.server` and `Dockerfile.client` (multi-stage
  Rust builds) plus a `docker-compose.yml` with `server` / `client` / `both`
  profiles (`docker compose --profile server up --build`).
- **One-click Render deployment**: `render.yaml` Blueprint (web service, Docker
  runtime, free plan) and a *Deploy to Render* button in the README. The server
  runs in webhook mode on Render; the client (SOCKS5) stays local.
- Render service guidance: non-secret env vars are pre-set in the Blueprint,
  secrets (`BALE_SERVER_BOT_TOKEN`, `BALE_CHAT_ID`, `WEBHOOK_BASE_URL`) are
  filled in during the deploy flow.

## [1.2.0] – 2026-09-02

### Added
- **Webhook mode for the server** (`server/src/webhook.rs`): when
  `WEBHOOK_BASE_URL` is set, the server starts an axum HTTP server that receives
  Bale webhook POSTs instead of long-polling via `getUpdates`. Falls back to
  polling when the env var is not set.
- `BotApi::set_webhook` and `BotApi::delete_webhook` methods
  (`shared/src/bot_api.rs`) for registering and removing the webhook with Bale.
- Server config fields: `WEBHOOK_BASE_URL` (optional), `WEBHOOK_PATH` (default
  `/webhook`), `SERVER_PORT` (default `8080`).

### Changed
- Server `main()` now selects between webhook and long-polling mode at startup
  based on `WEBHOOK_BASE_URL`. The polling path and `process_document` are
  unchanged.
- `process_document` visibility widened from private to `pub(crate)` so the
  webhook handler can call it directly.

## [1.1.0] – 2026-09-01

### Added
- Shared `BotApi` module (`shared/src/bot_api.rs`): one typed Bale Bot API client
  (poll, upload with retry, download, delete, cleanup) used by both client and
  server, replacing duplicated polling loops and `serde_json::Value` probing.
- Workspace-level Clippy lints (`pedantic`, `unwrap_used = "deny"`,
  `panic = "deny"`) — zero warnings across the workspace.
- Unit tests for chunk header serialization/deserialization and magic
  validation.

### Fixed
- **Chunk-drop race** (data loss / corrupted TLS streams under load): chunk
  receiver channels are now registered *before* the connection ack is sent or
  awaited, so early chunks can never be dropped.
- `cleanup_old_updates` deleted the wrong messages (passed `chat.id` as
  `message_id`).
- `delete_message` race: processed messages are now deleted after a short
  delay so the peer's `getFile` download can finish first.
- Chunk header magic (`0x424C4554`) is now validated on deserialize — corrupted
  packets are rejected instead of silently reassembled.
- Failed connection attempts no longer leak session resources.
- Compression savings check uses integer arithmetic (no `f64` precision loss).
- Default `INACTIVITY_TIMEOUT_MS` raised from 10 ms to 500 ms (10 ms caused one
  HTTP upload per tiny chunk).

### Changed
- Reduced codebase by ~350 lines by deduplicating client/server polling,
  upload, download, and delete logic into `shared`.
- Per-message `Config` clones removed; expected self-echo logging downgraded
  to debug level.

## [1.0.0] – 2025-05-15

### Added
- Initial release.
- SOCKS5 server in client using `fast-socks5`.
- Real TCP connection from server to target.
- Streaming with dynamic buffering (max chunk size + inactivity timeout).
- Smart LZ4 compression (only when beneficial, ≥5% saving).
- Retry for upstream chunks (3 attempts, exponential backoff).
- Idle timeout for downstream (30 seconds).
- Graceful shutdown with `Ctrl+C`.
- Automatic deletion of processed messages .
- Workspace structure: `client`, `server`, `shared` crates.


### Known limitations
- Throughput limited by Bale API.

### Fixed