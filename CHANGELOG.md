# Changelog

All notable changes to this project will be documented in this file.

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