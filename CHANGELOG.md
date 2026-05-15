# Changelog

All notable changes to this project will be documented in this file.

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