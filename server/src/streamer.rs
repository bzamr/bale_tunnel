use anyhow::Result;
use bytes::BytesMut;
use std::collections::BTreeMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use shared::compression::{decompress, try_compress};
use shared::{deserialize_header, serialize_header, ChunkHeader, ChunkPair, SessionId, HEADER_SIZE};

use crate::session_manager::SessionManager;

/// Reads from the TCP stream and sends downstream chunks to the client.
pub async fn run_downstream_sender(
    session_id: SessionId,
    mut stream: tokio::io::ReadHalf<TcpStream>,
    session_mgr: SessionManager,
    cancel_token: CancellationToken,
    buffer_conf: (usize, u64),
) -> Result<()> {
    let max_chunk_size: usize = buffer_conf.0;
    let inactivity_timeout = Duration::from_millis(buffer_conf.1);
    let mut buffer = BytesMut::with_capacity(max_chunk_size);
    let mut seq = 0u32;

    loop {
        tokio::select! {
            res = timeout(inactivity_timeout, stream.read_buf(&mut buffer)) => {
                match res {
                    Ok(Ok(0)) => {
                        info!("Downstream EOF for session {session_id}, flushing remaining buffer");
                        if !buffer.is_empty() {
                            let data = buffer.split().to_vec();
                            let packet = build_packet(session_id, seq, &data);
                            session_mgr.send_downstream_chunk(session_id, packet).await?;
                        }
                        session_mgr.send_end(session_id).await?;
                        break;
                    }
                    Ok(Ok(_n)) => {
                        if buffer.len() >= max_chunk_size {
                            let data = buffer.split_to(max_chunk_size).to_vec();
                            let packet = build_packet(session_id, seq, &data);
                            session_mgr.send_downstream_chunk(session_id, packet).await?;
                            seq += 1;
                        }
                    }
                    Ok(Err(e)) => {
                        error!("Stream read error: {e}");
                        return Err(e.into());
                    }
                    Err(_elapsed) => {
                        if !buffer.is_empty() {
                            info!("Flushing {} bytes due to inactivity (downstream)", buffer.len());
                            let data = buffer.split().to_vec();
                            let packet = build_packet(session_id, seq, &data);
                            session_mgr.send_downstream_chunk(session_id, packet).await?;
                            seq += 1;
                        }
                    }
                }
            }
            () = cancel_token.cancelled() => {
                info!("End signal received, downstream cancelled for session {session_id}");
                if !buffer.is_empty() {
                    let data = buffer.split().to_vec();
                    let packet = build_packet(session_id, seq, &data);
                    session_mgr.send_downstream_chunk(session_id, packet).await?;
                }
                session_mgr.send_end(session_id).await?;
                break;
            }
        }
    }
    Ok(())
}

/// Receives upstream chunks from the channel and writes them to the TCP stream.
pub async fn run_upstream_receiver(
    session_id: SessionId,
    mut stream: tokio::io::WriteHalf<TcpStream>,
    mut rx_data: UnboundedReceiver<ChunkPair>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let mut pending: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    let mut next_seq = 0u32;
    let mut last_activity = tokio::time::Instant::now();

    loop {
        tokio::select! {
            Some((hint_seq, packet)) = rx_data.recv() => {
                let (real_seq, payload) = parse_or_legacy(&packet, hint_seq, &cancel_token);
                let Some(payload) = payload else {
                    break;
                };
                pending.insert(real_seq, payload);
                while let Some(chunk) = pending.remove(&next_seq) {
                    stream.write_all(&chunk).await?;
                    next_seq += 1;
                }
                last_activity = tokio::time::Instant::now();
            }
            () = cancel_token.cancelled() => {
                info!("Upstream end signal for session {session_id}, closing");
                break;
            }
            () = tokio::time::sleep_until(last_activity + Duration::from_secs(30)) => {
                warn!("No upstream data for 30 seconds, closing session {session_id}");
                cancel_token.cancel();
                break;
            }
        }
    }
    info!("Upstream channel closed for session {session_id}");
    Ok(())
}

/// Attempt to parse a chunk header; fall back to legacy (headerless) packet.
/// If decompression fails, `cancel_token` is triggered and `(0, None)` is returned.
fn parse_or_legacy(
    packet: &[u8],
    hint_seq: u32,
    cancel_token: &CancellationToken,
) -> (u32, Option<Vec<u8>>) {
    if packet.len() < HEADER_SIZE {
        return (hint_seq, Some(packet.to_vec()));
    }
    match deserialize_header(&packet[..HEADER_SIZE]) {
        Some(header) => {
            let payload_data = &packet[HEADER_SIZE..];
            match decompress(payload_data, header.is_compressed()) {
                Ok(data) => (header.seq, Some(data)),
                Err(e) => {
                    error!("Failed to decompress chunk: {e}");
                    cancel_token.cancel();
                    (0, None)
                }
            }
        }
        None => (hint_seq, Some(packet.to_vec())),
    }
}

/// Build a complete packet (header + compressed/plain payload).
#[expect(clippy::cast_possible_truncation, reason = "chunks capped at max_chunk_size (~1 MB)")]
fn build_packet(session_id: SessionId, seq: u32, data: &[u8]) -> Vec<u8> {
    let (payload, was_compressed) = try_compress(data);
    let header = ChunkHeader::new(
        session_id.to_u128_le(),
        seq,
        was_compressed,
        payload.len() as u32,
        data.len() as u32,
    );
    let header_bytes = serialize_header(&header);
    let mut packet = header_bytes.to_vec();
    packet.extend_from_slice(&payload);
    packet
}
