use anyhow::Result;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};// for stream.read, stream.write
use tokio::sync::{mpsc::UnboundedReceiver};
use std::collections::BTreeMap;
use tracing::{info, warn, error};
use crate::session_manager::SessionManager;
use shared::SessionId;
use tokio_util::sync::CancellationToken;
use bytes::BytesMut;  // for zero‑copy buffering
use tokio::time::{Duration, timeout};

// buffer_conf conatains max_chunk_size and inactivity_timeout
// - max_chunk_size: maximum size of a single upstream chunk (bytes)
// - inactivity_timeout: how long to wait before flushing an incomplete chunk
pub async fn run_downstream_sender(
    session_id: SessionId,
    mut stream: tokio::io::ReadHalf<TcpStream>,  // read half from split
    session_mgr: SessionManager,
    cancel_token: CancellationToken,
    buffer_conf:(usize,u64) 
) -> Result<()> {
    let max_chunk_size: usize=buffer_conf.0;
    let inactivity_timeout: Duration=tokio::time::Duration::from_millis(buffer_conf.1);
    let mut buffer = BytesMut::with_capacity(max_chunk_size);
    loop {
          tokio::select! {
            // Attempt to read more data, but with a timeout to detect inactivity.
            res = timeout(inactivity_timeout, stream.read_buf(&mut buffer)) => {
                match res {
                    Ok(Ok(0)) => {
                        // EOF (0 bytes read) means the host closed the connection.
                        info!("downstream EOF for session {}, flushing remaining buffer", session_id);
                        if !buffer.is_empty() {
                            //debug!("Sending chunk of {} bytes", data.len());
                            let data = buffer.split().to_vec();// todo: change send_upstream parameter to Byte
                            session_mgr.send_downstream_chunk(session_id, data).await?;
                        }
                        session_mgr.send_end(session_id).await?;
                        break;
                    }
                    Ok(Ok(_n)) => {
                        // Data received successfully
                        if buffer.len() >= max_chunk_size {
                            // Buffer full: send immediately
                            // extra data will remain in buffer(extra safty)
                            // read_buf fill upto buffer capacity, so shouldn't have extra data
                            // debug!("Sending chunk of {} bytes", data.len());
                            let data = buffer.split_to(max_chunk_size).to_vec();
                            session_mgr.send_downstream_chunk(session_id, data).await?;
                        }
                        // Continue reading (buffer may still have space)
                    }
                    Ok(Err(e)) => {
                        error!("stream Read error: {}", e);
                        return Err(e.into());
                    }
                    Err(_timeout) => {
                        // Inactivity timeout triggered – flush any pending data
                        if !buffer.is_empty() {
                            info!("Flushing {} bytes due to inactivity", buffer.len());
                            //debug!("Sent chunk of {} bytes", data.len());
                            let data = buffer.split().to_vec();
                            session_mgr.send_downstream_chunk(session_id, data).await?;

                        }
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                info!("End signal received, downstream cancelled for session {}", session_id);
                // Send any remaining buffered data before closing
                if !buffer.is_empty() {
                    let data = buffer.split().to_vec();
                    session_mgr.send_downstream_chunk(session_id, data).await?;
                }
                session_mgr.send_end(session_id).await?;
                break;
            }
        }
    }
    Ok(())
}

pub async fn run_upstream_receiver(
    session_id: SessionId,
    mut stream: tokio::io::WriteHalf<TcpStream>,  // write half from split
    mut rx_data: UnboundedReceiver<(u32, Vec<u8>)>,
    cancel_token: CancellationToken,
) -> Result<()> {
    // Buffer for out‑of‑order chsunk (seq -> data)
    let mut pending = BTreeMap::new();// sorted by seq
    let mut next_seq = 0u32; // Next expected sequence number
    let mut last_activity = tokio::time::Instant::now();//for timeout 

    loop{
         tokio::select! {
            // steam data receive
            Some((seq, data)) = rx_data.recv() => {
                pending.insert(seq, data);
                while let Some(chunk) = pending.remove(&next_seq) {
                    stream.write_all(&chunk).await?;
                    next_seq += 1;
                }
                last_activity = tokio::time::Instant::now();
            }
            // end receive 
            _ = cancel_token.cancelled() => {
                info!("upstream end signal received for session {}, closing", session_id);
                break;
            }
            // 30s timeout
            _ = tokio::time::sleep_until(last_activity + tokio::time::Duration::from_secs(30)) => {
                warn!("No downstream data recieved for 30 seconds, closing session {}", session_id);
                cancel_token.cancel();
                break;
            }
        }
    }
    info!("upstream channel closed for session {}", session_id);
    Ok(())
}