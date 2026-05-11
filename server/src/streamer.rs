use anyhow::Result;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};// for stream.read, stream.write
use tokio::sync::{mpsc::UnboundedReceiver};
use std::collections::BTreeMap;
use tracing::{info, error};
use crate::session_manager::SessionManager;
use shared::SessionId;
use tokio_util::sync::CancellationToken;


pub async fn run_downstream_sender(
    session_id: SessionId,
    mut stream: tokio::io::ReadHalf<TcpStream>,  // read half from split
    session_mgr: SessionManager,
    cancel_token: CancellationToken,
) -> Result<()> {
    // Use a 1 MiB buffer to match the maximum chunk size allowed by the protocol.
    // todo : make buffer dynamic 
    let mut buf = vec![0u8; 1024 * 1024]; // 1 MiB
    loop {
        tokio::select! {
            res= stream.read(&mut buf)=> {
            let n=res?;
            // EOF (0 bytes read) means the host closed the connection.
            if n == 0 {
                info!("downstream EOF for session {}, sending end", session_id);
                session_mgr.send_end(session_id).await?;
                break;
            }
            let data = buf[..n].to_vec();  // Copy the actual read bytes(rest of buffer is 0).
            if let Err(e) = session_mgr.send_downstream_chunk(session_id, data).await {
                error!("Failed to send downstream chunk: {}", e);
                return Err(e);// todo : retry by checking error ( rate limit, ...)
            }
            }
            _ = cancel_token.cancelled() => {
                info!("end signal received, downstream cancelled for session {}", session_id);
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
    
    loop{
         tokio::select! {
            // steam data receive
            Some((seq, data)) = rx_data.recv() => {
                pending.insert(seq, data);
                while let Some(chunk) = pending.remove(&next_seq) {
                    stream.write_all(&chunk).await?;
                    next_seq += 1;
                }
            }
            // end receive 
            _ = cancel_token.cancelled() => {
                info!("upstream end signal received for session {}, closing", session_id);
                break;
            }
        }
    }
    info!("upstream channel closed for session {}", session_id);
    Ok(())
}