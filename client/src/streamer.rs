use anyhow::Result;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};// for stream.read, stream.write
use tokio::sync::mpsc::UnboundedReceiver;
use std::collections::BTreeMap;
use tracing::{info, error};
use crate::session_manager::SessionManager;
use shared::SessionId;

// Reads data from the SOCKS5 socket and sends upstream chunks
// to the server via the Bale bot as u_<session_id>_<seq>.bin files.
// The stream is expected to be the read half of a split TcpStream.
pub async fn run_upstream_sender(
    session_id: SessionId,
    mut stream: tokio::io::ReadHalf<TcpStream>,  // read half from split
    session_mgr: SessionManager,
) -> Result<()> {
    // Use a 1 MiB buffer to match the maximum chunk size allowed by the protocol.
    // todo : make buffer dynamic 
    let mut buf = vec![0u8; 1024 * 1024]; // 1 MiB
    loop {
        let n = stream.read(&mut buf).await?;
        // EOF (0 bytes read) means the browser closed the connection.
        if n == 0 {
            info!("Upstream EOF for session {}, sending end", session_id);
            session_mgr.send_end(session_id).await?;
            break;
        }
        let data = buf[..n].to_vec();  // Copy the actual read bytes(rest of buffer is 0).
        if let Err(e) = session_mgr.send_upstream_chunk(session_id, data).await {
            error!("Failed to send upstream chunk: {}", e);
            return Err(e);// todo : retry by checking error ( rate limit, ...)
        }
    }
    Ok(())
}

// Receives downstream chunks (content of d_<id>_<seq>.bin) from the channel, 
// reorders them if necessary,
// and writes the data to the SOCKS5 socket (outbound to the browser).
// Reordering uses a BTreeMap to buffer chunks that arrive out of order.
pub async fn run_downstream_receiver(
    session_id: SessionId,
    mut stream: tokio::io::WriteHalf<TcpStream>,  // write half from split
    mut rx: UnboundedReceiver<(u32, Vec<u8>)>,
) -> Result<()> {
    // Buffer for out‑of‑order chsunk (seq -> data)
    let mut pending = BTreeMap::new();// sorted by seq
    let mut next_seq = 0u32; // Next expected sequence number

    while let Some((seq, data)) = rx.recv().await {
        pending.insert(seq, data);
        // Write all consecutive chunks starting from next_seq.
        while let Some(chunk) = pending.remove(&next_seq) {
            stream.write_all(&chunk).await?;
            next_seq += 1;
        }
    }
    info!("Downstream channel closed for session {}", session_id);
    Ok(())
}