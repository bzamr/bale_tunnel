use anyhow::{Context, Result};
use fast_socks5::server::Socks5ServerProtocol;
use fast_socks5::Socks5Command;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use crate::session_manager::SessionManager;
use crate::streamer;

pub async fn run_socks5_server(
    listen_addr: SocketAddr,
    session_mgr: SessionManager,
    buffer_conf: (usize, u64),
) -> Result<()> {
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("Failed to bind to {listen_addr}"))?;
    info!("SOCKS5 server listening on {listen_addr}");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        info!("Accepted connection from {peer_addr}");
        let mgr = session_mgr.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_socks5_connection(stream, peer_addr, mgr, buffer_conf).await {
                warn!("Error handling connection from {peer_addr}: {e}");
            }
        });
    }
}

async fn handle_socks5_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    session_mgr: SessionManager,
    buffer_conf: (usize, u64),
) -> Result<()> {
    // fast-socks5 uses a type-state pattern: Unauthenticated → Authenticated → CommandRead
    let proto_handshaked: Socks5ServerProtocol<TcpStream, fast_socks5::server::states::Authenticated> =
        Socks5ServerProtocol::accept_no_auth(stream)
            .await
            .context("SOCKS5 handshake (no auth) failed")?;

    let (protocol, command, target_addr) = proto_handshaked
        .read_command()
        .await
        .context("Failed to read SOCKS5 command")?;

    if let Socks5Command::TCPConnect = command {
        let (host, port) = target_addr.into_string_and_port();
        info!("[{peer_addr}] sent CONNECT request to {host}:{port}");

        // TODO(#3): replace string-match error mapping with a typed ConnectOutcome
        // enum sent in the ack payload so the protocol carries meaning, not substrings.
        let (session_id, downstream_rx, cancel_token) =
            match session_mgr.request_connection(&host, port, 30).await {
                Ok(result) => result,
                Err(e) => {
                    warn!("Failed to establish tunnel: {e}");
                    let err_msg = format!("{e}");
                    let reply = if err_msg.contains("refused") {
                        fast_socks5::ReplyError::ConnectionRefused
                    } else if err_msg.contains("timeout") {
                        fast_socks5::ReplyError::ConnectionTimeout
                    } else if err_msg.contains("Unreachable") {
                        fast_socks5::ReplyError::HostUnreachable
                    } else {
                        fast_socks5::ReplyError::GeneralFailure
                    };
                    protocol.reply_error(&reply).await?;
                    return Ok(());
                }
            };

        let tcp_stream = protocol.reply_success(peer_addr).await?;
        info!("[{peer_addr}] Tunnel established, SOCKS5 reply sent");

        let (read_half, write_half) = tokio::io::split(tcp_stream);

        let mgr_clone = session_mgr.clone();
        let cancel_token_clone = cancel_token.clone();

        tokio::spawn(async move {
            if let Err(e) = streamer::run_upstream_sender(
                session_id,
                read_half,
                mgr_clone,
                cancel_token_clone,
                buffer_conf,
            )
            .await
            {
                error!("Upstream task error: {e}");
            }
        });

        tokio::spawn(async move {
            if let Err(e) =
                streamer::run_downstream_receiver(session_id, write_half, downstream_rx, cancel_token).await
            {
                error!("Downstream task error: {e}");
            }
        });
    } else {
        warn!("[{peer_addr}] sent unsupported command: {command:?}");
        protocol
            .reply_error(&fast_socks5::ReplyError::CommandNotSupported)
            .await?;
    }

    Ok(())
}
