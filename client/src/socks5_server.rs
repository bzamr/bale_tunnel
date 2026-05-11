use anyhow::{Context, Result};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn,error};
use fast_socks5::server::Socks5ServerProtocol;
use fast_socks5::Socks5Command;  


use crate::session_manager::SessionManager;
use crate::streamer;


// run socks5 server on the declared addr in .env file 
pub async fn run_socks5_server(listen_addr: SocketAddr, session_mgr: SessionManager) -> Result<()> {
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("Failed to bind to {}", listen_addr))?;//if port is taken or ...
    info!("SOCKS5 server listening on {}", listen_addr);

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        info!("Accepted connection from {}", peer_addr);
        let session_mgr_clone = session_mgr.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_socks5_connection(stream, peer_addr,session_mgr_clone).await {
                warn!("Error handling connection from {}: {}", peer_addr, e);
            }
        });
    }
}

async fn handle_socks5_connection(stream: TcpStream, peer_addr: SocketAddr, session_mgr: SessionManager) -> Result<()> {
    //fast-socks5 use Typestate and has 3 state: Unauthenticated, Authenticated, CommandRead
    // in each state, only some functions can be call 
    // step1: handshake with no_auth
    let  proto_handshaked: Socks5ServerProtocol<TcpStream, fast_socks5::server::states::Authenticated> // state changed to Authenticated
            = Socks5ServerProtocol::accept_no_auth(stream)
        .await
        .context("SOCKS5 handshake (no auth) failed")?;// if client doesn't accept handshake with no_auth

    // step2: read client command (CONNECT، BIND، UDP ASSOCIATE)
    let (protocol, command, target_addr) = proto_handshaked
        .read_command()// state changed to CommandRead
        .await
        .context("Failed to read SOCKS5 command")?;

    // step3: process command 
    match command {
        Socks5Command::TCPConnect => {
            let (host,port)=target_addr.into_string_and_port();
            info!("[{}] sent CONNECT request to {host}:{port}", peer_addr);

            
            let session_id = match session_mgr.request_connection(&host, port, 30).await {
                Ok(s_id)=> s_id , 
                Err(e)=>{
                    // error: timeout or invalid ack, notify client
                    warn!("Failed to establish tunnel: {}", e);
                    let err_msg=format!("{}",e);
                    let reply=if err_msg.contains("refused") {
                        fast_socks5::ReplyError::ConnectionRefused
                    } else if err_msg.contains("timeout") {
                        fast_socks5::ReplyError::ConnectionTimeout
                    } else if err_msg.contains("Unreachable") {
                        fast_socks5::ReplyError::HostUnreachable
                    }else {
                        fast_socks5::ReplyError::GeneralFailure
                    };
                    protocol.reply_error(&reply).await?;
                    return Ok(());
                }
            };
            
            //success reply to client
            let tcp_stream=protocol.reply_success(peer_addr).await?;
            info!("[{}] Tunnel established, SOCKS5 reply sent", peer_addr);
            
            // Split the single TcpStream into separate read and write halves.
            // This allows concurrent reading and writing without needing to share a single stream.
            let (read_half, write_half) = tokio::io::split(tcp_stream);
                    
            // Register a downstream channel for this session and obtain the receiving end.
            // The receiving end will be used to get (seq, data) pairs from the polling loop.
            let downstream_rx = session_mgr.register_downstream(session_id).await;
                    
            let mgr_clone = session_mgr.clone();
                    
            // Spawn a task to read from the SOCKS5 socket and send upstream chunks (content of u_ files).
            let _upstream_task = tokio::spawn(async move {
                if let Err(e) = streamer::run_upstream_sender(session_id, read_half, mgr_clone).await {
                    error!("Upstream task error: {}", e);
                }
            });
            
            // Spawn a task to receive downstream chunks (content of d_ files) and write them to the SOCKS5 socket.
            let _downstream_task = tokio::spawn(async move {
                if let Err(e) = streamer::run_downstream_receiver(session_id, write_half, downstream_rx).await {
                    error!("Downstream task error: {}", e);
                }
            });
        }
        _ => {
            warn!("[{}] sent Unsupported command: {:?}", peer_addr, command);
            protocol.reply_error(&fast_socks5::ReplyError::CommandNotSupported).await?;
        }
    }

    // let connection close (without sending response to client)
    Ok(())
}