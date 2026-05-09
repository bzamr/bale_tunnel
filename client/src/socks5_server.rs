use anyhow::{Context, Result};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn, error};
use fast_socks5::server::Socks5ServerProtocol;
use fast_socks5::Socks5Command;  

// run socks5 server on the declared addr in .env file 
pub async fn run_socks5_server(listen_addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("Failed to bind to {}", listen_addr))?;//if port is taken or ...
    info!("SOCKS5 server listening on {}", listen_addr);

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        info!("Accepted connection from {}", peer_addr);
        tokio::spawn(async move {
            if let Err(e) = handle_socks5_connection(stream, peer_addr).await {
                warn!("Error handling connection from {}: {}", peer_addr, e);
            }
        });
    }
}

async fn handle_socks5_connection(stream: TcpStream, peer_addr: SocketAddr) -> Result<()> {
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
            info!("[{}] sent CONNECT request to {}", peer_addr, target_addr);
        }
        _ => {
            warn!("[{}] sent Unsupported command: {:?}", peer_addr, command);
            // can send error response to clinet, for now just let connection close 
        }
    }

    // let connection close (without sending response to client)
    Ok(())
}