use anyhow::Result;
use tracing::info;
//use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // load environmet variables from .env
    dotenvy::dotenv().ok();
    
    // start logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    
    info!("Starting Bale Tunnel Client (SOCKS5 proxy)");
    
    
    
    Ok(())
}
