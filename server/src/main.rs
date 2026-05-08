use anyhow::Result;
use tracing::info;
//use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    
    info!("Starting Bale Tunnel Server (webhook receiver)");
    
    
    
    Ok(())
}
