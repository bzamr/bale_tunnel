use anyhow::{Context, Result};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration, timeout};
use tracing::{debug, error, info, trace, warn};  
use reqwest::Url;
use std::net::SocketAddr;

mod config;
mod socks5_server; 

use config::Config;

// =================== Bale API Response Data Structures ==================

// Response wrapper for getUpdates API call
#[derive(Debug, serde::Deserialize)]
struct GetUpdatesResponse {
    ok: bool,                     // Indicates if the request was successful
    result: Vec<Update>,          // List of new updates (messages, edits, etc.)
}

// A single update received from the bot
#[derive(Debug, serde::Deserialize)]
struct Update {
    update_id: i64,               
    message: Option<Message>,     // Optional message payload (may be other types like callback_query)
}

// A message within an update (simplified )
#[derive(Debug, serde::Deserialize)]
struct Message {
    document: Option<Document>,   // Optional document (file) attachment
}

// A document (file) attached to a message
#[derive(Debug, serde::Deserialize)]
struct Document {
    file_id: String,              // Unique Bale file identifier (used to download the file)
    file_name: Option<String>,    // Original filename 
}

// ======================== Application Entry Point ========================

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from `.env` file (if present); ignore errors.
    dotenvy::dotenv().ok();//todo handle error
    
    // Initialize structured logging with filtering from RUST_LOG environment variable.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting Bale Tunnel Client (basic SOCKS5 + polling)");

    // Load configuration from environment (BALE_BOT_TOKEN, etc.)
    let config = Config::from_env().context("Failed to load config from environment")?;
    let socks5_addr: SocketAddr = config
        .socks5_listen_addr
        .parse()
        .context("Invalid SOCKS5 listen address")?;

     // task1: socks5 server (defined at client/src/socks5_server.rs)
    let socks5_task = tokio::spawn(async move {
        if let Err(e) = socks5_server::run_socks5_server(socks5_addr).await {
            error!("SOCKS5 server terminated: {}", e);
        }
    });

    // task2: long polling 
    let polling_task = tokio::spawn(async move {
        if let Err(e) = run_polling(config).await {
            error!("Polling task terminated: {}", e);
        }
    });

    // waiting for terminate signal (Ctrl+C)
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install Ctrl+C handler");
    info!("Shutdown signal received, exiting gracefully...");

    // Abort both tasks (optional, but good practice)
    socks5_task.abort();
    polling_task.abort();

    Ok(())
}

async fn run_polling(config: Config) -> Result<()>{
    // Create an HTTP client with a global timeout, 10 sec longer than the long polling timeout.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.polling_timeout_seconds + 10))
        .build()
        .context("Failed to create HTTP client")?;

    // Shared atomic counter to track the last processed update_id across loops.
    let last_update_id = Arc::new(AtomicI64::new(0));
    let get_updates_url = config.get_updates_url();

    // Main polling loop – runs indefinitely until the process is terminated.
    loop {
        // Compute the next offset: last processed ID + 1.
        // Using SeqCst(Sequentially Consistent) ordering for future multi task(pooling+fileProsecc)
        let offset = last_update_id.load(Ordering::SeqCst) + 1;

        // Build URL with query parameters manually
        let url_with_params: Url = Url::parse_with_params(
            &get_updates_url,
            &[
                ("offset", offset.to_string()),
                ("timeout", config.polling_timeout_seconds.to_string()),
            ],
        ).context("Failed to construct URL with params")?;
        trace!("url_with_params: {}",url_with_params);
        info!("Polling updates with offset={}", offset);

        let request_future = client.get(url_with_params).send();
        // if request_future didn't recieve in 35s, warn! and sleep (network problem)
        let response: reqwest::Response = match timeout(
            Duration::from_secs(config.polling_timeout_seconds + 5),
            request_future,
        )
        .await
        {
            Ok(res) => res.context("HTTP request failed")?,
            Err(_) => {
                warn!("HTTP Request timeout, retrying...");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        
        // Check HTTP status code (200..=299 is ok)
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            error!("API error: {} - {}", status, text);//todo: handel error(rate limit ,...)
            sleep(Duration::from_secs(2)).await;
            continue;
        }

        // Parse the JSON response into our structs.
        let updates: GetUpdatesResponse = match response.json().await {
            Ok(up) => up,
            Err(e) => {
                error!("Failed to parse JSON: {}", e);
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        // Validate the 'ok' field – if false, something went wrong with the request.
        if !updates.ok {
            error!("API returned ok=false");
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        // Process each received update.
        for update in &updates.result {
            let update_id = update.update_id;
            // Always advance the offset to this update_id (even if we ignore it)
            last_update_id.store(update_id, Ordering::SeqCst);

            if let Some(msg) = &update.message {
                if let Some(doc) = &msg.document {
                    // log  document (file) metadata.
                    // todo: download the file using getFile and process it as a tunnel chunk.
                    let file_name = doc.file_name.as_deref().unwrap_or("(no name)");
                    info!(
                        "📎 Received document: file_id={}, file_name={}",
                        doc.file_id, file_name
                    );
                } else {
                    debug!("Message received but doesn't have document, ignoring");
                }
            } else {
                debug!("Update {} has no message", update_id);
            }
        }

        // Short sleep  to avoid busy-waiting and reduce CPU usage.
        if updates.result.is_empty() {
            sleep(Duration::from_millis(100)).await;
        }
    }
}