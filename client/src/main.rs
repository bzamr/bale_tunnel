use anyhow::{Context, Result};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration, timeout};
use tracing::{debug, error, info, trace, warn};  
use reqwest::{Client, Url};
use std::net::SocketAddr;

mod config;
mod socks5_server; 
mod session_manager;
mod streamer;
use config::Config;

use crate::session_manager::SessionManager;

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
    chat:Chat,
}
// A chat within an update (simplified )
#[derive(Debug, serde::Deserialize)]
struct Chat {
    id:i64,   // channel chatId used for communication 
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
     // Create an HTTP client with a global timeout, 10 sec longer than the long polling timeout.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.polling_timeout_seconds + 10))
        .build()
        .context("Failed to create HTTP client")?;
    let session=SessionManager::new(client.clone(),config.bale_client_bot_token.clone(),config.bale_chat_id.to_string() ,config.bale_api_base_url.clone());
    let session_clone=session.clone();
    // task1: socks5 server (defined at client/src/socks5_server.rs)
    let socks5_task = tokio::spawn(async move {
        if let Err(e) = socks5_server::run_socks5_server(socks5_addr,session).await {
            error!("SOCKS5 server terminated: {}", e);
        }
    });

    // task2: long polling 
    let polling_task = tokio::spawn(async move {
        if let Err(e) = run_polling(config,client,session_clone).await {
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

async fn run_polling(config: Config,client:Client,session_mgr: SessionManager) -> Result<()>{

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
        // to check if received update is from communication channel
        let expected_chat_id=config.bale_chat_id;
        // Process each received update.
        for update in &updates.result {
            let update_id = update.update_id;
            // Always advance the offset to this update_id (even if we ignore it)
            last_update_id.store(update_id, Ordering::SeqCst);
            //check of its a communication message through channel
            
            if let Some(msg) = &update.message {
                if msg.chat.id != expected_chat_id{
                warn!("Receive update from unrelevant chat, ignoring.");
                continue;
                }
                if let Some(doc) = &msg.document {
                    // log  document (file) metadata.
                    // todo: download the file using getFile and process it as a tunnel chunk.
                    let file_name = doc.file_name.as_deref().unwrap_or("(no name)");
                    info!(
                        "📎 Received document: file_id={}, file_name={}",
                        doc.file_id, file_name
                    );
                    // extract fileType and sessionID from fileName
                    if let Ok((file_type, session_id, seq)) = shared::parse_filename(file_name) {
                        match file_type {
                            shared::FileType::Ack => {
                                // download ack file 
                                match download_file(&client, &config, &doc.file_id).await {
                                    Ok(content) => {
                                        let content_str = String::from_utf8_lossy(&content).to_string();
                                        session_mgr.notify_ack(session_id, content_str).await;
                                        info!("Processed ack for session {}", session_id);
                                    }
                                    Err(e) => {
                                        error!("Failed to download ack file for session {}: {}", session_id, e);
                                    }
                                }
                            }
                            shared::FileType::Downstream => {
                                debug!("Received data chunk: {:?}", file_type);
                                if let Some(seq_num) = seq { //seq option come from parse name
                                    let client_clone=client.clone();
                                    let config_clone=config.clone();
                                    let file_id_clone=doc.file_id.clone();
                                    let session_mngr_clone=session_mgr.clone();
                                    let _=tokio::spawn(async move{
                                        match download_file(&client_clone, &config_clone, &file_id_clone).await {
                                            Ok(content)=>{
                                                session_mngr_clone
                                                .on_downstream_chunk(session_id, seq_num, content).await;
                                                // on_downstream_chunk will send data to steamer via Unbound_channel
                                            }
                                            Err(e)=>{
                                                error!("Failed to download downstream file for session {}: {}",
                                                        session_id, e);
                                            }
                                        } 
                                    });
                                }else {
                                    error!("Missing sequence number for downstream file");
                                }                       
                            }
                            shared::FileType::End => {
                                info!("End signal received for session {}, closing...", session_id);
                                session_mgr.cancel_session(session_id).await;
                            }
                            _=>{ 
                                warn!("Received unValid file type for session {}", session_id);
                            }
                        }
                    } else {
                        debug!("Unknown file format: {}", file_name);
                    }
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

//Download file from bale server by fileID
async fn download_file(client: &reqwest::Client, config: &Config, file_id: &str) -> Result<Vec<u8>> {
    // Step1: get filePath by getFile method 
    let get_file_url = format!("{}/bot{}/getFile", config.bale_api_base_url, config.bale_client_bot_token);
    let resp: serde_json::Value = client
        .post(&get_file_url)
        .json(&serde_json::json!({ "file_id": file_id }))
        .send()
        .await
        .context("Failed to call getFile")?
        .json()
        .await
        .context("Failed to parse getFile response")?;
    //reminder: filePath is valid upto 1hour. 
    let file_path = resp["result"]["file_path"]
        .as_str()
        .context("Missing file_path in response")?;
    //Step2: download file by filePath     
    let file_url = format!("{}/file/bot{}/{}", config.bale_api_base_url, config.bale_client_bot_token, file_path);
    let file_data = client
        .get(&file_url)
        .send()
        .await
        .context("Failed to download file")?
        .bytes()
        .await
        .context("Failed to read file bytes")?;
    
    Ok(file_data.to_vec())
}