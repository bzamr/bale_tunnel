use anyhow::{Context, Result};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration, timeout};
use tracing::{debug, error, info, warn};
use reqwest::Url;

mod config;
mod session_manager;
use config::Config;
use session_manager::SessionManager;


// ---------- Bale API response structures ----------
#[derive(Debug, serde::Deserialize)]
struct GetUpdatesResponse {
    ok: bool,
    result: Vec<Update>,
}

#[derive(Debug, serde::Deserialize)]
struct Update {
    update_id: i64,
    message: Option<Message>,
}

#[derive(Debug, serde::Deserialize)]
struct Message {
    document: Option<Document>,
     chat:Chat,
}
// A chat within a message (simplified )
#[derive(Debug, serde::Deserialize)]
struct Chat {
    id:i64,   // channel chatId used for communication 
}

#[derive(Debug, serde::Deserialize)]
struct Document {
    file_id: String,
    file_name: Option<String>,
}

// ---------- Polling task ----------
async fn run_polling(config: Config, session_mgr: SessionManager) -> Result<()> {
    info!("Starting server polling loop (receiving tunnel chunks from client)");
    // Create an HTTP client with a global timeout, 10 sec longer than the long polling timeout.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.polling_timeout_seconds + 10))
        .build()
        .context("Failed to create HTTP client")?;

    let last_update_id = Arc::new(AtomicI64::new(0));
    let get_updates_url = config.get_updates_url();

    loop {
        let offset = last_update_id.load(Ordering::SeqCst) + 1;

        let url_with_params = Url::parse_with_params(
            &get_updates_url,
            &[
                ("offset", offset.to_string()),
                ("timeout", config.polling_timeout_seconds.to_string()),
            ],
        )
        .context("Failed to construct URL with params")?;

        debug!("Server polling with offset={}", offset);

        let request_future = client.get(url_with_params).send();
        let response = match timeout(
            Duration::from_secs(config.polling_timeout_seconds + 5),
            request_future,
        )
        .await
        {
            Ok(res) => res.context("HTTP request failed")?,
            Err(_) => {
                warn!(" Polling request timeout, retrying...");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            error!("API error: {} - {}", status, text);
            sleep(Duration::from_secs(2)).await;
            continue;
        }

        let updates: GetUpdatesResponse = match response.json().await {
            Ok(up) => up,
            Err(e) => {
                error!("Failed to parse JSON: {}", e);
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        if !updates.ok {
            error!("Server: Bale API returned ok=false");
            sleep(Duration::from_secs(1)).await;
            continue;
        }
        // to check if received update is from communication channel
        let expected_chat_id=config.bale_chat_id;

        for update in &updates.result {
            let update_id = update.update_id;
            last_update_id.store(update_id, Ordering::SeqCst);
            //check of its a communication message through channel
            if let Some(msg) = &update.message {
                if msg.chat.id != expected_chat_id{
                warn!("Receive update from unrelevant chat, ignoring.");
                continue;
                }
                if let Some(doc) = &msg.document {
                    let file_name = doc.file_name.as_deref().unwrap_or("(no name)");
                    info!(
                        "📄 Server received document: file_id={}, file_name={}",
                        doc.file_id, file_name
                    );

                     // extract fileType and sessionID from fileName
                    if let Ok((file_type, session_id, _seq)) = shared::parse_filename(file_name) {
                        match file_type {
                            shared::FileType::Conn => {
                                let session_mngr_clone=session_mgr.clone();
                                let file_id_clone = doc.file_id.clone();
                                tokio::spawn(async move{
                                    if let Err(e) = session_mngr_clone.handle_conn_file(session_id, &file_id_clone).await {
                                    error!("Failed to handle conn for {}: {}", session_id, e);
                                }
                                });
                            }
                            shared::FileType::Upstream => {
                                //todo use for tunnel 
                                debug!("Received data chunk: {:?}", file_type);
                            }
                            shared::FileType::End => {
                                info!("Received end signal for session {}", session_id);
                                // todo: close connection.
                            }
                            _=>{ 
                                warn!("Received unValid file type for session {}", session_id);
                            }
                        }
                    } else {
                        debug!("Unknown file format: {}", file_name);
                    }
                } else {
                    debug!("Server: message has no document, ignoring");
                }
            } else {
                debug!("Server: update {} has no message", update_id);
            }
        }

        if updates.result.is_empty() {
            sleep(Duration::from_millis(100)).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {

    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting Bale Tunnel Server with SessionManager");

    let config = Config::from_env().context("Failed to load server config from environment")?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.polling_timeout_seconds + 10))
        .build()?;
    let session_mgr = SessionManager::new(client, config.clone());
    // Run polling in a separate task 
    let polling_task = tokio::spawn(async move {
        if let Err(e) = run_polling(config,session_mgr).await {
            error!("Polling task terminated with error: {}", e);
        }
    });

    // Wait for Ctrl+C or termination signal
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install Ctrl+C handler");
    info!("Shutdown signal received, exiting gracefully...");

    // Abort polling task (optional, but good practice)
    polling_task.abort();
    
    Ok(())
}