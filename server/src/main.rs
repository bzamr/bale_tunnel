use anyhow::{Context, Result};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration, timeout};
use tracing::{debug, error, info, warn};
use reqwest::Url;
use reqwest::multipart::{Form, Part};

mod config;
use config::Config;

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
// A chat within an update (simplified )
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
async fn run_polling(config: Config) -> Result<()> {
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
                                // download conn file 
                                match download_file(&client, &config, &doc.file_id).await {
                                    Ok(content) => {
                                        let content_str = String::from_utf8_lossy(&content).to_string();
                                        //todo: for now, just log contetn and send ack file,should have server session manager
                                        info!("Client requested connection to: {}",content_str);
                                        let file_name=shared::ack_filename(session_id);
                                        let ack_data=String::from("OK");
                                        send_document(&config, &client, &file_name, ack_data.as_bytes())
                                        .await
                                        .context("Failed to send ack_ file")?;
                                    }
                                    Err(e) => {
                                        error!("Failed to download conn file for session {}: {}", session_id, e);
                                    }
                                }
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
                    // TODO: download file and process tunnel packets (conn_, u_, end_)
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
//Download file from bale server by fileID
async fn download_file(client: &reqwest::Client, config: &Config, file_id: &str) -> Result<Vec<u8>> {
    // Step1: get filePath by getFile method 
    let get_file_url = format!("{}/bot{}/getFile", config.bale_api_base_url, config.bale_server_bot_token);
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
    let file_url = format!("{}/file/bot{}/{}", config.bale_api_base_url, config.bale_server_bot_token, file_path);
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


async fn send_document( config: &Config,client: &reqwest::Client, filename: &str, data: &[u8]) -> Result<()> {
    let url = format!("{}/bot{}/sendDocument", config.bale_api_base_url, config.bale_server_bot_token);
    // Build multipart form with the document part.
    let part = Part::bytes(data.to_vec())//data.to_vec copies data to heap,ok for small data.
        .file_name(filename.to_string())
        .mime_str("application/octet-stream")?;
    let form = Form::new()
        .text("chat_id", config.bale_chat_id.to_string())
        .part("document", part);
    
    let response = client
        .post(&url)
        .multipart(form)
        .send()
        .await
        .context("Failed to send document")?;
    // Treat any non‑2xx status as error.
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!("sendDocument failed: {} - {}", status, text);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {

    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting Bale Tunnel Server (polling-only mode)");

    let config = Config::from_env().context("Failed to load server config from environment")?;

    // Run polling in a separate task 
    let polling_task = tokio::spawn(async move {
        if let Err(e) = run_polling(config).await {
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