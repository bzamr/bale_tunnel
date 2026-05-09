use anyhow::{Context, Result};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration, timeout};
use tracing::{debug, error, info, warn};
use reqwest::Url;

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

        for update in &updates.result {
            let update_id = update.update_id;
            last_update_id.store(update_id, Ordering::SeqCst);

            if let Some(msg) = &update.message {
                if let Some(doc) = &msg.document {
                    let file_name = doc.file_name.as_deref().unwrap_or("(no name)");
                    info!(
                        "📄 Server received document: file_id={}, file_name={}",
                        doc.file_id, file_name
                    );
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