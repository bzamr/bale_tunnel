use anyhow::{Context, Result};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use shared::bot_api::BotApi;

mod config;
mod socks5_server;
mod session_manager;
mod streamer;
use config::Config;
use session_manager::SessionManager;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting Bale Tunnel Client (SOCKS5 + polling)");

    let config = Arc::new(
        Config::from_env().context("Failed to load config from environment")?,
    );

    let socks5_addr: std::net::SocketAddr = config
        .socks5_listen_addr
        .parse()
        .context("Invalid SOCKS5 listen address")?;

    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(config.polling_timeout_seconds + 10))
        .build()
        .context("Failed to create HTTP client")?;

    let bot_api = BotApi::new(
        http_client,
        config.bale_client_bot_token.clone(),
        config.bale_chat_id,
        config.bale_api_base_url.clone(),
    );

    let session_mgr = SessionManager::new(bot_api.clone());
    let session_mgr_clone = session_mgr.clone();
    let config_clone = config.clone();

    let buffer_conf = (config.max_chunk_size_bytes, config.inactivity_timeout_ms);

    // Task 1: SOCKS5 server
    let socks5_task = tokio::spawn(async move {
        if let Err(e) =
            socks5_server::run_socks5_server(socks5_addr, session_mgr, buffer_conf).await
        {
            error!("SOCKS5 server terminated: {e}");
        }
    });

    // Task 2: long polling
    let polling_task = tokio::spawn(async move {
        if let Err(e) = run_polling(&config_clone, bot_api, session_mgr_clone).await {
            error!("Polling task terminated: {e}");
        }
    });

    tokio::signal::ctrl_c()
        .await
        .context("Failed to install Ctrl+C handler")?;
    info!("Shutdown signal received, exiting gracefully...");

    socks5_task.abort();
    polling_task.abort();
    Ok(())
}

async fn run_polling(
    config: &Config,
    bot_api: BotApi,
    session_mgr: SessionManager,
) -> Result<()> {
    bot_api.cleanup_old_updates().await?;

    let last_update_id = Arc::new(AtomicI64::new(0));
    let expected_chat_id = config.bale_chat_id;

    loop {
        // Using SeqCst because the atomic is only touched from this task;
        // Relaxed would also be fine, but SeqCst keeps intent clear for
        // a future multi-task refactor if one ever happens.
        let offset = last_update_id.load(Ordering::SeqCst) + 1;

        let updates = match bot_api
            .poll_updates(offset, config.polling_timeout_seconds)
            .await
        {
            Ok(Some(updates)) => updates,
            Ok(None) => continue,
            Err(e) => {
                warn!("Polling error: {e}, retrying…");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        if updates.result.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        }

        for update in updates.result {
            let update_id = update.update_id;
            last_update_id.store(update_id, Ordering::SeqCst);

            let Some(msg) = update.message else {
                debug!("Update {update_id} has no message");
                continue;
            };

            if msg.chat.id != expected_chat_id {
                warn!("Received update from irrelevant chat, ignoring");
                continue;
            }

            if let Some(doc) = &msg.document {
                let file_name = doc.file_name.as_deref().unwrap_or("(no name)");
                info!("📎 Received document: file_id={}, file_name={}", doc.file_id, file_name);

                match shared::parse_filename(file_name) {
                    Ok((file_type, session_id, seq)) => {
                        process_document(
                            &bot_api,
                            &session_mgr,
                            file_type,
                            session_id,
                            seq,
                            &doc.file_id,
                        )
                        .await;
                    }
                    Err(e) => {
                        debug!("Unknown file format '{file_name}': {e}");
                    }
                }
            } else {
                debug!("Message received without document, ignoring");
            }

            // Delete the consumed message (best-effort, fire-and-forget).
            // A short delay gives the peer bot's getFile() time to finish before
            // the message (and possibly its file) disappears.
            let api = bot_api.clone();
            let msg_id = msg.message_id;
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                if let Err(e) = api.delete_message(msg_id).await {
                    error!("Failed to delete message {msg_id}: {e}");
                } else {
                    debug!("Deleted message {msg_id}");
                }
            });
        }
    }
}

async fn process_document(
    bot_api: &BotApi,
    session_mgr: &SessionManager,
    file_type: shared::FileType,
    session_id: shared::SessionId,
    seq: Option<shared::Sequence>,
    file_id: &str,
) {
    use shared::FileType;

    match file_type {
        FileType::Ack => {
            match bot_api.download_file(file_id).await {
                Ok(content) => {
                    let content_str = String::from_utf8_lossy(&content).to_string();
                    session_mgr.notify_ack(session_id, content_str).await;
                    info!("Processed ack for session {session_id}");
                }
                Err(e) => {
                    error!("Failed to download ack for session {session_id}: {e}");
                }
            }
        }

        FileType::Downstream => {
            debug!("Received downstream chunk: seq={seq:?}");
            let Some(seq_num) = seq else {
                error!("Missing sequence number for downstream chunk");
                return;
            };
            let api = bot_api.clone();
            let mgr = session_mgr.clone();
            let fid = file_id.to_string();
            tokio::spawn(async move {
                match api.download_file(&fid).await {
                    Ok(content) => {
                        mgr.on_downstream_chunk(session_id, seq_num, content).await;
                    }
                    Err(e) => {
                        error!("Failed to download downstream chunk for session {session_id}: {e}");
                    }
                }
            });
        }

        // The shared channel echoes our own uploads back to us; these are
        // expected and safe to ignore, so log at debug level.
        FileType::End => {
            info!("End signal received for session {session_id}, closing…");
            session_mgr.cancel_session(session_id).await;
        }

        other => {
            debug!("Ignoring echo of own upload ({other:?}) for session {session_id}");
        }
    }
}
