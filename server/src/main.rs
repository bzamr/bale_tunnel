use anyhow::{Context, Result};
use std::sync::atomic::{AtomicI64, Ordering};
use tracing::{debug, error, info, warn};

use shared::bot_api::BotApi;

mod config;
mod session_manager;
mod streamer;
mod webhook;
use config::Config;
use session_manager::SessionManager;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting Bale Tunnel Server");

    let config = Config::from_env().context("Failed to load server config from environment")?;

    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(config.polling_timeout_seconds + 10))
        .build()
        .context("Failed to create HTTP client")?;

    let bot_api = BotApi::new(
        http_client,
        config.bale_server_bot_token.clone(),
        config.bale_chat_id,
        config.bale_api_base_url.clone(),
    );

    let session_mgr = SessionManager::new(bot_api.clone());

    // ── Mode selection: webhook vs polling ────────────────────────────

    if let Some(ref base_url) = config.webhook_base_url {
        let webhook_url = format!("{}{}", base_url.trim_end_matches('/'), config.webhook_path);
        info!("Webhook mode enabled — registering {webhook_url}");

        bot_api
            .set_webhook(&webhook_url)
            .await
            .context("Failed to register webhook with Bale")?;

        let webhook_api = bot_api.clone();
        let webhook_task = tokio::spawn(async move {
            if let Err(e) =
                webhook::run_server(&config, webhook_api, session_mgr)
                    .await
            {
                error!("Webhook server terminated: {e}");
            }
        });

        tokio::signal::ctrl_c()
            .await
            .context("Failed to install Ctrl+C handler")?;
        info!("Shutdown signal received, removing webhook…");

        webhook_task.abort();

        if let Err(e) = bot_api.delete_webhook().await {
            warn!("Failed to remove webhook on shutdown: {e}");
        }

        info!("Exiting gracefully…");
    } else {
        info!("Long-polling mode enabled (no webhook configured)");

        let polling_task = tokio::spawn(async move {
            if let Err(e) = run_polling(&config, bot_api, session_mgr).await {
                error!("Polling task terminated: {e}");
            }
        });

        tokio::signal::ctrl_c()
            .await
            .context("Failed to install Ctrl+C handler")?;
        info!("Shutdown signal received, exiting gracefully…");

        polling_task.abort();
    }

    Ok(())
}

async fn run_polling(
    config: &Config,
    bot_api: BotApi,
    session_mgr: SessionManager,
) -> Result<()> {
    bot_api.cleanup_old_updates().await?;

    let last_update_id = std::sync::Arc::new(AtomicI64::new(0));
    let expected_chat_id = config.bale_chat_id;

    loop {
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
                debug!("Server: update {update_id} has no message");
                continue;
            };

            if msg.chat.id != expected_chat_id {
                warn!("Received update from irrelevant chat, ignoring");
                continue;
            }

            if let Some(doc) = &msg.document {
                let file_name = doc.file_name.as_deref().unwrap_or("(no name)");
                info!("📄 Server received document: file_id={}, file_name={}", doc.file_id, file_name);

                match shared::parse_filename(file_name) {
                    Ok((file_type, session_id, seq)) => {
                        process_document(
                            &bot_api,
                            &session_mgr,
                            config,
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
                debug!("Server: message has no document, ignoring");
            }

            let api = bot_api.clone();
            let msg_id = msg.message_id;
            tokio::spawn(async move {
                // A short delay gives the peer bot's getFile() time to finish
                // before the message (and possibly its file) disappears.
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

pub(crate) async fn process_document(
    bot_api: &BotApi,
    session_mgr: &SessionManager,
    config: &Config,
    file_type: shared::FileType,
    session_id: shared::SessionId,
    seq: Option<shared::Sequence>,
    file_id: &str,
) {
    use shared::FileType;

    match file_type {
        FileType::Conn => {
            let mgr = session_mgr.clone();
            let fid = file_id.to_string();
            let buf_conf = (config.max_chunk_size_bytes, config.inactivity_timeout_ms);
            tokio::spawn(async move {
                if let Err(e) = mgr.handle_conn_file(session_id, &fid, buf_conf).await {
                    error!("Failed to handle conn for {session_id}: {e}");
                }
            });
        }

        FileType::Upstream => {
            debug!("Received upstream chunk: seq={seq:?}");
            let Some(seq_num) = seq else {
                error!("Missing sequence number for upstream chunk");
                return;
            };
            let mgr = session_mgr.clone();
            let api = bot_api.clone();
            let fid = file_id.to_string();
            tokio::spawn(async move {
                match api.download_file(&fid).await {
                    Ok(content) => {
                        mgr.on_upstream_chunk(session_id, seq_num, content).await;
                    }
                    Err(e) => {
                        error!("Failed to download upstream chunk for session {session_id}: {e}");
                    }
                }
            });
        }

        FileType::End => {
            info!("End signal received for session {session_id}, closing…");
            session_mgr.cancel_session(session_id).await;
        }

        // The shared channel echoes our own uploads (d_/ack_/end_) back to us;
        // these are expected and safe to ignore, so log at debug level.
        other => {
            debug!("Ignoring echo of own upload ({other:?}) for session {session_id}");
        }
    }
}
