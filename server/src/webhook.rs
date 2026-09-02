use std::net::SocketAddr;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, warn};

use shared::bot_api::Update;

use crate::config::Config;
use crate::session_manager::SessionManager;
use crate::{process_document, BotApi};

/// Shared state passed to the axum handler.
#[derive(Clone)]
pub(crate) struct WebhookState {
    pub bot_api: BotApi,
    pub session_mgr: SessionManager,
    pub config: Config,
}

/// Build the axum [`Router`] with the webhook endpoint.
fn router(state: WebhookState, webhook_path: &str) -> Router {
    Router::new()
        .route(webhook_path, post(handle_update))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Axum POST handler for Bale webhook updates.
///
/// Bale expects a fast 200 response; heavy work is spawned asynchronously.
async fn handle_update(
    State(state): State<WebhookState>,
    Json(update): Json<Update>,
) -> StatusCode {
    let update_id = update.update_id;

    let Some(msg) = update.message else {
        debug!("Webhook update {update_id} has no message");
        return StatusCode::OK;
    };

    if msg.chat.id != state.config.bale_chat_id {
        warn!("Webhook received update from irrelevant chat, ignoring");
        return StatusCode::OK;
    }

    if let Some(doc) = &msg.document {
        let file_name = doc.file_name.as_deref().unwrap_or("(no name)");
        info!(
            "📄 Server received document (webhook): file_id={}, file_name={}",
            doc.file_id, file_name
        );

        match shared::parse_filename(file_name) {
            Ok((file_type, session_id, seq)) => {
                let bot_api = state.bot_api.clone();
                let session_mgr = state.session_mgr.clone();
                let config = state.config.clone();
                let file_id = doc.file_id.clone();
                tokio::spawn(async move {
                    process_document(
                        &bot_api,
                        &session_mgr,
                        &config,
                        file_type,
                        session_id,
                        seq,
                        &file_id,
                    )
                    .await;
                });
            }
            Err(e) => {
                debug!("Unknown file format '{file_name}': {e}");
            }
        }
    } else {
        debug!("Webhook: message has no document, ignoring");
    }

    // Schedule delayed deletion of the source message (same as polling path).
    let api = state.bot_api.clone();
    let msg_id = msg.message_id;
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        if let Err(e) = api.delete_message(msg_id).await {
            error!("Failed to delete message {msg_id}: {e}");
        } else {
            debug!("Deleted message {msg_id}");
        }
    });

    StatusCode::OK
}

/// Start the axum HTTP server. Runs until the task is aborted by the caller.
pub(crate) async fn run_server(
    config: &Config,
    bot_api: BotApi,
    session_mgr: SessionManager,
) -> anyhow::Result<()> {
    let state = WebhookState {
        bot_api,
        session_mgr,
        config: config.clone(),
    };

    let app = router(state, &config.webhook_path);
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server_port));

    info!("Starting webhook server on {addr}");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind to {addr}: {e}"))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!("Axum server error: {e}"))
}
