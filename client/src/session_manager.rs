use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}, oneshot, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use uuid::Uuid;

use shared::bot_api::BotApi;
use shared::{conn_filename, end_filename, upstream_filename, ChunkPair, SessionId};

#[derive(Clone)]
pub struct SessionManager {
    bot_api: BotApi,
    pending_acks: Arc<Mutex<HashMap<SessionId, oneshot::Sender<String>>>>,
    downstream_senders: Arc<Mutex<HashMap<SessionId, UnboundedSender<ChunkPair>>>>,
    upstream_seq: Arc<Mutex<HashMap<SessionId, u32>>>,
    cancellation_tokens: Arc<Mutex<HashMap<SessionId, CancellationToken>>>,
}

impl SessionManager {
    pub fn new(bot_api: BotApi) -> Self {
        Self {
            bot_api,
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
            downstream_senders: Arc::new(Mutex::new(HashMap::new())),
            upstream_seq: Arc::new(Mutex::new(HashMap::new())),
            cancellation_tokens: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Sends a `conn_<session_id>.bin` and waits for the server ack.
    /// Returns the session id plus the pre-registered downstream channel
    /// (registered before the ack so no early `d_` chunk is ever dropped).
    pub async fn request_connection(
        &self,
        target_host: &str,
        target_port: u16,
        timeout_secs: u64,
    ) -> Result<(SessionId, UnboundedReceiver<ChunkPair>, CancellationToken)> {
        let session_id = Uuid::new_v4();
        // TODO(#1): encrypt/encode destination so it doesn't cross the channel in plaintext
        let content = format!("{target_host}:{target_port}");

        self.bot_api
            .upload_document(&conn_filename(session_id), content.as_bytes())
            .await
            .context("Failed to send conn_ file")?;

        // Register the downstream channel BEFORE waiting for the ack: the server
        // starts sending d_ chunks as soon as the target responds, which can race
        // with our ack handling. Any chunk arriving before registration would be
        // silently dropped.
        let (rx_data, token) = self.register_downstream(session_id).await;

        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending_acks.lock().await;
            pending.insert(session_id, tx);
        }

        let ack_content = match tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            rx,
        )
        .await
        {
            Ok(Ok(content)) => content,
            Ok(Err(_)) => {
                self.cancel_session(session_id).await;
                anyhow::bail!("Ack channel closed");
            }
            Err(_) => {
                self.cancel_session(session_id).await;
                anyhow::bail!("Timeout waiting for ack");
            }
        };

        if ack_content.trim() != "OK" {
            self.cancel_session(session_id).await;
            anyhow::bail!("Invalid ack content: {ack_content}");
        }

        {
            let mut pending = self.pending_acks.lock().await;
            pending.remove(&session_id);
        }

        info!("Session {session_id} established, ack received");
        Ok((session_id, rx_data, token))
    }

    /// Registers a downstream channel for the given session.
    pub async fn register_downstream(
        &self,
        session_id: SessionId,
    ) -> (UnboundedReceiver<ChunkPair>, CancellationToken) {
        let (tx_data, rx_data) = unbounded_channel();
        let token = CancellationToken::new();
        {
            let mut senders = self.downstream_senders.lock().await;
            senders.insert(session_id, tx_data);
            let mut tokens = self.cancellation_tokens.lock().await;
            tokens.insert(session_id, token.clone());
        }
        (rx_data, token)
    }

    /// Sends one upstream chunk (`u_<id>_<seq>.bin`).
    pub async fn send_upstream_chunk(&self, session_id: SessionId, data: Vec<u8>) -> Result<()> {
        let mut seq_map = self.upstream_seq.lock().await;
        let seq = seq_map.entry(session_id).or_insert(0);
        let filename = upstream_filename(session_id, *seq);
        *seq += 1;
        drop(seq_map);
        self.bot_api
            .upload_document_with_retry(&filename, &data, 3)
            .await
    }

    /// Sends an end marker.
    pub async fn send_end(&self, session_id: SessionId) -> Result<()> {
        self.bot_api
            .upload_document(&end_filename(session_id), &[0])
            .await
    }

    /// Called by the polling loop for each received downstream chunk.
    pub async fn on_downstream_chunk(&self, session_id: SessionId, seq: u32, data: Vec<u8>) {
        let senders = self.downstream_senders.lock().await;
        if let Some(tx) = senders.get(&session_id) {
            let _ = tx.send((seq, data));
        } else {
            // Expected when a straggler chunk arrives after the session ended.
            debug!("No downstream sender for session {session_id} (already closed?)");
        }
    }

    /// Resolves a pending oneshot, unblocking `request_connection`.
    pub async fn notify_ack(&self, session_id: SessionId, content: String) {
        let mut pending = self.pending_acks.lock().await;
        if let Some(sender) = pending.remove(&session_id) {
            let _ = sender.send(content);
        } else {
            info!("Received ack for unknown session {session_id}");
        }
    }

    /// Cancels all resources for a session.
    pub async fn cancel_session(&self, session_id: SessionId) {
        let mut tokens = self.cancellation_tokens.lock().await;
        if let Some(token) = tokens.remove(&session_id) {
            token.cancel();
        }
        let mut senders = self.downstream_senders.lock().await;
        senders.remove(&session_id);
        // Clean up sequence tracking to prevent memory leak
        let mut seq_map = self.upstream_seq.lock().await;
        seq_map.remove(&session_id);
    }
}
