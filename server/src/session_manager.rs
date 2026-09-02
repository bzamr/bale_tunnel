use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}, Mutex};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use shared::bot_api::BotApi;
use shared::{ack_filename, end_filename, ChunkPair, SessionId};

#[derive(Clone)]
pub struct SessionManager {
    bot_api: BotApi,
    upstream_senders: Arc<Mutex<HashMap<SessionId, UnboundedSender<ChunkPair>>>>,
    cancellation_tokens: Arc<Mutex<HashMap<SessionId, CancellationToken>>>,
    downstream_seq: Arc<Mutex<HashMap<SessionId, u32>>>,
}

impl SessionManager {
    pub fn new(bot_api: BotApi) -> Self {
        Self {
            bot_api,
            upstream_senders: Arc::new(Mutex::new(HashMap::new())),
            downstream_seq: Arc::new(Mutex::new(HashMap::new())),
            cancellation_tokens: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Downloads the `conn_` file, connects to the target, sends ack, spawns streamer tasks.
    pub async fn handle_conn_file(
        &self,
        session_id: SessionId,
        file_id: &str,
        buffer_conf: (usize, u64),
    ) -> Result<()> {
        let content = self.bot_api.download_file(file_id).await?;
        let host_port = String::from_utf8_lossy(&content).to_string();
        info!("Conn request for session {session_id}: {host_port}");

        let ack_file_name = ack_filename(session_id);

        let connect_future = TcpStream::connect(&host_port);
        match timeout(std::time::Duration::from_secs(10), connect_future).await {
            Ok(Ok(stream)) => {
                // Register the upstream channel BEFORE sending the ack: the client
                // starts uploading u_ chunks as soon as it sees the ack, and any
                // chunk arriving before registration would be silently dropped.
                let (upstream_rx, cancel_token) = self.register_upstream(session_id).await;

                self.bot_api
                    .upload_document(&ack_file_name, b"OK")
                    .await?;
                info!("Session {session_id} established, ack sent");

                let (read_half, write_half) = tokio::io::split(stream);

                let mgr = self.clone();
                let token_clone = cancel_token.clone();
                tokio::spawn(async move {
                    if let Err(e) = crate::streamer::run_downstream_sender(
                        session_id, read_half, mgr, token_clone, buffer_conf,
                    )
                    .await
                    {
                        error!("Downstream sender error: {e}");
                    }
                });

                tokio::spawn(async move {
                    if let Err(e) = crate::streamer::run_upstream_receiver(
                        session_id, write_half, upstream_rx, cancel_token,
                    )
                    .await
                    {
                        error!("Upstream receiver error: {e}");
                    }
                });

                Ok(())
            }
            Ok(Err(e)) => {
                let err_msg = format!("ERR: {e}");
                self.bot_api
                    .upload_document(&ack_file_name, err_msg.as_bytes())
                    .await?;
                warn!("Failed to connect for session {session_id}: {e}");
                anyhow::bail!("{err_msg}")
            }
            Err(_) => {
                let err_msg = "ERR: Connection timeout".to_string();
                self.bot_api
                    .upload_document(&ack_file_name, err_msg.as_bytes())
                    .await?;
                warn!("Connection timeout for session {session_id}");
                anyhow::bail!("{err_msg}")
            }
        }
    }

    /// Upload a downstream chunk (`d_<id>_<seq>.bin`).
    pub async fn send_downstream_chunk(&self, session_id: SessionId, data: Vec<u8>) -> Result<()> {
        let mut seq_map = self.downstream_seq.lock().await;
        let seq = seq_map.entry(session_id).or_insert(0);
        let filename = shared::downstream_filename(session_id, *seq);
        *seq += 1;
        drop(seq_map);
        self.bot_api
            .upload_document_with_retry(&filename, &data, 3)
            .await
    }

    pub async fn send_end(&self, session_id: SessionId) -> Result<()> {
        self.bot_api
            .upload_document(&end_filename(session_id), &[0])
            .await
    }

    pub async fn register_upstream(
        &self,
        session_id: SessionId,
    ) -> (UnboundedReceiver<ChunkPair>, CancellationToken) {
        let (tx_data, rx_data) = unbounded_channel();
        let token = CancellationToken::new();
        {
            let mut senders = self.upstream_senders.lock().await;
            senders.insert(session_id, tx_data);
            let mut tokens = self.cancellation_tokens.lock().await;
            tokens.insert(session_id, token.clone());
        }
        (rx_data, token)
    }

    pub async fn on_upstream_chunk(&self, session_id: SessionId, seq: u32, data: Vec<u8>) {
        let senders = self.upstream_senders.lock().await;
        if let Some(tx) = senders.get(&session_id) {
            let _ = tx.send((seq, data));
        } else {
            // Expected when a straggler chunk arrives after the session ended.
            debug!("No upstream sender for session {session_id} (already closed?)");
        }
    }

    pub async fn cancel_session(&self, session_id: SessionId) {
        let mut tokens = self.cancellation_tokens.lock().await;
        if let Some(token) = tokens.remove(&session_id) {
            token.cancel();
        }
        let mut senders = self.upstream_senders.lock().await;
        senders.remove(&session_id);
        // Clean up sequence tracking to prevent memory leak
        let mut seq_map = self.downstream_seq.lock().await;
        seq_map.remove(&session_id);
    }
}
