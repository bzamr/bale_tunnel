use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;
use tracing::{debug,info,warn};
use reqwest::multipart::{Form, Part};
use reqwest::Client;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use std::collections::BTreeMap;// buffer out of order chunks
use shared::{upstream_filename, downstream_filename, end_filename};
use shared::{conn_filename, SessionId};

// Manages sessions and coordinates between SOCKS5-server and polling-loop.
#[derive(Clone)]
pub struct SessionManager {
    http_client: Client,
    bot_token: String,
    chat_id:String,
    api_base_url: String,
    // Maps session IDs to oneshot senders waiting for ack content.
    // Mutex ensures thread-safe access, Arc allows sharing across tasks.
    pending_acks: Arc<Mutex<HashMap<SessionId, oneshot::Sender<String>>>>,
    
    // Map of session IDs to unbounded channel senders(TX) for downstream chunks.
    // Each session has a sender that the polling loop uses to push (seq, data) pairs.
    // The receiver side is given to the downstream receiver task.
    downstream_senders: Arc<Mutex<HashMap<SessionId, UnboundedSender<(u32, Vec<u8>)>>>>,

    // Map of session IDs to the next upstream sequence number (starts at 0).
    // Used to generate sequential filenames for upstream chunks (u_<sess_id>_<seq>.bin).
    upstream_seq: Arc<Mutex<HashMap<SessionId, u32>>>,
}

impl SessionManager {
    pub fn new(http_client: Client, bot_token: String, chat_id:String, api_base_url: String) -> Self {
        Self {
            http_client,
            bot_token,
            chat_id,
            api_base_url,
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
            downstream_senders: Arc::new(Mutex::new(HashMap::new())),
            upstream_seq: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    //call by socks5_server.rs for each connection requset
    // Sends a conn_<session_id>.bin file to the bot and waits for the corresponding ack.
    // target_host: Destination host (e.g., "example.com")
    // target_port: Destination port (e.g., 443)
    // timeout_secs: How long to wait for ack before giving up.
    // On success (ack with "OK") returns the generated SessionId; otherwise returns an error.
    pub async fn request_connection(
        &self,
        target_host: &str,
        target_port: u16,
        timeout_secs: u64,
        ) -> Result<SessionId> 
        {
        let session_id = Uuid::new_v4();
        // The conn file content is simply "host:port".
        let content = format!("{}:{}", target_host, target_port);//todo: encode this content
        
        // Step 1: Upload the conn_ file to Bale bot.
        self.send_document(&conn_filename(session_id), content.as_bytes())
            .await
            .context("Failed to send conn_ file")?;
        
        // Step 2: Create a oneshot channel to receive the ack content.
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending_acks.lock().await;
            pending.insert(session_id, tx);//polling loop can send ack through getting tx from sessionID
        }// mutex lock opens,pending goes out of scope.
        
        // Step 3: Wait for the ack or timeout.
        let ack_content = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            rx,
        )
        .await
        .context("Timeout waiting for ack")?
        .context("Ack channel closed")?;//tx drop without sending, error Ack channel closed.
        
        // Step 4: Validate ack content – must be exactly "OK".
        if ack_content.trim() != "OK" {
            anyhow::bail!("Invalid ack content: {}", ack_content);//return err
        }
        
        // Step 5: Clean up pending entry (already removed in notify_ack, but double‑safe).
        {
            let mut pending = self.pending_acks.lock().await;
            pending.remove(&session_id);
        }
        
        info!("Session {} established, ack received", session_id);
        Ok(session_id)
    }
    // call by session manager after request_connection returned sessionID
    // Registers a downstream channel for the given session.
    // Returns the receiving end of an unbounded channel that will deliver (seq, data) pairs.
    // The sender side is stored in downstream_senders and used by on_downstream_chunk.
    pub async fn register_downstream(&self, session_id: SessionId) -> UnboundedReceiver<(u32, Vec<u8>)> {
        let (tx, rx) = unbounded_channel();
        let mut senders = self.downstream_senders.lock().await;
        senders.insert(session_id, tx);
        rx
    }

    // Sends one upstream data chunk (u_<id>_<seq>.bin) to the bot.
    // Automatically increments the sequence number for the session.
    pub async fn send_upstream_chunk(&self, session_id: SessionId, data: Vec<u8>) -> Result<()> {
        let mut seq_map = self.upstream_seq.lock().await;
        let seq = seq_map.entry(session_id).or_insert(0);
        let filename = upstream_filename(session_id, *seq);
        *seq += 1;
        drop(seq_map); // Release lock before network I/O
        self.send_document(&filename, &data).await
    }

    // Sends an end marker (empty file) to signal the other side that the session is closed.
    pub async fn send_end(&self, session_id: SessionId) -> Result<()> {
        let filename = end_filename(session_id);
        let mut data:Vec<u8>=Vec::new();
        data.push(0);
        self.send_document(&filename, &data).await
    }

    // Called by the polling loop when a downstream chunk (d_<id>_<seq>.bin) is received.
    // Forwards the (seq, data) to the corresponding downstream channel sender.
    pub async fn on_downstream_chunk(&self, session_id: SessionId, seq: u32, data: Vec<u8>) {
        let senders = self.downstream_senders.lock().await;
        if let Some(tx) = senders.get(&session_id) {
            // Ignore send error – the receiver may have been dropped (e.g., session closed).
            let _ = tx.send((seq, data));
        } else {
            warn!("No downstream sender for session {}", session_id);
        }
    }
    
    // Internal helper: uploads a document (file) to the Bale bot using sendDocument API.
    // filename: "conn_<uuid>.bin".
    // data: Raw bytes of the file content.
    async fn send_document(&self, filename: &str, data: &[u8]) -> Result<()> {
        let url = format!("{}/bot{}/sendDocument", self.api_base_url, self.bot_token);
        // Build multipart form with the document part.
        let part = Part::bytes(data.to_vec())//data.to_vec copies data to heap,ok for small data.
            .file_name(filename.to_string())
            .mime_str("application/octet-stream")?;
        let form = Form::new()
            .text("chat_id", self.chat_id.clone())
            .part("document", part);
        
        let response= self.http_client
            .post(&url)
            .multipart(form)
            .send()
            .await
            .context("Failed to send document")?;

        // Treat any non‑2xx status as error.
        debug!("send document {},response : {}",filename,response.status());
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("sendDocument failed: {} - {}", status, text);
        }
        
        Ok(())
    }
    
    // Called by the polling loop when an ack_<session_id>.bin file is received.
    // session_id: The session ID extracted from the filename.
    // content: The raw content of the ack file (expected to be "OK" on success).
    // This method resolves the pending oneshot channel, unblocking request_connection.
    pub async fn notify_ack(&self, session_id: SessionId, content: String) {
        let mut pending = self.pending_acks.lock().await;
        if let Some(sender) = pending.remove(&session_id) {
            // Send the content; ignore error if receiver already dropped (e.g., timeout).
            let _ = sender.send(content);
        } else {
            info!("Received ack for unknown session {}", session_id);
        }
    }
}