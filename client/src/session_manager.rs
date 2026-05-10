use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;
use tracing::{debug,info};
use reqwest::multipart::{Form, Part};
use reqwest::Client;

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
}

impl SessionManager {
    pub fn new(http_client: Client, bot_token: String, chat_id:String, api_base_url: String) -> Self {
        Self {
            http_client,
            bot_token,
            chat_id,
            api_base_url,
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    //call by socks5_server.rs for each connection requset
    // Sends a conn_<session_id>.bin file to the bot and waits for the corresponding ack.
    // target_host: Destination host (e.g., "example.com")
    // target_port: Destination port (e.g., 443)
    // timeout_secs: How long to wait for ack before giving up.
    // Returns Ok(()) if ack with "OK" content is received within timeout.
    pub async fn request_connection(
        &self,
        target_host: &str,
        target_port: u16,
        timeout_secs: u64,
        ) -> Result<()>
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
        Ok(())
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