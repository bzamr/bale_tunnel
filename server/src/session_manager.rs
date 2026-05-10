use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{info, warn};
use reqwest::Client;
use reqwest::multipart::{Form, Part};
use shared::{SessionId, ack_filename};
use crate::config;
// Manages sessions
#[derive(Clone)]
pub struct SessionManager {
    http_client: Client,
    // include: bot_token, chat_id، base_url)
    config: Arc<config::Config>,
    streams: Arc<Mutex<HashMap<SessionId, TcpStream>>>,
}

impl SessionManager {
    pub fn new(http_client: Client, config: config::Config) -> Self {
        Self {
            http_client,
            config: Arc::new(config),
            streams: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    //call by server's long polling, download conn_ file, connet to target host
    // send ack and add stream to hashmap
    pub async fn handle_conn_file(
        &self,
        session_id: SessionId,
        file_id: &str,
    ) -> Result<()> {
        // step1:download conn_ file 
        let content = self.download_file_content(file_id).await?;
        let host_port = String::from_utf8_lossy(&content).to_string();
        info!("Conn request for session {}: {}", session_id, host_port);

        // step2: connet to target with 10s timeout
        let connect_future = TcpStream::connect(&host_port);
        match timeout(std::time::Duration::from_secs(10), connect_future).await {
            Ok(Ok(stream)) => {
                // store stream in map
                let mut streams = self.streams.lock().await;
                streams.insert(session_id, stream);
                drop(streams);
                // send successful ack 
                self.send_ack(session_id, "OK").await?;
                info!("Session {} established, ack sent", session_id);
                Ok(())
            }
            Ok(Err(e)) => {
                let err_msg = format!("ERR: {}", e);
                self.send_ack(session_id, &err_msg).await?;
                warn!("Failed to connect for session {}: {}", session_id, e);
                anyhow::bail!(err_msg)
            }
            Err(_) => {
                let err_msg = "ERR: Connection timeout".to_string();
                self.send_ack(session_id, &err_msg).await?;
                warn!("Connection timeout for session {}", session_id);
                anyhow::bail!(err_msg)
            }
        }
    }

    // download file from bale by file_id
    async fn download_file_content(&self, file_id: &str) -> Result<Vec<u8>> {
        //Step1: get file_path by getFile method.
        let get_file_url = format!("{}/bot{}/getFile", self.config.bale_api_base_url, self.config.bale_server_bot_token);
        let resp: serde_json::Value = self.http_client
            .post(&get_file_url)
            .json(&serde_json::json!({ "file_id": file_id }))
            .send()
            .await
            .context("Failed to call getFile")?
            .json()
            .await
            .context("Failed to parse getFile response")?;
        let file_path = resp["result"]["file_path"]
            .as_str()
            .context("Missing file_path in response")?;

        //Step2: download file from file_path
        let file_url = format!("{}/file/bot{}/{}", self.config.bale_api_base_url, self.config.bale_server_bot_token, file_path);
        let file_data = self.http_client
            .get(&file_url)
            .send()
            .await
            .context("Failed to download file")?
            .bytes()
            .await
            .context("Failed to read file bytes")?;
        Ok(file_data.to_vec())
    }

    /// send ack_ file to bale channel
    async fn send_ack(&self, session_id: SessionId, content: &str) -> Result<()> {
        let filename = ack_filename(session_id);
        let url = format!("{}/bot{}/sendDocument", self.config.bale_api_base_url, self.config.bale_server_bot_token);
        let part = Part::bytes(content.as_bytes().to_vec())
            .file_name(filename)
            .mime_str("application/octet-stream")?;
        let form = Form::new()
            .text("chat_id", self.config.bale_chat_id.to_string())
            .part("document", part);
        let response = self.http_client
            .post(&url)
            .multipart(form)
            .send()
            .await
            .context("Failed to send ack")?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("sendAck failed: {} - {}", status, text);
        }
        Ok(())
    }

    pub async fn get_stream(&self, session_id: &SessionId) -> Option<TcpStream> {
        let mut streams = self.streams.lock().await;
        streams.remove(session_id) 
    }

    // remove stream for end_ request
    pub async fn remove_stream(&self, session_id: &SessionId) -> Option<TcpStream> {
        let mut streams = self.streams.lock().await;
        streams.remove(session_id)
    }
}