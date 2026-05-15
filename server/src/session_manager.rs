use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, info, warn,error};
use reqwest::Client;
use reqwest::multipart::{Form, Part};
use shared::{SessionId, ack_filename, end_filename};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use tokio_util::sync::CancellationToken;
use crate::config;
// Manages sessions
#[derive(Clone)]
pub struct SessionManager {
    http_client: Client,
    // include: bot_token, chat_id، base_url)
    config: Arc<config::Config>,
    //streams: Arc<Mutex<HashMap<SessionId, TcpStream>>>,
    upstream_senders: Arc<Mutex<HashMap<SessionId, UnboundedSender<(u32, Vec<u8>)>>>>,
    cancellation_tokens: Arc<Mutex<HashMap<SessionId, CancellationToken>>>,
    downstream_seq: Arc<Mutex<HashMap<SessionId, u32>>>,
}

impl SessionManager {
    pub fn new(http_client: Client, config: config::Config) -> Self {
        Self {
            http_client,
            config: Arc::new(config),
            //streams: Arc::new(Mutex::new(HashMap::new())),
            upstream_senders: Arc::new(Mutex::new(HashMap::new())),
            downstream_seq: Arc::new(Mutex::new(HashMap::new())),
            cancellation_tokens:  Arc::new(Mutex::new(HashMap::new())),
        }
    }
    //call by server's long polling, download conn_ file, connet to target host
    // send ack and add stream to hashmap
    pub async fn handle_conn_file(
        &self,
        session_id: SessionId,
        file_id: &str,
        buffer_conf:(usize,u64),
    ) -> Result<()> {
        // step1:download conn_ file 
        let content = self.download_file_content(file_id).await?;
        let host_port = String::from_utf8_lossy(&content).to_string();
        info!("Conn request for session {}: {}", session_id, host_port);
        let ack_file_name = ack_filename(session_id);
        // step2: connet to target with 10s timeout
        let connect_future = TcpStream::connect(&host_port);
        match timeout(std::time::Duration::from_secs(10), connect_future).await {
            Ok(Ok(stream)) => {
                // store stream in map
                /* 
                let mut streams = self.streams.lock().await;
                streams.insert(session_id, stream);
                drop(streams);
                */
                // send successful ack 
                self.send_document(&ack_file_name, "OK".as_bytes()).await?;
                info!("Session {} established, ack sent", session_id);
                let (read_half, write_half) 
                    = tokio::io::split(stream);
                let (downstream_rx, cancel_token) =
                self.register_upstream(session_id).await;       
                let mgr_clone = self.clone();
                let cancel_token_clone = cancel_token.clone();
        
                // Spawn a task to read from the stream and send downstream chunks (content of d_ files).
                let _upstream_task = tokio::spawn(async move {
                    if let Err(e) = crate::streamer::
                            run_downstream_sender(session_id, read_half, mgr_clone, cancel_token_clone,buffer_conf).await {
                        error!("Upstream task error: {}", e);
                    }
                });
            
                // Spawn a task to receive upstream chunks (content of u_ files) and write them stream.
                let _downstream_task = tokio::spawn(async move {
                    if let Err(e) = crate::streamer::
                        run_upstream_receiver(session_id, write_half, downstream_rx,cancel_token).await {
                        error!("Downstream task error: {}", e);
                    };
                });
                Ok(())
            }    
            Ok(Err(e)) => {
                let err_msg = format!("ERR: {}", e);
                self.send_document(&ack_file_name, err_msg.as_bytes()).await?;
                warn!("Failed to connect for session {}: {}", session_id, e);
                anyhow::bail!(err_msg)
            }
            Err(_) => {
                let err_msg = "ERR: Connection timeout".to_string();
                self.send_document(&ack_file_name, err_msg.as_bytes()).await?;
                warn!("Connection timeout for session {}", session_id);
                anyhow::bail!(err_msg)
            }
        }
    }

    // download file from bale by file_id
    pub async fn download_file_content(&self, file_id: &str) -> Result<Vec<u8>> {
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

    async fn send_document(&self, filename: &str, data: &[u8]) -> Result<()> {
        let url = format!("{}/bot{}/sendDocument", self.config.bale_api_base_url, self.config.bale_server_bot_token);
        // Build multipart form with the document part.
        let part = Part::bytes(data.to_vec())//data.to_vec copies data to heap,ok for small data.
            .file_name(filename.to_string())
            .mime_str("application/octet-stream")?;
        let form = Form::new()
            .text("chat_id", self.config.bale_chat_id.to_string())
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

    async fn send_document_with_retry(&self, filename: &str, data: &[u8], max_retries: u32) -> Result<()> {
        let mut attempt = 0;
        let mut delay = tokio::time::Duration::from_secs(1);
        loop {
            match self.send_document(filename, data).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_retries {
                        anyhow::bail!("Failed to send {} after {} retries: {}", filename, max_retries, e);
                    }
                    warn!("Retry {}/{} for {} after {:?}: {}", attempt, max_retries, filename, delay, e);
                    tokio::time::sleep(delay).await;
                    delay = delay * 2; // backoff: 1s, 2s, 4s, ...
                }
            }
        }
    }
    
    pub async fn register_upstream(&self, session_id: SessionId)
    ->  (UnboundedReceiver<(u32, Vec<u8>)>, CancellationToken) 
    {
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
    pub async fn send_end(&self, session_id: SessionId) -> Result<()> {
        let filename = end_filename(session_id);
        let mut data:Vec<u8>=Vec::new();
        data.push(0);
        self.send_document(&filename, &data).await
    }
     pub async fn send_downstream_chunk(&self, session_id: SessionId, data: Vec<u8>) -> Result<()> {
        let mut seq_map = self.downstream_seq.lock().await;
        let seq = seq_map.entry(session_id).or_insert(0);
        let filename = shared::downstream_filename(session_id, *seq);
        *seq += 1;
        drop(seq_map); // Release lock before network I/O
        self.send_document_with_retry(&filename, &data, 3).await
    }
    pub async fn on_upstream_chunk(&self, session_id: SessionId, seq: u32, data: Vec<u8>) {
        let senders = self.upstream_senders.lock().await;
        if let Some(tx) = senders.get(&session_id) {
            // Ignore send error – the receiver may have been dropped (e.g., session closed).
            let _ = tx.send((seq, data));
        } else {
            warn!("No downstream sender for session {}", session_id);
        }
    }
    pub async fn cancel_session(&self, session_id: SessionId) {
        let mut tokens = self.cancellation_tokens.lock().await;
        if let Some(token) = tokens.remove(&session_id) {
            token.cancel();
        }
        // remove other resources
        let mut senders = self.upstream_senders.lock().await;
        senders.remove(&session_id);
    }
    /* 
    pub async fn get_stream(&self, session_id: &SessionId) -> Option<TcpStream> {
        let mut streams = self.streams.lock().await;
        streams.remove(session_id) 
    }

    // remove stream for end_ request
    pub async fn remove_stream(&self, session_id: &SessionId) -> Option<TcpStream> {
        let mut streams = self.streams.lock().await;
        streams.remove(session_id)
    }*/
}