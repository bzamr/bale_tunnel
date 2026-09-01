use anyhow::{Context, Result};
use reqwest::multipart::{Form, Part};
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, warn};

// ── Typed Bale API DTOs ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct GetUpdatesResponse {
    pub ok: bool,
    pub result: Vec<Update>,
}

#[derive(Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
}

#[derive(Debug, Deserialize)]
pub struct Message {
    pub message_id: i64,
    pub document: Option<Document>,
    pub chat: Chat,
}

#[derive(Debug, Deserialize)]
pub struct Chat {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct Document {
    pub file_id: String,
    pub file_name: Option<String>,
}

// ── BotApi ─────────────────────────────────────────────────────────────

/// A thin client around the Bale Bot HTTP API.
#[derive(Clone, Debug)]
pub struct BotApi {
    client: Client,
    pub bot_token: String,
    pub chat_id: i64,
    pub base_url: String,
}

impl BotApi {
    /// Create a new `BotApi` instance.
    #[must_use]
    pub fn new(client: Client, bot_token: String, chat_id: i64, base_url: String) -> Self {
        Self {
            client,
            bot_token,
            chat_id,
            base_url,
        }
    }

    // ── Polling ───────────────────────────────────────────────────────

    /// Build the getUpdates URL for the given offset and timeout.
    ///
    /// # Errors
    /// Returns an error if the URL cannot be constructed.
    pub fn get_updates_url(&self, offset: i64, timeout_secs: u64) -> Result<reqwest::Url> {
        let raw = format!("{}/bot{}/getUpdates", self.base_url, self.bot_token);
        reqwest::Url::parse_with_params(
            &raw,
            &[
                ("offset", offset.to_string()),
                ("timeout", timeout_secs.to_string()),
            ],
        )
        .context("Failed to construct getUpdates URL")
    }

    /// Fetch updates with a hard timeout around the request itself.
    ///
    /// # Errors
    /// Returns an error on network failure, non-2xx status, or `ok=false`.
    pub async fn poll_updates(
        &self,
        offset: i64,
        poll_timeout_secs: u64,
    ) -> Result<Option<GetUpdatesResponse>> {
        let url = self.get_updates_url(offset, poll_timeout_secs)?;
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(poll_timeout_secs + 5),
            self.client.get(url).send(),
        )
        .await
        .context("Polling request timed out")?
        .context("HTTP request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("API error: {status} – {text}");
        }

        let updates: GetUpdatesResponse = response
            .json()
            .await
            .context("Failed to parse getUpdates JSON")?;

        if !updates.ok {
            anyhow::bail!("Bale API returned ok=false");
        }

        Ok(Some(updates))
    }

    // ── Upload ────────────────────────────────────────────────────────

    /// Upload a file via sendDocument.
    ///
    /// # Errors
    /// Returns an error on network failure or non-2xx status.
    pub async fn upload_document(&self, filename: &str, data: &[u8]) -> Result<()> {
        let url = format!("{}/bot{}/sendDocument", self.base_url, self.bot_token);
        let part = Part::bytes(data.to_vec())
            .file_name(filename.to_string())
            .mime_str("application/octet-stream")?;
        let form = Form::new()
            .text("chat_id", self.chat_id.to_string())
            .part("document", part);

        let response = self
            .client
            .post(&url)
            .multipart(form)
            .send()
            .await
            .context("Failed to send document")?;

        debug!("sendDocument {filename}: {}", response.status());
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("sendDocument failed: {status} – {text}");
        }

        Ok(())
    }

    /// Upload with exponential-backoff retry.
    ///
    /// # Errors
    /// Returns an error after exhausting all retries.
    pub async fn upload_document_with_retry(
        &self,
        filename: &str,
        data: &[u8],
        max_retries: u32,
    ) -> Result<()> {
        let mut attempt = 0;
        let mut delay = std::time::Duration::from_secs(1);
        loop {
            match self.upload_document(filename, data).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_retries {
                        anyhow::bail!(
                            "Failed to send {filename} after {max_retries} retries: {e}"
                        );
                    }
                    warn!(
                        "Retry {attempt}/{max_retries} for {filename} after {delay:?}: {e}"
                    );
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
            }
        }
    }

    // ── Download ──────────────────────────────────────────────────────

    /// Download file contents by Bale `file_id`.
    ///
    /// # Errors
    /// Returns an error if the file cannot be resolved or downloaded.
    pub async fn download_file(&self, file_id: &str) -> Result<Vec<u8>> {
        let get_url = format!("{}/bot{}/getFile", self.base_url, self.bot_token);
        let resp: serde_json::Value = self
            .client
            .post(&get_url)
            .json(&serde_json::json!({ "file_id": file_id }))
            .send()
            .await
            .context("Failed to call getFile")?
            .json()
            .await
            .context("Failed to parse getFile response")?;

        let file_path = resp["result"]["file_path"]
            .as_str()
            .context("Missing file_path in getFile response")?;

        let file_url = format!(
            "{}/file/bot{}/{}",
            self.base_url, self.bot_token, file_path
        );
        let bytes = self
            .client
            .get(&file_url)
            .send()
            .await
            .context("Failed to download file")?
            .bytes()
            .await
            .context("Failed to read file bytes")?;

        Ok(bytes.to_vec())
    }

    // ── Delete ────────────────────────────────────────────────────────

    /// Delete a message by id.
    ///
    /// # Errors
    /// Returns an error on network failure or non-2xx status.
    pub async fn delete_message(&self, message_id: i64) -> Result<()> {
        let url = format!("{}/bot{}/deleteMessage", self.base_url, self.bot_token);
        let response = self
            .client
            .post(&url)
            .json(&serde_json::json!({
                "chat_id": self.chat_id,
                "message_id": message_id,
            }))
            .send()
            .await
            .context("Failed to call deleteMessage")?;
        if !response.status().is_success() {
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("deleteMessage failed: {text}");
        }
        Ok(())
    }

    /// Delete all messages from the communication channel (best-effort).
    ///
    /// # Errors
    /// Returns an error if the initial fetch fails.
    pub async fn cleanup_old_updates(&self) -> Result<()> {
        let resp = self
            .client
            .get(self.get_updates_url(0, 0)?)
            .send()
            .await
            .context("Failed to fetch updates for cleanup")?;
        let updates: GetUpdatesResponse = resp.json().await?;
        for update in updates.result {
            if let Some(msg) = update.message
                && msg.chat.id == self.chat_id
                && let Err(e) = self.delete_message(msg.message_id).await
            {
                warn!("cleanup: failed to delete message {}: {e}", msg.message_id);
            }
        }
        Ok(())
    }
}
