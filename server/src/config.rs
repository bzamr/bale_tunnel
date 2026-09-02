use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub bale_server_bot_token: String,
    pub bale_chat_id: i64,

    #[serde(default = "default_api_base_url")]
    pub bale_api_base_url: String,

    /// Long polling timeout in seconds for getUpdates.
    #[serde(default = "default_polling_timeout")]
    pub polling_timeout_seconds: u64,

    /// Max chunk buffer size in bytes (default 1 MB).
    #[serde(default = "default_max_chunk_size")]
    pub max_chunk_size_bytes: usize,

    /// Inactivity timeout in ms before flushing a partial buffer (default 500 ms).
    #[serde(default = "default_inactivity_timeout_ms")]
    pub inactivity_timeout_ms: u64,

    /// Public URL where Bale should POST webhook updates.
    /// When set, the server uses webhook mode instead of long polling.
    pub webhook_base_url: Option<String>,

    /// Path appended to `webhook_base_url` for the webhook endpoint.
    #[serde(default = "default_webhook_path")]
    pub webhook_path: String,

    /// Port the axum HTTP server listens on in webhook mode.
    #[serde(default = "default_server_port")]
    pub server_port: u16,
}

fn default_api_base_url() -> String {
    "https://tapi.bale.ai".to_string()
}

fn default_polling_timeout() -> u64 {
    30
}

fn default_max_chunk_size() -> usize {
    1_048_576 // 1 MB
}

fn default_inactivity_timeout_ms() -> u64 {
    500
}

fn default_webhook_path() -> String {
    "/webhook".to_string()
}

fn default_server_port() -> u16 {
    8080
}

impl Config {
    /// Loads configuration from environment variables.
    ///
    /// # Prerequisites
    /// `dotenvy::dotenv().ok()` should have been called first.
    pub fn from_env() -> Result<Self> {
        Ok(envy::from_env::<Config>()?)
    }
}
