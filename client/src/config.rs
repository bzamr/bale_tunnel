use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub bale_client_bot_token: String,
    pub bale_chat_id: i64,

    #[serde(default = "default_api_base_url")]
    pub bale_api_base_url: String,

    /// Long polling timeout in seconds for getUpdates.
    #[serde(default = "default_polling_timeout")]
    pub polling_timeout_seconds: u64,

    /// Local SOCKS5 listen address.
    #[serde(default = "default_socks5_addr")]
    pub socks5_listen_addr: String,

    /// Max chunk buffer size in bytes (default 1 MB).
    #[serde(default = "default_max_chunk_size")]
    pub max_chunk_size_bytes: usize,

    /// Inactivity timeout in ms before flushing a partial buffer (default 500 ms).
    /// A value of 10 ms was too aggressive for a tunnel carried over a bot API
    /// (one HTTP upload per chunk).
    #[serde(default = "default_inactivity_timeout_ms")]
    pub inactivity_timeout_ms: u64,
}

fn default_api_base_url() -> String {
    "https://tapi.bale.ai".to_string()
}

fn default_polling_timeout() -> u64 {
    30
}

fn default_socks5_addr() -> String {
    "127.0.0.1:1080".to_string()
}

fn default_max_chunk_size() -> usize {
    1_048_576 // 1 MB
}

/// 500 ms — long enough to batch a few reads, short enough to stay responsive.
fn default_inactivity_timeout_ms() -> u64 {
    500
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
