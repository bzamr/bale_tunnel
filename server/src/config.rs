use anyhow::Result;
use serde::Deserialize;

// Configuration for the Bale tunnel server (polling ).
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    // Bale bot token obtained from @BotFather.
    // Mandatory field; causes an error if missing.
    pub bale_bot_token: String,

    // Base URL for the Bale Bot API.
    // Optional; defaults to "https://tapi.bale.ai".
    #[serde(default = "default_api_base_url")]
    pub bale_api_base_url: String,

    // Long polling timeout in seconds for getUpdates.
    #[serde(default = "default_polling_timeout")]
    pub polling_timeout_seconds: u64,
}

fn default_api_base_url() -> String {
    "https://tapi.bale.ai".to_string()
}

fn default_polling_timeout() -> u64 {
    30
}

impl Config {
    // Load configuration from environment variables.
    // # Prerequisites
    // `dotenvy::dotenv().ok()` should have been called before this method
    pub fn from_env() -> Result<Self> {
        Ok(envy::from_env::<Config>()?)
    }

    /// Returns the full URL for getUpdates endpoint.
    pub fn get_updates_url(&self) -> String {
        format!(
            "{}/bot{}/getUpdates",
            self.bale_api_base_url, self.bale_bot_token
        )
    }
}