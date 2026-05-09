use anyhow::Result;
use serde::Deserialize;
// load config for bale-tunnel client form .env file (auto convert uppercase)

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    // Bale bot token obtained from @BotFather.
    // Mandatory field; causes an error if missing.
    pub bale_bot_token: String,

    // Base URL for the Bale Bot API.
    // Optional; defaults to "https://tapi.bale.ai".
    #[serde(default = "default_api_base_url")]
    pub bale_api_base_url: String,

    // Long polling timeout in seconds for getUpdates requests.
    // Optional; defaults to 30 seconds.
    #[serde(default = "default_polling_timeout")]
    pub polling_timeout_seconds: u64,

    // Local SOCKS5 server listen address .
    // Optional; defaults to "127.0.0.1:1080".
    #[serde(default = "default_socks5_addr")]
    pub socks5_listen_addr: String,
}

// Default Bale API base URL.
fn default_api_base_url() -> String {
    "https://tapi.bale.ai".to_string()
}

// Default long polling timeout (seconds).
fn default_polling_timeout() -> u64 {
    30
}

// Default SOCKS5 listen address.
fn default_socks5_addr() -> String {
    "127.0.0.1:1080".to_string()
}

impl Config {
    // Loads configuration from environment variables.
    // # Prerequisites
    // `dotenvy::dotenv().ok()` should have been called before this method
    // to load the `.env` file, if present.
    // # Errors
    // Returns an error if any mandatory environment variable is missing
    // or fails to parse into the expected type.
    pub fn from_env() -> Result<Self> {
        Ok(envy::from_env::<Config>()?)
        //turbo-fish: from_env::<type> declare the the return type 
    }

    // Constructs the full URL for the getUpdates API endpoint.
    pub fn get_updates_url(&self) -> String {
        format!(
            "{}/bot{}/getUpdates",
            self.bale_api_base_url, self.bale_bot_token
        )
    }
}