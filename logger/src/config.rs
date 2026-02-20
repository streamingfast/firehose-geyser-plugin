use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPluginError, Result as PluginResult,
};
use serde::Deserialize;
use std::{fs::read_to_string, path::Path};

fn default_true() -> bool {
    true
}

#[derive(Deserialize, Debug)]
pub struct Config {
    /// Path to the log file where events will be written.
    pub log_file: String,

    /// Whether to enable account data notifications (default: true).
    #[serde(default = "default_true")]
    pub with_accounts: bool,

    /// Whether to enable transaction notifications (default: true).
    #[serde(default = "default_true")]
    pub with_transactions: bool,
}

impl Config {
    fn load_from_str(config: &str) -> PluginResult<Self> {
        serde_json::from_str(config).map_err(|error| GeyserPluginError::ConfigFileReadError {
            msg: error.to_string(),
        })
    }

    pub fn load_from_file<P: AsRef<Path>>(file: P) -> PluginResult<Self> {
        let config = read_to_string(file).map_err(GeyserPluginError::ConfigFileOpenError)?;
        Self::load_from_str(&config)
    }
}
