use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPluginError, Result as PluginResult,
};
use serde::Deserialize;
use serde_json;

use std::{fs::read_to_string, path::Path};

#[derive(Deserialize, Default, Debug)]
pub struct Config {
    pub libpath: String,
    pub local_rpc_client: RpcClientConfig,
    pub remote_rpc_client: RpcClientConfig,
    pub cursor_file: String,

    #[serde(default)]
    pub noop: bool,

    #[serde(default)]
    pub send_processed: bool,
    #[serde(default)]
    pub log: ConfigLog,
    pub account_block_destination_file: String,
    pub block_destination_file: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigLog {
    /// Log level.
    #[serde(default = "ConfigLog::default_level")]
    pub level: String,
}

impl Default for ConfigLog {
    fn default() -> Self {
        Self {
            level: Self::default_level(),
        }
    }
}

impl ConfigLog {
    fn default_level() -> String {
        "info".to_owned()
    }
}

#[derive(Deserialize, Default, Debug)]
pub struct RpcClientConfig {
    pub endpoint: String,
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
