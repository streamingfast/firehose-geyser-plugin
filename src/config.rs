use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPluginError, Result as PluginResult,
};
use serde::Deserialize;
use serde_json;

use std::{fs::read_to_string, path::Path};

#[derive(Deserialize, Default, Debug)]
pub struct Config {
    pub libpath: String,
    pub rpc_client: RpcClientConfig,
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
