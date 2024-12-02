use agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use {
    crate::{config::Config as PluginConfig, state::BlockInfo, state::State},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult,
    },
    gxhash::gxhash64,
    std::{concat, env, sync::RwLock},
};

use crate::utils::convert_sol_timestamp;
use env_logger::Target;
use log::{debug, info, LevelFilter};
use solana_rpc_client::rpc_client::RpcClient;
use std::fmt;
use std::str::FromStr;

const SEED: i64 = 76;

#[derive(Default)]
pub struct Plugin {
    state: RwLock<State>,
    send_processed: bool,
    trace: bool,
}

impl fmt::Debug for Plugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Plugin").finish()
    }
}

fn cursor_from_file(cursor_file: &str) -> Option<u64> {
    match std::fs::read_to_string(cursor_file) {
        Ok(cursor) => {
            let cursor = cursor.trim().parse::<u64>().ok();
            cursor
        }
        Err(_) => None,
    }
}

impl Plugin {
    fn set_account(
        &self,
        slot: u64,
        pub_key: &[u8],
        data: &[u8],
        owner: &[u8],
        write_version: u64,
        deleted: bool,
        is_startup: bool,
    ) {
        let mut lock_state = self.state.write().unwrap();

        if !is_startup && lock_state.should_skip_slot(slot) {
            return;
        }

        let data_hash = if data.len() == 0 {
            0
        } else {
            gxhash64(data, SEED)
        };

        if self.trace {
            debug!(
                "slot: {}, pub_key: {:?}, owner: {:?}, write_version: {}, deleted: {}, data_hash: {}, is_startup: {}",
                slot, hex::encode(pub_key), hex::encode(owner), write_version, deleted, data_hash, is_startup
            );
        }

        lock_state.set_account(
            slot,
            pub_key,
            data,
            owner,
            write_version,
            deleted,
            is_startup,
            data_hash,
            self.trace,
        );
    }
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        let plugin_config = PluginConfig::load_from_file(config_file)?;

        let filter_level =
            LevelFilter::from_str(plugin_config.log.level.as_str()).unwrap_or(LevelFilter::Info);

        if filter_level == LevelFilter::Trace {
            self.trace = true;
        }

        env_logger::Builder::new()
            .filter_level(filter_level)
            .format_timestamp_nanos()
            .target(Target::Stdout)
            .init();

        debug!("on load");

        let local_rpc_client = RpcClient::new(plugin_config.local_rpc_client.endpoint);
        let remote_rpc_client = RpcClient::new(plugin_config.remote_rpc_client.endpoint);
        let cursor = cursor_from_file(&plugin_config.cursor_file);
        self.send_processed = plugin_config.send_processed;

        self.state = RwLock::new(State::new(
            local_rpc_client,
            remote_rpc_client,
            cursor,
            plugin_config.cursor_file,
            plugin_config.noop,
        ));

        println!("FIRE INIT 3.0 sf.solana.type.v1.AccountBlock");
        info!("cursor: {:?}", cursor);

        Ok(())
    }

    fn on_unload(&mut self) {}

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => {
                self.set_account(
                    slot,
                    account.pubkey,
                    account.data,
                    account.owner,
                    account.write_version,
                    account.lamports == 0,
                    is_startup,
                );
            }

            ReplicaAccountInfoVersions::V0_0_2(account) => {
                self.set_account(
                    slot,
                    account.pubkey,
                    account.data,
                    account.owner,
                    account.write_version,
                    account.lamports == 0,
                    is_startup,
                );
            }

            ReplicaAccountInfoVersions::V0_0_3(account) => {
                self.set_account(
                    slot,
                    account.pubkey,
                    account.data,
                    account.owner,
                    account.write_version,
                    account.lamports == 0,
                    is_startup,
                );
            }
        }

        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        info!(
            "preloaded account data hash count: {}",
            self.state.read().unwrap().get_hash_count()
        );
        info!("end of startup");
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: u64,
        _parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        println!("GRRRRR: BLOCK STATUS ({}) {}", slot, status.as_str());

        match status {
            SlotStatus::Processed => match self.send_processed {
                true => {
                    debug!(
                        "slot processed {} (parent: {}) acting as confirmed",
                        slot,
                        _parent.unwrap_or_default()
                    );
                    let mut lock_state = self.state.write().unwrap();
                    lock_state.set_confirmed_slot(slot);
                }
                false => {
                    debug!(
                        "slot processed {} (parent: {}) (noop)",
                        slot,
                        _parent.unwrap_or_default()
                    );
                }
            },
            SlotStatus::Rooted => {
                debug!("slot rooted {}", slot);
                self.state.write().unwrap().set_lib(slot);
            }
            SlotStatus::Confirmed => match self.send_processed {
                true => {
                    debug!(
                        "slot confirmed {} (parent: {}) (noop)",
                        slot,
                        _parent.unwrap_or_default()
                    );
                }
                false => {
                    debug!(
                        "slot confirmed {} (parent: {})",
                        slot,
                        _parent.unwrap_or_default()
                    );
                    let mut lock_state = self.state.write().unwrap();
                    lock_state.set_confirmed_slot(slot);
                }
            },
        }

        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> PluginResult<()> {
        match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(tx) => {
                panic!("V0_0_1 not supported");
            }
            ReplicaTransactionInfoVersions::V0_0_2(tx) => {
                println!("GRRRRR: TRX ({}) - {}", slot, tx.signature);
            }
        }

        Ok(())
    }

    fn notify_entry(&self, _entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        Ok(())
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions<'_>) -> PluginResult<()> {
        match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(_) => {
                panic!("V0_0_1 not supported");
            }
            ReplicaBlockInfoVersions::V0_0_2(blockinfo) => {
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap()),
                };

                let mut lock_state = self.state.write().unwrap();
                lock_state.set_block_info(blockinfo.slot, block_info);
                println!("GRRRRR: BLOCK ({}) ", blockinfo.slot);
            }
            ReplicaBlockInfoVersions::V0_0_3(blockinfo) => {
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap()),
                };

                let mut lock_state = self.state.write().unwrap();
                lock_state.set_block_info(blockinfo.slot, block_info);
                println!("GRRRRR: BLOCK ({}) ", blockinfo.slot);
            }

            ReplicaBlockInfoVersions::V0_0_4(blockinfo) => {
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap()),
                };

                let mut lock_state = self.state.write().unwrap();
                lock_state.set_block_info(blockinfo.slot, block_info);
                println!("GRRRRR: BLOCK META ({}) ", blockinfo.slot);
            }
        }

        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        false
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = Plugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
