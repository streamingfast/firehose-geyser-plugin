use agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use base58::ToBase58;
use solana_sdk::signer::Signer;
use {
    crate::{config::Config as PluginConfig, state::BlockInfo, state::State},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult,
    },
    std::{concat, env, sync::RwLock},
};

use crate::pb;
use crate::state::AccountWithWriteVersion;
use crate::utils::convert_sol_timestamp;
use env_logger::Target;
use log::{debug, info, LevelFilter};
use pb::sf::solana::r#type::v1::Account;
use solana_rpc_client::rpc_client::RpcClient;
use std::fmt;
use std::str::FromStr;

const VOTE_ACCOUNT: &str = "Vote111111111111111111111111111111111111111";
const MY_ACCOUNT: &str = "4K7V3sSDGN2MaAD9runRWekXRDffADQsE6CiER6w69dN";
const DERIVED_ACCOUNT: &str = "9QiiQiqg2riRns9CAuVvgFsAQ1RM6CH38EFysZ6R8Nac";


#[derive(Default)]
pub struct Plugin {
    state: RwLock<State>,
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

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        let plugin_config = PluginConfig::load_from_file(config_file)?;

        let filter_level =
            LevelFilter::from_str(plugin_config.log.level.as_str()).unwrap_or(LevelFilter::Info);
        env_logger::Builder::new()
            .filter_level(filter_level)
            .format_timestamp_nanos()
            .target(Target::Stdout)
            .init();

        debug!("on load");

        let local_rpc_client = RpcClient::new(plugin_config.local_rpc_client.endpoint);
        let remote_rpc_client = RpcClient::new(plugin_config.remote_rpc_client.endpoint);
        let cursor = cursor_from_file(&plugin_config.cursor_file);

        self.state = RwLock::new(State::new(
            local_rpc_client,
            remote_rpc_client,
            cursor,
            plugin_config.cursor_file,
        ));

        println!("FIRE INIT 3.0 sf.solana.type.v1.AccountBlock");

        Ok(())
    }

    fn on_unload(&mut self) {}

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        if is_startup {
            return Ok(());
        }

        let mut lock_state = self.state.write().unwrap();

        match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => {

                if account.pubkey.to_base58() == MY_ACCOUNT || 
                    account.owner.to_base58() == MY_ACCOUNT ||
                    account.pubkey.to_base58() == DERIVED_ACCOUNT ||
                    account.owner.to_base58() == DERIVED_ACCOUNT {

                    debug!("received my account: {} (owner: {}) on slot {}", 
                        account.owner.to_base58(),
                        account.pubkey.to_base58(),
                        slot);
                }
                if account.owner.to_base58() == VOTE_ACCOUNT {
                    return Ok(());
                }
                let account_key = account.pubkey.to_vec();
                let pb_account = Account {
                    address: account.pubkey.to_vec(),
                    data: account.data.to_vec(),
                    owner: account.owner.to_vec(),
                    deleted: account.lamports == 0,
                };

                let awv = AccountWithWriteVersion {
                    account: pb_account,
                    write_version: account.write_version,
                };

                lock_state.set_account(slot, account_key, awv);
            }

            ReplicaAccountInfoVersions::V0_0_2(account) => {
                if account.owner.to_base58() == VOTE_ACCOUNT {
                    return Ok(());
                }
                let account_key = account.pubkey.to_vec();
                let pb_account = Account {
                    address: account.pubkey.to_vec(),
                    data: account.data.to_vec(),
                    owner: account.owner.to_vec(),
                    deleted: account.lamports == 0,
                };
                let awv = AccountWithWriteVersion {
                    account: pb_account,
                    write_version: account.write_version,
                };

                lock_state.set_account(slot, account_key, awv);
            }

            ReplicaAccountInfoVersions::V0_0_3(account) => {
                if account.owner.to_base58() == VOTE_ACCOUNT {
                    return Ok(());
                }
                let account_key = account.pubkey.to_vec();
                let pb_account = Account {
                    address: account.pubkey.to_vec(),
                    data: account.data.to_vec(),
                    owner: account.owner.to_vec(),
                    deleted: account.lamports == 0,
                };

                let awv = AccountWithWriteVersion {
                    account: pb_account,
                    write_version: account.write_version,
                };

                lock_state.set_account(slot, account_key, awv);
            }
        }

        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        info!("end of startup");
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: u64,
        _parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        match status {
            SlotStatus::Processed => {
                debug!("slot processed {}", slot);
            }
            SlotStatus::Rooted => {
                debug!("slot rooted {}", slot);
                self.state.write().unwrap().set_lib(slot);
            }
            SlotStatus::Confirmed => {
                debug!("slot confirmed {}", slot);
                let mut lock_state = self.state.write().unwrap();
                lock_state.set_confirmed_slot(slot);
            }
        }

        Ok(())
    }

    fn notify_transaction(
        &self,
        _transaction: ReplicaTransactionInfoVersions<'_>,
        _slot: u64,
    ) -> PluginResult<()> {
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
        true
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
