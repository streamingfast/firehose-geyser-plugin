use agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use {
    crate::{
        state::State,
        state::BlockInfo,
        state::AccountChanges,
        utils::{convert_sol_timestamp, create_account_block},
        block_printer::BlockPrinter,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult,
    },
    std::{
        concat, env, sync::RwLock
    },
};

use crate::pb::sf::solana::r#type::v1::{AccountBlock, Account};
use prost_types::{Any as ProtoAny};
use prost::{Message as ProtoMessage};

#[derive(Debug, Default)]
pub struct Plugin {
    state: RwLock<State>,
}

impl Plugin {
}


impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, config_file: &str, is_reload: bool) -> PluginResult<()> {
        println!("ON_LOADING WITH SOL ACCOUNTS PLUGIN");
        self.state = RwLock::new(State::new());
        Ok(())
    }

    fn on_unload(&mut self) {
    }

    fn update_account(&self,account: ReplicaAccountInfoVersions, slot: u64, is_startup: bool) -> PluginResult<()> {
        match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => {
                let account_key = account.pubkey.to_vec();
                let account_data = account.data.to_vec();

                self.state.write().unwrap().set_account_data(slot, account_key, account_data);
            },

            ReplicaAccountInfoVersions::V0_0_2(account) => {
                let account_key = account.pubkey.to_vec();
                let account_data = account.data.to_vec();

                self.state.write().unwrap().set_account_data(slot, account_key, account_data);
            },

            ReplicaAccountInfoVersions::V0_0_3(account) => {
                let account_key = account.pubkey.to_vec();
                let account_data = account.data.to_vec();

                self.state.write().unwrap().set_account_data(slot, account_key, account_data);
            },

            _ => {
                panic!("Unsupported account version");
            },

        }

        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        Ok(())
    }
    /* 
        Order of stuff received
    
    1. We receive a bunch of account changes (ex: 203, 204, 205, 206, 207, 208...)
    2. We then receive a bunch of slot updates Confirmed: (ex: 205, 206, 207)
    -- Since we don't have the blockmeta for ALL those blocks, we ignore them for now (TODO: fetch using RPC...)
    3. We then receive a first blockmetadata for a block (ex: 208)
    4. Followed by a Slot:Processed (208)
    5. We may receive a few account changes between this step and the next one, ex: 209, 210, 211...
    6. Followed by a Slot:Confirmed (208)
    Then the steps 3-6 repeat, with some "slot:Rooted" sprinkled...
    
     */

    fn update_slot_status(&self,slot: u64, parent: Option<u64>, status: SlotStatus) -> PluginResult<()> {
        //TODO: Optimize Read/Write Lock
        let mut lock_state = self.state.write().unwrap();

        match status {

            // we ignore the Processed state ?
            SlotStatus::Processed => {
                println!("slot processed {}", slot);
                // TODO: 
                // 1. fix so it doesn't roll over 0
                // 2. fix logic so it can be unset (Option u64 in the set_last_finalized_block)
            //    lock_state.set_last_finalized_block(slot - 31);
            }
            SlotStatus::Rooted => {
                println!("slot rooted {}", slot);
                lock_state.set_last_finalized_block(slot);
            }
            SlotStatus::Confirmed => {
                println!("slot confirmed {}", slot);

                lock_state.set_last_confirmed_block(slot);
                lock_state.stats();
                lock_state.purge_blocks_below(slot);

                let block_info = lock_state.get_block_info(slot);
                if block_info.is_none() {
                    println!("No block info for slot {}, ignoring", slot);
                    return Ok(());
                }
                let block_info = block_info.unwrap();

                // FIXME: how to detect that we have no account changes but the slot is valid ?
                // distinguish this from the "first slot update sent" which also doesn't contain any account changes
                // what if we get "slot update", then another "slot update" to say that there was a fork or something ? is it possible ??
                let account_changes = lock_state.get_account_changes(slot);

                println!("Would write the block here {} ({}) parent: {} ({})", slot, block_info.block_hash, block_info.parent_slot, block_info.parent_hash);
                //TODO : lib_bum should be computed using status::Processed...
                // let lib_num = lock_state.get_last_finalized_block();
                // fix logic so it can be unset (Option u64 in the set_last_finalized_block)

                 let acc_block = create_account_block(slot, slot - 200, account_changes.unwrap_or(&AccountChanges::default()), block_info);
                 let block_printer = BlockPrinter::new(&acc_block);
                 _ = block_printer;
                // block_printer.print();

            }
            _ => {
                panic!("Unsupported slot status");
            }
        }

        Ok(())
    }

    fn notify_transaction(&self,transaction: ReplicaTransactionInfoVersions<'_>, slot: u64) -> PluginResult<()> {
        Ok(())
    }

    fn notify_entry(&self, entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        Ok(())
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions<'_>) -> PluginResult<()> {
        match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(blockinfo) => {
                panic!("V0_0_1 not supported");
            },
            ReplicaBlockInfoVersions::V0_0_2(blockinfo) => {
                println!("SOLACCOUNTPLUGIN block metadata {}", blockinfo.slot);
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap())
                };
                
                self.state.write().unwrap().set_block_info(blockinfo.slot, block_info)
            },
            ReplicaBlockInfoVersions::V0_0_3(blockinfo) => {
                println!("SOLACCOUNTPLUGIN block metadata {}", blockinfo.slot);
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap())
                };
                
                self.state.write().unwrap().set_block_info(blockinfo.slot, block_info)
            },
            
            ReplicaBlockInfoVersions::V0_0_4(blockinfo) => {
                println!("SOLACCOUNTPLUGIN block metadata {}", blockinfo.slot);
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap())
                };

                self.state.write().unwrap().set_block_info(blockinfo.slot, block_info)
            },

            _ => {
                panic!("Unsupported block version");
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