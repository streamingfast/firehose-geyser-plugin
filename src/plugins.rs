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
        println!("on load");
        println!("FIRE INIT 3.0 sf.solana.type.v1.AccountBlock");
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
        println!("end of startup");
        println!("end of startup");
        println!("end of startup");
        println!("end of startup");
        println!("end of startup");
        println!("end of startup");
        println!("end of startup");
        println!("end of startup");
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
            SlotStatus::Processed => {
                println!("slot processed {}", slot);
            }
            SlotStatus::Rooted => {
                println!("slot rooted {}", slot);
                lock_state.set_last_finalized_block(slot);
            }
            SlotStatus::Confirmed => {
                println!("slot confirmed {}", slot);

                lock_state.set_last_confirmed_block(slot);

                let block_info = match lock_state.get_block_info(slot) {
                    None => {
                        println!("No block info for slot {}, ignoring for now", slot);
                        lock_state.set_confirmed_slot(slot);
                        return Ok(());
                    }
                    Some(block_info) => block_info
                };


                let account_changes = lock_state.get_account_changes(slot);

                let acc_block = create_account_block(slot, lock_state.get_last_finalized_block(slot), account_changes.unwrap_or(&AccountChanges::default()), block_info);
                BlockPrinter::new(&acc_block).print();

                lock_state.purge_blocks_up_to(slot);

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
        let mut slot= 0;
        match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(_) => {
                panic!("V0_0_1 not supported");
            },
            ReplicaBlockInfoVersions::V0_0_2(blockinfo) => {
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap())
                };
                slot = blockinfo.slot;
                
                self.state.write().unwrap().set_block_info(blockinfo.slot, block_info)
            },
            ReplicaBlockInfoVersions::V0_0_3(blockinfo) => {
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap())
                };
                slot = blockinfo.slot;
                
                self.state.write().unwrap().set_block_info(blockinfo.slot, block_info)
            },
            
            ReplicaBlockInfoVersions::V0_0_4(blockinfo) => {
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap())
                };
                slot = blockinfo.slot;

                self.state.write().unwrap().set_block_info(blockinfo.slot, block_info)
            },

            _ => {
                panic!("Unsupported block version");
            }

        }

        println!("blockmeta {}", slot);

        // backprocess slots that were already confirmed up to this block
        let mut lock_state = self.state.write().unwrap();
        for toproc in lock_state.ordered_confirmed_slots_up_to(slot) {
            let block_info = match lock_state.get_block_info(toproc) {
                None => {
                    println!("No block info for slot {} after processing {}", toproc, slot);
                    continue;
                }
                Some(block_info) => block_info
            };

            let account_changes = lock_state.get_account_changes(slot);

            let acc_block = create_account_block(slot, lock_state.get_last_finalized_block(slot), account_changes.unwrap_or(&AccountChanges::default()), block_info);
            BlockPrinter::new(&acc_block).print();
            lock_state.purge_blocks_up_to(toproc);
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