use agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use {
    crate::{
        block_printer::BlockPrinter,
        config::Config as PluginConfig,
        state::AccountChanges,
        state::BlockInfo,
        state::State,
        utils::{convert_sol_timestamp, create_account_block},
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult,
    },
    std::{concat, env, sync::RwLock},
};

use crate::pb;
use pb::sf::solana::r#type::v1::Account;
use solana_rpc_client::rpc_client::RpcClient;
use std::fmt;

#[derive(Default)]
pub struct Plugin {
    state: RwLock<State>,
}

impl fmt::Debug for Plugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Plugin")
            .field("rpc_client", &"RpcClient") // Omit details of `RpcClient`
            .finish()
    }
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        println!("on load");
        println!("FIRE INIT 3.0 sf.solana.type.v1.AccountBlock");

        let plugin_config = PluginConfig::load_from_file(config_file)?;
        let rpc_client = RpcClient::new(plugin_config.rpc_client.endpoint);

        self.state = RwLock::new(State::new(rpc_client));

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
            // we never process those, we don't even want them.
            return Ok(());
        }

        self.state
            .write()
            .unwrap()
            .set_last_finalized_block_from_rpc();
        match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => {
                let account_key = account.pubkey.to_vec();
                let account = Account {
                    address: account.pubkey.to_vec(),
                    data: account.data.to_vec(),
                    owner: account.owner.to_vec(),
                    write_version: account.write_version,
                    source_slot: slot,
                    rent_epoch: account.rent_epoch,
                };

                self.state
                    .write()
                    .unwrap()
                    .set_account(slot, account_key, account);
            }

            ReplicaAccountInfoVersions::V0_0_2(account) => {
                let account_key = account.pubkey.to_vec();
                let account = Account {
                    address: account.pubkey.to_vec(),
                    data: account.data.to_vec(),
                    owner: account.owner.to_vec(),
                    write_version: account.write_version,
                    source_slot: slot,
                    rent_epoch: account.rent_epoch,
                };

                self.state
                    .write()
                    .unwrap()
                    .set_account(slot, account_key, account);
            }

            ReplicaAccountInfoVersions::V0_0_3(account) => {
                let account_key = account.pubkey.to_vec();
                let account = Account {
                    address: account.pubkey.to_vec(),
                    data: account.data.to_vec(),
                    owner: account.owner.to_vec(),
                    write_version: account.write_version,
                    source_slot: slot,
                    rent_epoch: account.rent_epoch,
                };

                self.state
                    .write()
                    .unwrap()
                    .set_account(slot, account_key, account);
            }

            _ => {
                panic!("Unsupported account version");
            }
        }
        let mut lock_state = self.state.write().unwrap();

        // if we have no blockmeta received yet, we truncate our list to the last x blocks to prevent filling up the RAM on catch up
        if !lock_state.get_first_blockmeta_received()
            && lock_state.accounts_len() > 200
            && slot > 200
        {
            println!("Purging blocks up to {}", slot - 200);
            lock_state.purge_blocks_up_to(slot - 200); // this may keep less than 200 blocks because of forked blocks
        }

        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        println!("end of startup");
        Ok(())
    }
    /*
        Order of stuff received

    1. We receive a bunch of account changes (ex: 203, 204, 205, 206, 207, 208...)
    2. We then receive a bunch of slot updates Confirmed: (ex: 205, 206, 207) -- we only keep a max number of those to prevent filling up the RAM
    -- Since we don't have the blockmeta for ALL those blocks, we only add them to the list of confirmed slots
    3. We then receive a first blockmetadata for a block (ex: 208)
    4. Followed by a Slot:Processed (208)
    5. We may receive a few account changes between this step and the next one, ex: 209, 210, 211...
    6. Followed by a Slot:Confirmed (208)
    Then the steps 3-6 repeat, with some "slot:Rooted" sprinkled...

     */

    fn update_slot_status(
        &self,
        slot: u64,
        _parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        match status {
            SlotStatus::Processed => {
                println!("slot processed {}", slot);
            }
            SlotStatus::Rooted => {
                println!("slot rooted {}", slot);
                self.state.write().unwrap().set_last_finalized_block(slot);
            }
            SlotStatus::Confirmed => {
                println!("slot confirmed {}", slot);

                let mut lock_state = self.state.write().unwrap();
                if !lock_state.get_first_blockmeta_received() {
                    println!(
                        "Delaying processing slot {} as we have not received any blockmeta yet",
                        slot
                    );
                    if !lock_state.is_already_purged(slot) {
                        // align with purged account_data
                        lock_state.set_confirmed_slot(slot);
                    }
                    return Ok(());
                }

                if !lock_state.set_last_finalized_block_from_rpc() {
                    println!(
                        "Delaying processing slot {} as we have not set a finalized block yet",
                        slot
                    );
                    lock_state.set_confirmed_slot(slot);
                    return Ok(());
                }

                if lock_state.get_block_info(slot).is_none() {
                    println!("Delaying processing slot {} as we have not received blockmeta for that block yet", slot);
                    lock_state.set_confirmed_slot(slot);
                    return Ok(());
                }

                if !lock_state.backprocess_below(slot) {
                    println!(
                        "Delaying processing slot {} as we have not backprocessed everything yet",
                        slot
                    );
                    lock_state.set_confirmed_slot(slot);
                    return Ok(());
                }
                let block_info = lock_state.get_block_info(slot).unwrap();

                let account_changes = lock_state.get_account_changes(slot);
                let acc_block = create_account_block(
                    slot,
                    lock_state.get_last_finalized_block().unwrap(),
                    account_changes.unwrap_or(&AccountChanges::default()),
                    &block_info,
                );
                BlockPrinter::new(&acc_block).print();
                lock_state.purge_blocks_up_to(slot);
            }
            _ => {
                panic!("Unsupported slot status");
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
        let mut slot = 0;
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
                slot = blockinfo.slot;

                self.state
                    .write()
                    .unwrap()
                    .set_block_info(blockinfo.slot, block_info);
            }
            ReplicaBlockInfoVersions::V0_0_3(blockinfo) => {
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap()),
                };
                slot = blockinfo.slot;

                self.state
                    .write()
                    .unwrap()
                    .set_block_info(blockinfo.slot, block_info);
            }

            ReplicaBlockInfoVersions::V0_0_4(blockinfo) => {
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap()),
                };
                slot = blockinfo.slot;

                self.state
                    .write()
                    .unwrap()
                    .set_block_info(blockinfo.slot, block_info);
            }

            _ => {
                panic!("Unsupported block version");
            }
        }

        let mut lock_state = self.state.write().unwrap();
        if !lock_state.set_last_finalized_block_from_rpc() {
            println!("Received blockmeta {}, delaying processing as we have not set a finalized block yet", slot);
            return Ok(());
        }

        println!("received blockmeta {}", slot);
        if !lock_state.backprocess_below(slot) {
            println!("Received blockmeta {}, delaying processing as we have not backprocessed everything yet", slot);
            return Ok(());
        };
        let block_info = lock_state.get_block_info(slot).unwrap();
        if lock_state.is_confirmed_slot(slot) {
            let account_changes = lock_state.get_account_changes(slot);

            let acc_block = create_account_block(
                slot,
                lock_state.get_last_finalized_block().unwrap(),
                account_changes.unwrap_or(&AccountChanges::default()),
                &block_info,
            );
            BlockPrinter::new(&acc_block).print();
            lock_state.purge_blocks_up_to(slot);
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
