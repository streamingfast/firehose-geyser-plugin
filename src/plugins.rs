use agave_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaTransactionInfoV2, SlotStatus,
};
use {
    crate::{config::Config as PluginConfig, state::BlockInfo, state::State},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult,
    },
    gxhash::gxhash64,
    std::{concat, env, sync::RwLock},
};

use crate::pb::sf::solana::r#type::v1::{
    CompiledInstruction, ConfirmedTransaction, InnerInstruction, InnerInstructions, Message,
    MessageAddressTableLookup, MessageHeader, ReturnData, Reward, Transaction,
    TransactionStatusMeta,
};
use crate::utils::convert_sol_timestamp;
use env_logger::Target;
use log::{debug, info, LevelFilter};
use solana_rpc_client::rpc_client::RpcClient;

use solana_sdk::hash::Hash;
use std::fmt;
use std::fs::OpenOptions;
use std::str::FromStr;

use crate::block_printer::BlockPrinter;
use solana_sdk::message::AccountKeys;
use solana_sdk::transaction_context::TransactionReturnData;

const SEED: i64 = 76;

pub struct ConfirmTransactionWithIndex {
    pub index: usize,
    pub transaction: ConfirmedTransaction,
}

pub struct Plugin {
    state: Option<RwLock<State>>,
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
    pub fn new(send_processed: bool, trace: bool) -> Self {
        Plugin {
            state: None,
            send_processed,
            trace,
        }
    }

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
        let mut lock_state = self.state.as_ref().unwrap().write().unwrap();

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

        let mut account_block_printer = BlockPrinter::new(
            OpenOptions::new()
                .write(true)
                .open(plugin_config.account_block_destination_file)
                .expect("Failed to open FIFO"),
            plugin_config.noop,
        );
        account_block_printer
            .print_init("sf.solana.type.v1.AccountBlock")
            .expect("Failed to print init");

        let mut block_printer = BlockPrinter::new(
            OpenOptions::new()
                .write(true)
                .open(plugin_config.block_destination_file)
                .expect("Failed to open FIFO"),
            plugin_config.noop,
        );
        block_printer
            .print_init("sf.solana.type.v1.Block")
            .expect("Failed to print init");

        self.state = Some(RwLock::new(State::new(
            local_rpc_client,
            remote_rpc_client,
            cursor,
            plugin_config.cursor_file,
            account_block_printer,
            block_printer,
        )));

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
            self.state
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .get_hash_count()
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
                    let mut lock_state = self.state.as_ref().unwrap().write().unwrap();
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
                self.state.as_ref().unwrap().write().unwrap().set_lib(slot);
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
                    let mut lock_state = self.state.as_ref().unwrap().write().unwrap();
                    lock_state.set_confirmed_slot(slot);
                    lock_state.process_upto(slot);
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
        let transaction = match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(_info) => {
                unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
            }
            ReplicaTransactionInfoVersions::V0_0_2(info) => info,
        };

        let compiled_transaction = to_confirm_transaction(&transaction);
        let tx = ConfirmTransactionWithIndex {
            index: transaction.index,
            transaction: compiled_transaction,
        };

        let mut lock_state = self.state.as_ref().unwrap().write().unwrap();
        lock_state.set_transaction(slot, tx);

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
                    height: blockinfo.block_height,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap()),
                };

                let mut lock_state = self.state.as_ref().unwrap().write().unwrap();
                lock_state.set_block_info(blockinfo.slot, block_info);
                if lock_state.is_slot_confirm(blockinfo.slot) {
                    lock_state.process_upto(blockinfo.slot);
                }
            }

            ReplicaBlockInfoVersions::V0_0_3(blockinfo) => {
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    height: blockinfo.block_height,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap()),
                };

                let mut lock_state = self.state.as_ref().unwrap().write().unwrap();
                lock_state.set_block_info(blockinfo.slot, block_info);
                if lock_state.is_slot_confirm(blockinfo.slot) {
                    lock_state.process_upto(blockinfo.slot);
                }
            }

            ReplicaBlockInfoVersions::V0_0_4(blockinfo) => {
                let block_info = BlockInfo {
                    block_hash: blockinfo.blockhash.to_string(),
                    parent_hash: blockinfo.parent_blockhash.to_string(),
                    parent_slot: blockinfo.parent_slot,
                    slot: blockinfo.slot,
                    height: blockinfo.block_height,
                    timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap()),
                };

                let mut lock_state = self.state.as_ref().unwrap().write().unwrap();
                lock_state.set_block_info(blockinfo.slot, block_info);
                if lock_state.is_slot_confirm(blockinfo.slot) {
                    lock_state.process_upto(blockinfo.slot);
                }
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
    let plugin = Plugin::new(false, false);
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}

fn to_confirm_transaction(tx: &'_ ReplicaTransactionInfoV2<'_>) -> ConfirmedTransaction {
    ConfirmedTransaction {
        transaction: Some(to_transaction(tx.transaction)),
        meta: Some(to_transaction_meta_status(tx.transaction_status_meta)),
    }
}

fn to_transaction_meta_status(
    status: &solana_transaction_status::TransactionStatusMeta,
) -> TransactionStatusMeta {
    TransactionStatusMeta {
        err: None, //todo
        fee: 0,
        pre_balances: status.pre_balances.to_vec(),
        post_balances: status.post_balances.to_vec(),
        inner_instructions: to_inner_instructions(&status.inner_instructions),
        log_messages: vec![],       //todo
        pre_token_balances: vec![], //todo
        post_token_balances: vec![],
        rewards: to_rewards(&status.rewards),
        loaded_writable_addresses: vec![], //todo
        loaded_readonly_addresses: vec![], //todo
        return_data: to_return_data(&status.return_data),
        compute_units_consumed: status.compute_units_consumed,
    }
}

fn to_inner_instructions(
    inner_instructions: &Option<Vec<solana_transaction_status::InnerInstructions>>,
) -> Vec<InnerInstructions> {
    match inner_instructions {
        None => {
            vec![]
        }
        Some(instructions) => instructions
            .iter()
            .map(|inner_instruction| InnerInstructions {
                index: inner_instruction.index as u32,
                instructions: inner_instruction
                    .instructions
                    .iter()
                    .map(|instruction| InnerInstruction {
                        program_id_index: instruction.instruction.program_id_index as u32,
                        accounts: instruction.instruction.accounts.to_vec(),
                        data: instruction.instruction.data.clone(),
                        stack_height: instruction.stack_height,
                    })
                    .collect::<Vec<InnerInstruction>>(),
            })
            .collect(),
    }
}

fn to_rewards(rewards: &Option<solana_transaction_status::Rewards>) -> Vec<Reward> {
    rewards
        .as_ref()
        .map(|rws| {
            rws.iter()
                .map(|rw| Reward {
                    pubkey: rw.pubkey.clone(),
                    lamports: rw.lamports,
                    post_balance: rw.post_balance,
                    reward_type: rw.reward_type.unwrap() as i32,
                    commission: "".to_string(), //todo
                })
                .collect()
        })
        .unwrap_or_else(Vec::new)
}

fn to_return_data(d: &Option<TransactionReturnData>) -> Option<ReturnData> {
    match d {
        Some(d) => Some(ReturnData {
            program_id: d.program_id.to_bytes().to_vec(),
            data: d.data.to_vec(),
        }),
        None => None,
    }
}

fn to_transaction(tx: &solana_sdk::transaction::SanitizedTransaction) -> Transaction {
    Transaction {
        signatures: to_signature(tx.signatures()),
        message: Some(to_message(tx.message())),
    }
}

fn to_message(msg: &solana_sdk::message::SanitizedMessage) -> Message {
    Message {
        header: Some(to_header(msg.header())),
        account_keys: to_account_keys(msg.account_keys()),
        recent_blockhash: to_recent_block_hash(msg.recent_blockhash()),
        instructions: to_compiled_instructions(msg.instructions()),
        versioned: true, //todo: What is this?
        address_table_lookups: to_address_table_lookups(msg.message_address_table_lookups()),
    }
}

fn to_address_table_lookups(
    addresses: &[solana_sdk::message::v0::MessageAddressTableLookup],
) -> Vec<MessageAddressTableLookup> {
    addresses
        .iter()
        .map(|lookup| MessageAddressTableLookup {
            account_key: lookup.account_key.to_bytes().to_vec(),
            writable_indexes: lookup.writable_indexes.clone(),
            readonly_indexes: lookup.readonly_indexes.clone(),
        })
        .collect()
}

fn to_compiled_instructions(
    instructions: &[solana_sdk::instruction::CompiledInstruction],
) -> Vec<CompiledInstruction> {
    instructions
        .iter()
        .map(|instruction| CompiledInstruction {
            program_id_index: instruction.program_id_index as u32,
            accounts: instruction.accounts.to_vec(),
            data: instruction.data.to_vec(),
        })
        .collect()
}

fn to_recent_block_hash(h: &Hash) -> Vec<u8> {
    h.as_ref().to_vec()
}

fn to_account_keys(keys: AccountKeys) -> Vec<Vec<u8>> {
    keys.iter().map(|key| key.to_bytes().to_vec()).collect()
}

fn to_header(h: &solana_sdk::message::MessageHeader) -> MessageHeader {
    MessageHeader {
        num_required_signatures: h.num_required_signatures as u32,
        num_readonly_signed_accounts: h.num_readonly_signed_accounts as u32,
        num_readonly_unsigned_accounts: h.num_readonly_unsigned_accounts as u32,
    }
}

fn to_signature(signatures: &[solana_sdk::signature::Signature]) -> Vec<Vec<u8>> {
    signatures
        .iter()
        .map(|signature| signature.as_ref().to_vec())
        .collect()
}
