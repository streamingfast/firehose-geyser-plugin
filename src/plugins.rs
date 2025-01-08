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
    MessageAddressTableLookup, MessageHeader, ReturnData, Reward, RewardType, TokenBalance,
    Transaction, TransactionError, TransactionStatusMeta, UiTokenAmount,
};

use crate::state::{ACC_MUTEX, BLOCK_MUTEX};
use crate::utils::convert_sol_timestamp;
use env_logger::Target;
use log::{debug, info, LevelFilter};
use solana_rpc_client::rpc_client::RpcClient;

use crate::block_printer::BlockPrinter;

use solana_sdk::hash::Hash;
use solana_sdk::message::v0::LoadedAddresses;
use solana_sdk::message::AccountKeys;
use solana_sdk::transaction_context::TransactionReturnData;
use std::fmt;
use std::fs::OpenOptions;
use std::str::FromStr;

const SEED: i64 = 76;

#[derive(Clone)]
pub struct ConfirmTransactionWithIndex {
    pub index: usize,
    pub transaction: ConfirmedTransaction,
}

pub struct Plugin {
    state: Option<RwLock<State>>,
    send_processed: bool,
    trace: bool,
    with_block: bool,
    with_account: bool,
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
            with_account: true, // in case account_data_notifications_enabled gets called before on_load
            with_block: true, // in case transaction_notifications_enabled gets called before on_load
        }
    }
    const VOTE111111111111111111111111111111111111111: [u8; 32] = [
        0x07, 0x61, 0x48, 0x1d, 0x35, 0x74, 0x74, 0xbb, 0x7c, 0x4d, 0x76, 0x24, 0xeb, 0xd3, 0xbd,
        0xb3, 0xd8, 0x35, 0x5e, 0x73, 0xd1, 0x10, 0x43, 0xfc, 0x0d, 0xa3, 0x53, 0x80, 0x00, 0x00,
        0x00, 0x00,
    ];

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
        if owner == Self::VOTE111111111111111111111111111111111111111 {
            return;
        }

        let mut lock_state = self
            .state
            .as_ref()
            .expect("cannot get RW lock for set_account (state is None)")
            .write()
            .expect("cannot get RW lock for set_account (poisoned)");

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

        let blk_file = match plugin_config.block_destination_file.as_str() {
            "" => {
                self.with_block = false;
                None
            }
            _ => {
                self.with_block = true;
                Some(
                    OpenOptions::new()
                        .write(true)
                        .open(plugin_config.block_destination_file)
                        .expect("Failed to open FIFO for blocks"),
                )
            }
        };

        let acc_blk_file = match plugin_config.account_block_destination_file.as_str() {
            "" => {
                self.with_account = false;
                None
            }
            _ => {
                self.with_account = true;
                Some(
                    OpenOptions::new()
                        .write(true)
                        .open(plugin_config.account_block_destination_file)
                        .expect("Failed to open FIFO for account_blocks"),
                )
            }
        };
        if self.with_account && self.with_block {
            info!("processing blocks and accountBlocks...");
        } else if self.with_account {
            info!("processing accountBlocks only (no block)...");
        } else if self.with_block {
            info!("processing blocks only (no accountsBlocks)...");
        } else {
            info!("no processing enabled...");
        }

        let mut printer = BlockPrinter::new(blk_file, acc_blk_file, plugin_config.noop);
        printer
            .print_init("sf.solana.type.v1.Block", "sf.solana.type.v1.AccountBlock")
            .expect("Failed to print init");

        self.state = Some(RwLock::new(State::new(
            local_rpc_client,
            remote_rpc_client,
            cursor,
            plugin_config.cursor_file,
            printer,
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
        if !self.with_account {
            return Ok(());
        }
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
                .expect("cannot get state while getting hash count (state is None)")
                .read()
                .expect("cannot get state while getting hash count (poisoned)")
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
        if ACC_MUTEX.is_poisoned() || BLOCK_MUTEX.is_poisoned() {
            panic!("poisoned mutex")
        }
        match status {
            SlotStatus::Processed => match self.send_processed {
                true => {
                    debug!(
                        "slot processed {} (parent: {}) acting as confirmed",
                        slot,
                        _parent.unwrap_or_default()
                    );
                    let mut lock_state = self
                        .state
                        .as_ref()
                        .expect("cannot get RW lock for update_slot_status (state is None)")
                        .write()
                        .expect("cannot get RW lock for update_slot_status (poisoned)");
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
                self.state
                    .as_ref()
                    .expect("cannot get RW lock for set_lib (state is None)")
                    .write()
                    .expect("cannot get RW lock for set_lib (poisoned)")
                    .set_lib(slot);
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
                    let mut lock_state = self
                        .state
                        .as_ref()
                        .expect("cannot get RW lock for set_confirmed_slot (state is None)")
                        .write()
                        .expect("cannot get RW lock for set_confirmed_slot (poisoned)");
                    lock_state.set_confirmed_slot(slot);
                    if lock_state.process_upto(slot).is_err() {
                        panic!("poisoned mutex")
                    }
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
        if !self.with_block {
            return Ok(());
        }
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

        let mut lock_state = self
            .state
            .as_ref()
            .expect("cannot get RW lock for update_slot_status (state is None)")
            .write()
            .expect("cannot get RW lock for update_slot_status (poisoned)");
        lock_state.set_transaction(slot, tx);

        Ok(())
    }

    fn notify_entry(&self, _entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        Ok(())
    }

    fn notify_block_metadata(&self, block_info: ReplicaBlockInfoVersions<'_>) -> PluginResult<()> {
        if ACC_MUTEX.is_poisoned() || BLOCK_MUTEX.is_poisoned() {
            panic!("poisoned mutex")
        }

        let block_info = match block_info {
            ReplicaBlockInfoVersions::V0_0_1(_) => {
                panic!("V0_0_1 not supported");
            }
            ReplicaBlockInfoVersions::V0_0_2(blockinfo) => BlockInfo {
                block_hash: blockinfo.blockhash.to_string(),
                parent_hash: blockinfo.parent_blockhash.to_string(),
                parent_slot: blockinfo.parent_slot,
                slot: blockinfo.slot,
                height: blockinfo.block_height,
                timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap_or_default()),
                rewards: to_block_rewards_from_vec(blockinfo.rewards),
            },

            ReplicaBlockInfoVersions::V0_0_3(blockinfo) => BlockInfo {
                block_hash: blockinfo.blockhash.to_string(),
                parent_hash: blockinfo.parent_blockhash.to_string(),
                parent_slot: blockinfo.parent_slot,
                slot: blockinfo.slot,
                height: blockinfo.block_height,
                timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap_or_default()),
                rewards: to_block_rewards_from_vec(blockinfo.rewards),
            },

            ReplicaBlockInfoVersions::V0_0_4(blockinfo) => BlockInfo {
                block_hash: blockinfo.blockhash.to_string(),
                parent_hash: blockinfo.parent_blockhash.to_string(),
                parent_slot: blockinfo.parent_slot,
                slot: blockinfo.slot,
                height: blockinfo.block_height,
                timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap_or_default()),
                rewards: to_block_rewards(&Some(blockinfo.rewards.rewards.clone())),
            },
        };
        let slot = block_info.slot;

        let mut lock_state = self
            .state
            .as_ref()
            .expect("state is None while updating slot status")
            .write()
            .expect("rw mutex poisoned while updating slot status");

        lock_state.set_block_info(block_info);

        if lock_state.is_slot_confirmed(slot) {
            if lock_state.process_upto(slot).is_err() {
                panic!("poisoned mutex")
            }
        }

        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        self.with_account
    }

    fn transaction_notifications_enabled(&self) -> bool {
        self.with_block
    }

    fn entry_notifications_enabled(&self) -> bool {
        false
    }
}

pub fn to_block_rewards_from_vec(rewards: &[solana_transaction_status::Reward]) -> Vec<Reward> {
    rewards
        .iter()
        .map(|rw| {
            let commission = match rw.commission.unwrap_or_default().to_string() {
                c if c == "0" => String::new(),
                c => c,
            };
            Reward {
                pubkey: rw.pubkey.clone(),
                lamports: rw.lamports,
                post_balance: rw.post_balance,
                reward_type: to_pb_reward_type(rw.reward_type) as i32,
                commission,
            }
        })
        .collect()
}

pub fn to_block_rewards(rewards: &Option<solana_transaction_status::Rewards>) -> Vec<Reward> {
    match rewards {
        None => {
            vec![]
        }

        Some(rewards) => rewards
            .iter()
            .map(|rw| {
                let commission = match rw.commission.unwrap_or_default().to_string() {
                    c if c == "0" => String::new(),
                    c => c,
                };
                Reward {
                    pubkey: rw.pubkey.clone(),
                    lamports: rw.lamports,
                    post_balance: rw.post_balance,
                    reward_type: to_pb_reward_type(rw.reward_type) as i32,
                    commission,
                }
            })
            .collect(),
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
        transaction: Some(to_transaction(
            tx.transaction,
            &tx.transaction_status_meta.loaded_addresses,
        )),
        meta: Some(to_transaction_meta_status(tx.transaction_status_meta)),
    }
}

fn to_transaction_meta_status(
    status: &solana_transaction_status::TransactionStatusMeta,
) -> TransactionStatusMeta {
    TransactionStatusMeta {
        err: to_transaction_err(status),
        fee: status.fee,
        pre_balances: status.pre_balances.to_vec(),
        post_balances: status.post_balances.to_vec(),
        inner_instructions: to_inner_instructions(&status.inner_instructions),
        log_messages: to_log_messages(&status.log_messages),
        pre_token_balances: to_token_balances(&status.pre_token_balances),
        post_token_balances: to_token_balances(&status.post_token_balances),
        rewards: to_rewards(&status.rewards),
        loaded_writable_addresses: status
            .loaded_addresses
            .writable
            .iter()
            .map(|pubkey| pubkey.to_bytes().to_vec())
            .collect(),
        loaded_readonly_addresses: status
            .loaded_addresses
            .readonly
            .iter()
            .map(|pubkey| pubkey.to_bytes().to_vec())
            .collect(),
        return_data: to_return_data(&status.return_data),
        compute_units_consumed: status.compute_units_consumed,
    }
}

fn to_token_balances(
    balances: &Option<Vec<solana_transaction_status::TransactionTokenBalance>>,
) -> Vec<TokenBalance> {
    balances
        .as_ref()
        .map(|balances_vec| {
            balances_vec
                .iter()
                .map(|balance| TokenBalance {
                    account_index: balance.account_index as u32,
                    mint: balance.mint.clone(),
                    owner: balance.owner.clone(),
                    program_id: balance.program_id.clone(),
                    ui_token_amount: Some(UiTokenAmount {
                        ui_amount: balance.ui_token_amount.ui_amount.unwrap_or_default(),
                        decimals: balance.ui_token_amount.decimals as u32,
                        amount: balance.ui_token_amount.amount.clone(),
                        ui_amount_string: balance.ui_token_amount.ui_amount_string.clone(),
                    }),
                })
                .collect()
        })
        .unwrap_or_else(Vec::new)
}

fn to_log_messages(logs: &Option<Vec<String>>) -> Vec<String> {
    match logs {
        Some(logs) => logs.clone(),
        None => vec![],
    }
}

fn to_transaction_err(
    status: &solana_transaction_status::TransactionStatusMeta,
) -> Option<TransactionError> {
    match &status.status {
        Ok(_) => None,
        Err(e) => {
            let bytes = bincode::serialize(e).expect("error serializing TransactionError");
            let err = TransactionError { err: bytes };
            Some(err)
        }
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
                    reward_type: to_pb_reward_type(rw.reward_type) as i32, // SCARY
                    commission: "".to_string(), //was not set in the poller to keep compatibility
                })
                .collect()
        })
        .unwrap_or_else(Vec::new)
}

fn to_pb_reward_type(reward_type: Option<solana_transaction_status::RewardType>) -> RewardType {
    match reward_type {
        None => RewardType::Unspecified,
        Some(solana_transaction_status::RewardType::Fee) => RewardType::Fee,
        Some(solana_transaction_status::RewardType::Rent) => RewardType::Rent,
        Some(solana_transaction_status::RewardType::Voting) => RewardType::Voting,
        Some(solana_transaction_status::RewardType::Staking) => RewardType::Staking,
    }
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

fn to_transaction(
    tx: &solana_sdk::transaction::SanitizedTransaction,
    loaded_addresses: &LoadedAddresses,
) -> Transaction {
    Transaction {
        signatures: to_signature(tx.signatures()),
        message: Some(to_message(tx.message(), loaded_addresses)),
    }
}

fn to_message(
    msg: &solana_sdk::message::SanitizedMessage,
    loaded_addresses: &LoadedAddresses,
) -> Message {
    Message {
        header: Some(to_header(msg.header())),
        account_keys: to_account_keys(msg.account_keys(), loaded_addresses),
        recent_blockhash: to_recent_block_hash(msg.recent_blockhash()),
        instructions: to_compiled_instructions(msg.instructions()),
        versioned: msg.legacy_message().is_none(),
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

fn to_account_keys(keys: AccountKeys, loaded_addresses: &LoadedAddresses) -> Vec<Vec<u8>> {
    // Create a HashSet of all loaded addresses (address lookup table)
    let lookup_keys: std::collections::HashSet<_> = loaded_addresses
        .writable
        .iter()
        .chain(loaded_addresses.readonly.iter())
        .collect();

    // Filter and convert account keys
    keys.iter()
        .filter(|key| !lookup_keys.contains(key))
        .map(|key| key.to_bytes().to_vec())
        .collect()
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
