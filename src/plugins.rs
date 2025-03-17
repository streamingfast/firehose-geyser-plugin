use agave_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaTransactionInfoV2, SlotStatus,
};
use bs58;
use {
    crate::{config::Config as PluginConfig, state::BlockInfo, state::State},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult,
    },
    std::{concat, env, sync::RwLock},
};

use crate::pb::sf::solana::r#type::v1::{
    CompiledInstruction, ConfirmedTransaction, InnerInstruction, InnerInstructions, Message,
    MessageAddressTableLookup, MessageHeader, ReturnData, Reward, RewardType, TokenBalance,
    Transaction, TransactionError, TransactionStatusMeta, UiTokenAmount,
};

use crate::utils::convert_sol_timestamp;
use env_logger::Target;
use log::{debug, LevelFilter};

use solana_sdk::hash::Hash;
use solana_sdk::message::v0::LoadedAddresses;
use solana_sdk::message::AccountKeys;
use solana_sdk::transaction_context::TransactionReturnData;
use std::fmt;
use std::fs::OpenOptions;

const SEED: i64 = 76;

#[derive(Clone)]
pub struct ConfirmTransactionWithIndex {
    pub index: usize,
    pub transaction: ConfirmedTransaction,
}

pub struct Plugin {
    state: Option<RwLock<State>>,
    with_block: bool,
    with_account: bool,
    with_sampling: bool,
    sampling_rate: u8,
    print_startup: bool,
}

impl fmt::Debug for Plugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Plugin").finish()
    }
}

//const VOTE111111111111111111111111111111111111111: [u8; 32] = [
//    0x07, 0x61, 0x48, 0x1d, 0x35, 0x74, 0x74, 0xbb, 0x7c, 0x4d, 0x76, 0x24, 0xeb, 0xd3, 0xbd, 0xb3,
//    0xd8, 0x35, 0x5e, 0x73, 0xd1, 0x10, 0x43, 0xfc, 0x0d, 0xa3, 0x53, 0x80, 0x00, 0x00, 0x00, 0x00,
//];

impl Plugin {
    pub fn new() -> Self {
        Plugin {
            state: None,
            with_account: true,
            with_block: true,
            print_startup: false,
            with_sampling: false,
            sampling_rate: 0,
        }
    }
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        env_logger::Builder::new()
            .filter_level(LevelFilter::Debug)
            .format_timestamp_nanos()
            .target(Target::Stdout)
            .init();

        let plugin_config = PluginConfig::load_from_file(config_file)?;

        // Open output file for writing
        debug!(
            "Opening output file for writing: {}",
            plugin_config.output_file
        );
        match OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&plugin_config.output_file)
        {
            Ok(f) => {
                debug!(
                    "Successfully opened output file: {}",
                    plugin_config.output_file
                );

                self.state = Some(RwLock::new(State::new(f)));
            }
            Err(e) => return Err(e.into()),
        }

        self.print_startup = plugin_config.print_startup;
        self.with_sampling = plugin_config.sampling_rate > 0;
        self.sampling_rate = (255u32 * plugin_config.sampling_rate as u32 / 100u32) as u8;

        debug!(
            "on load, dumb-printer-mode, sampling: {}",
            self.sampling_rate
        );

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
        if is_startup && !self.print_startup {
            return Ok(());
        }

        let mut state_rw = self
            .state
            .as_ref()
            .expect("cannot get RW lock for update_account (state is None)")
            .write()
            .expect("cannot get RW lock for update_account (poisoned)");

        match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => {
                if self.with_sampling {
                    // Basic hash of the pubkey to help with sampling
                    let hash_value: u8 = account
                        .pubkey
                        .iter()
                        .fold(SEED as u8, |acc, &x| acc.wrapping_add(x));
                    if hash_value > self.sampling_rate {
                        return Ok(());
                    }
                }

                let pubkey_as_base58 = bs58::encode(&account.pubkey).into_string();
                state_rw
                    .write(format!(
                        "a:{}:{}:{}{}",
                        slot,
                        &pubkey_as_base58,
                        if is_startup { ":s" } else { "" },
                        account.data.len(),
                    ))
                    .unwrap();
            }

            ReplicaAccountInfoVersions::V0_0_2(account) => {
                if self.with_sampling {
                    // Basic hash of the pubkey to help with sampling
                    let hash_value: u8 = account
                        .pubkey
                        .iter()
                        .fold(SEED as u8, |acc, &x| acc.wrapping_add(x));
                    if hash_value > self.sampling_rate {
                        return Ok(());
                    }
                }

                let pubkey_as_base58 = bs58::encode(&account.pubkey).into_string();

                state_rw
                    .write(format!(
                        "a:{}:{}:{}{}",
                        slot,
                        &pubkey_as_base58,
                        if is_startup { ":s" } else { "" },
                        account.data.len(),
                    ))
                    .unwrap();
            }

            ReplicaAccountInfoVersions::V0_0_3(account) => {
                if self.with_sampling {
                    // Basic hash of the pubkey to help with sampling
                    let hash_value: u8 = account
                        .pubkey
                        .iter()
                        .fold(SEED as u8, |acc, &x| acc.wrapping_add(x));
                    if hash_value > self.sampling_rate {
                        return Ok(());
                    }
                }

                let pubkey_as_base58 = bs58::encode(&account.pubkey).into_string();

                state_rw
                    .write(format!(
                        "a:{}:{}:{}{}",
                        slot,
                        &pubkey_as_base58,
                        if is_startup { ":s" } else { "" },
                        account.data.len(),
                    ))
                    .unwrap();
            }
        }

        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        debug!("end of startup");
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: u64,
        _parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        let mut state_rw = self
            .state
            .as_ref()
            .expect("cannot get RW lock for update_account (state is None)")
            .write()
            .expect("cannot get RW lock for update_account (poisoned)");

        match status {
            SlotStatus::Processed => {
                state_rw.write(format!("s:{}:p", slot,)).unwrap();
            }

            SlotStatus::Rooted => {
                state_rw.write(format!("s:{}:r", slot,)).unwrap();
            }
            SlotStatus::Confirmed => {
                state_rw.write(format!("s:{}:c", slot,)).unwrap();
            }
        }
        Ok(())
    }

    fn notify_transaction(
        &self,
        _transaction: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> PluginResult<()> {
        if self.with_block {
            self.state
                .as_ref()
                .expect("cannot get RW lock for update_account (state is None)")
                .write()
                .expect("cannot get RW lock for update_account (poisoned)")
                .write(format!("t:{}", slot))
                .unwrap();
        }
        Ok(())
    }

    fn notify_entry(&self, _entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        Ok(())
    }

    fn notify_block_metadata(&self, block_info: ReplicaBlockInfoVersions<'_>) -> PluginResult<()> {
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
                transaction_count: blockinfo.executed_transaction_count,
            },

            ReplicaBlockInfoVersions::V0_0_3(blockinfo) => BlockInfo {
                block_hash: blockinfo.blockhash.to_string(),
                parent_hash: blockinfo.parent_blockhash.to_string(),
                parent_slot: blockinfo.parent_slot,
                slot: blockinfo.slot,
                height: blockinfo.block_height,
                timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap_or_default()),
                rewards: to_block_rewards_from_vec(blockinfo.rewards),
                transaction_count: blockinfo.executed_transaction_count,
            },

            ReplicaBlockInfoVersions::V0_0_4(blockinfo) => BlockInfo {
                block_hash: blockinfo.blockhash.to_string(),
                parent_hash: blockinfo.parent_blockhash.to_string(),
                parent_slot: blockinfo.parent_slot,
                slot: blockinfo.slot,
                height: blockinfo.block_height,
                timestamp: convert_sol_timestamp(blockinfo.block_time.unwrap_or_default()),
                rewards: to_block_rewards(&Some(blockinfo.rewards.rewards.clone())),
                transaction_count: blockinfo.executed_transaction_count,
            },
        };

        self.state
            .as_ref()
            .expect("cannot get RW lock for update_account (state is None)")
            .write()
            .expect("cannot get RW lock for update_account (poisoned)")
            .write(format!(
                "b:{}:{}:{}:{}",
                block_info.slot,
                if block_info.block_hash.len() >= 8 {
                    &block_info.block_hash[0..8]
                } else {
                    &block_info.block_hash
                },
                if block_info.parent_hash.len() >= 8 {
                    &block_info.parent_hash[0..8]
                } else {
                    &block_info.parent_hash
                },
                block_info.transaction_count,
            ))
            .unwrap();

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
    let plugin = Plugin::new();
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
