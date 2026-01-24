use crate::block_printer::BlockPrinter;
use crate::config::DevelopmentConfig;
use crate::pb;
use crate::utils::{convert_sol_timestamp, create_account_block};
use lazy_static::lazy_static;
use pb::sf::solana::r#type::v1::Account;
use prost_types::Timestamp;
use rustc_hash::FxHashMap as HashMap;
use solana_rpc_client::rpc_client::RpcClient;

#[derive(Debug, Clone, PartialEq)]
pub struct AccountDataHashValue {
    pub owner: [u8; 32],
    pub data_hash: u64,
}

type BlockAccountChanges = HashMap<u64, AccountChanges>;
pub type AccountChanges = HashMap<[u8; 32], AccountWithWriteVersion>; // pubkey(32) -> AccountWithWriteVersion
pub type AccountDataHash = HashMap<[u8; 32], AccountDataHashValue>; // pubkey(32) -> AccountDataHashValue
pub type StartupAccountReceivedSlot = HashMap<[u8; 32], u64>; // pubkey(32) -> composite value (slot << 24 + write_version)

pub type Transactions = HashMap<u64, Vec<ConfirmTransactionWithIndex>>;
type ProcessedSlot = HashMap<u64, bool>;

type BlockInfoMap = HashMap<u64, BlockInfo>;
type ConfirmedSlotsMap = HashMap<u64, bool>;
use crate::pb::sf::solana::r#type::v1::{Block, BlockHeight, Reward, UnixTimestamp};
use crate::plugins::{to_block_rewards, ConfirmTransactionWithIndex};
use log::{debug, error, info, warn};
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::bs58;
use solana_transaction_status::TransactionDetails;

#[derive(Debug, Clone, PartialEq)]
pub struct AccountFixed {
    pub address: [u8; 32],
    pub owner: [u8; 32],
    pub data: Vec<u8>,
    pub deleted: bool,
}

impl AccountFixed {
    pub fn to_account(&self) -> Account {
        Account {
            address: self.address.to_vec(),
            owner: self.owner.to_vec(),
            data: self.data.clone(),
            deleted: self.deleted,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AccountWithWriteVersion {
    pub account: AccountFixed,
    pub write_version: u64,
    pub data_hash: u64,
}

lazy_static! {
    pub static ref BLOCK_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());
    pub static ref ACC_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());
    pub static ref CURSOR_MUTEX: std::sync::Mutex<u64> = std::sync::Mutex::new(0);
}

#[derive(Default, Clone)]
pub struct BlockInfo {
    pub slot: u64,
    pub parent_slot: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub timestamp: Timestamp,
    pub height: Option<u64>,
    pub rewards: Vec<Reward>,
    pub transaction_count: u64,
}

const DEFAULT_RPC_BLOCK_CONFIG: RpcBlockConfig = RpcBlockConfig {
    encoding: None,
    transaction_details: Some(TransactionDetails::Signatures),
    rewards: Some(true),
    commitment: Some(CommitmentConfig::confirmed()),
    max_supported_transaction_version: Some(0),
};

pub struct State {
    pub initialized: bool, // passed the first received blockmeta

    pub first_received_blockmeta: Option<u64>,
    pub first_block_to_process: Option<u64>,

    pub last_sent_block: Option<u64>,

    pub cursor: Option<u64>,
    pub lib: Option<u64>,

    pub block_account_changes: BlockAccountChanges,

    pub account_data_hash: AccountDataHash, // only updated when we print the block
    pub startup_received_slot: StartupAccountReceivedSlot, // only used during startup phase
    pub set_account_on_startup_call_count: u64, // counter for logging

    pub block_infos: BlockInfoMap,
    pub confirmed_slots: ConfirmedSlotsMap,

    pub with_block: bool,
    //with_account: bool,
    pub transactions: Transactions,
    pub processed_slots: ProcessedSlot,

    pub cursor_path: String,
    pub dev_config: DevelopmentConfig,

    local_rpc_client: Option<RpcClient>,
    remote_rpc_client: Option<RpcClient>,
    block_printer: BlockPrinter,
}

impl State {
    pub fn new(
        local_rpc_client: RpcClient,
        remote_rpc_client: RpcClient,
        cursor: Option<u64>,
        cursor_path: String,
        block_printer: BlockPrinter,
        with_block: bool,
        dev_config: DevelopmentConfig,
    ) -> Self {
        State {
            cursor,
            first_block_to_process: if dev_config.force_send { Some(0) } else { None },
            first_received_blockmeta: if dev_config.force_send { Some(0) } else { None },
            lib: None,
            initialized: false,

            block_account_changes: HashMap::default(),
            account_data_hash: HashMap::default(),
            startup_received_slot: HashMap::default(),
            set_account_on_startup_call_count: 0,
            block_infos: HashMap::default(),
            confirmed_slots: HashMap::default(),
            last_sent_block: None,

            transactions: HashMap::default(),
            processed_slots: HashMap::default(),

            local_rpc_client: Some(local_rpc_client),
            remote_rpc_client: Some(remote_rpc_client),
            cursor_path,
            block_printer,
            with_block,
            dev_config,
        }
    }

    /// Internal copy for testing purposes, cannot clone RpcClient nor
    /// BlockPrinter which are set respectively to None and a new instance.
    pub(crate) fn internal_copy(&self) -> State {
        State {
            initialized: self.initialized,
            first_received_blockmeta: self.first_received_blockmeta,
            first_block_to_process: self.first_block_to_process,
            last_sent_block: self.last_sent_block,
            cursor: self.cursor,
            lib: self.lib,
            block_account_changes: self.block_account_changes.clone(),
            account_data_hash: self.account_data_hash.clone(),
            startup_received_slot: self.startup_received_slot.clone(),
            set_account_on_startup_call_count: self.set_account_on_startup_call_count,
            block_infos: self.block_infos.clone(),
            confirmed_slots: self.confirmed_slots.clone(),
            with_block: self.with_block,
            transactions: self.transactions.clone(),
            processed_slots: self.processed_slots.clone(),
            cursor_path: self.cursor_path.clone(),
            dev_config: self.dev_config.clone(),

            // Cannot clone those
            local_rpc_client: None,
            remote_rpc_client: None,
            block_printer: BlockPrinter::new(None, None, true),
        }
    }

    fn set_last_finalized_block_from_rpc(&mut self) {
        let commitment_config = CommitmentConfig::finalized();
        match self
            .local_rpc_client
            .as_ref()
            .expect("local_rpc_client not set")
            .get_slot_with_commitment(commitment_config)
        {
            Ok(lib_num) => {
                info!("Block lib received from rpc client: {}", lib_num);
                self.lib = Some(lib_num);
                if let Some(cursor) = self.cursor {
                    if lib_num > cursor {
                        info!(
                            "ignoring cursor {} because LIB {} is greater",
                            cursor, lib_num
                        );
                        self.cursor = None;
                        self.first_block_to_process = None; // it would have been set by the cursor, we get rid of it too
                    }
                }
            }
            Err(e) => {
                info!("Error getting lib num from rpc client: {}", e);
            }
        }
    }

    pub fn set_lib(&mut self, slot: u64) {
        self.lib = Some(slot);
    }

    fn get_lib(&self) -> Option<u64> {
        self.lib
    }

    pub fn cache_block_from_rpc(&mut self, slot: u64, trace: bool) {
        match self
            .local_rpc_client
            .as_ref()
            .expect("local_rpc_client not set")
            .get_block_with_config(slot, DEFAULT_RPC_BLOCK_CONFIG)
        {
            Ok(block) => {
                debug!("Block Info fetched locally for slot {}", slot);
                self.set_block_info(
                    BlockInfo {
                        timestamp: convert_sol_timestamp(block.block_time.unwrap_or_default()),
                        parent_slot: block.parent_slot.clone(),
                        slot,
                        block_hash: block.blockhash.clone(),
                        parent_hash: block.previous_blockhash.clone(),
                        height: block.block_height,
                        rewards: to_block_rewards(&block.rewards),
                        transaction_count: block.transactions.unwrap_or_default().len() as u64,
                    },
                    trace,
                )
            }
            Err(_err) => {
                match self
                    .remote_rpc_client
                    .as_ref()
                    .expect("remote_rpc_client not set")
                    .get_block_with_config(slot, DEFAULT_RPC_BLOCK_CONFIG)
                {
                    Ok(block) => {
                        debug!("Block Info fetched remotely for slot {}", slot);
                        self.set_block_info(
                            BlockInfo {
                                timestamp: convert_sol_timestamp(
                                    block.block_time.unwrap_or_default(),
                                ),
                                parent_slot: block.parent_slot.clone(),
                                slot,
                                block_hash: block.blockhash.clone(),
                                parent_hash: block.previous_blockhash.clone(),
                                height: block.block_height,
                                rewards: to_block_rewards(&block.rewards),
                                transaction_count: block.transactions.unwrap_or_default().len()
                                    as u64,
                            },
                            trace,
                        )
                    }
                    Err(_err) => return,
                }
            }
        }
    }

    pub fn ordered_confirmed_slots_upto(&self, slot: u64) -> Vec<u64> {
        // Collect all keys from confirmed_slots that are less than the given slot
        let mut slots: Vec<u64> = self
            .confirmed_slots
            .keys()
            .cloned()
            .filter(|&x| x <= slot)
            .collect();
        slots.sort();
        slots
    }

    fn add_missing_slots_to_confirmed_slots(
        &mut self,
        last_sent: u64,
        parent_slot: u64,
        trace: bool,
    ) -> bool {
        let mut i = parent_slot;
        while i > last_sent {
            match self.block_infos.get(&i) {
                Some(bi) => {
                    if self.confirmed_slots.insert(i, true).is_none() {
                        info!("added missing slot {} to confirmed_slots", i);
                    };
                    i = bi.parent_slot;
                }
                None => {
                    self.cache_block_from_rpc(i, trace);
                    match self.block_infos.get(&i) {
                        Some(bi) => {
                            if self.confirmed_slots.insert(i, true).is_none() {
                                info!("added missing slot {} to confirmed_slots", i);
                            };
                            i = bi.parent_slot;
                        }
                        None => {
                            warn!("Failed to get block info for slot {} while adding missing slots to confirmed_slots", i);
                            return false;
                        }
                    }
                }
            }
        }
        i == last_sent
    }

    // should_skip_slot skips when not initialized and below target block

    pub fn set_confirmed_slot(&mut self, slot: u64, trace: bool) {
        if let Some(cursor) = self.cursor {
            if self.first_block_to_process.is_none() {
                if slot >= cursor {
                    self.first_block_to_process = Some(slot);
                    info!("setting first_block_to_process: {}", slot - 1);
                }
            }
        }
        debug!("set_confirmed_slot: {}", slot);
        self.confirmed_slots.insert(slot, true);

        if self.is_ready(slot) {
            if self.process_upto(trace, slot).is_err() {
                panic!("poisoned mutex")
            }
        }
    }

    pub fn has_block_info(&self, slot: u64) -> bool {
        self.block_infos.get(&slot).is_some()
    }

    pub fn is_ready(&self, slot: u64) -> bool {
        if self.confirmed_slots.get(&slot).is_none() {
            return false;
        }
        match self.block_infos.get(&slot) {
            None => false,
            Some(blk) => {
                if !self.with_block {
                    return true; // if we only track account changes, we don't need to count the transactions
                }
                if let Some(trxs) = self.transactions.get(&slot) {
                    if blk.transaction_count == trxs.len() as u64 {
                        true
                    } else {
                        debug!(
                            "slot {} has {} transactions, but {} were received, waiting for more",
                            slot,
                            blk.transaction_count,
                            trxs.len()
                        );
                        {
                            false
                        }
                    }
                } else {
                    if blk.transaction_count == 0 {
                        return true;
                    }
                    debug!(
                        "slot {} has no transactions, but is confirmed, waiting for transactions",
                        slot
                    );
                    false
                }
            }
        }
    }

    pub fn set_block_info(&mut self, block_info: BlockInfo, trace: bool) {
        let slot = block_info.slot;
        if self.lib.is_none() {
            // this may set the cursor to none
            self.set_last_finalized_block_from_rpc();
        }
        if self.first_received_blockmeta.is_none() {
            self.first_received_blockmeta = Some(slot);
            if self.cursor.is_none() {
                // usually because the lib has been set from rpc
                debug!("setting first_block_to_process to: {}", slot);
                self.first_block_to_process = Some(slot);

                // since we don't send these blocks, we apply their changes to the cache manually
                self.apply_changes_upto(trace, slot - 1);

                debug!("deleting blocks up to: {}", slot - 1);
                self.purge_blocks_up_to(slot - 1);
            }
        }
        debug!(
            "setting block info for slot {}, hash {}",
            slot, block_info.block_hash
        );
        self.block_infos.insert(slot, block_info);

        // if we get block_info for block 25, but we have 'confirmed blocks' 20 to 24, we'll fetch their block_info from RPC, which is a bit costly but prevents being stuck forever. This happens in rare cases, mostly upon startup
        for slot in self.ordered_confirmed_slots_upto(slot) {
            if !self.has_block_info(slot) {
                self.cache_block_from_rpc(slot, trace);
            }
        }

        if self.is_ready(slot) {
            if self.process_upto(trace, slot).is_err() {
                panic!("poisoned mutex")
            }
        }
    }

    // set_account_on_startup populates the account caches on startup
    pub fn set_account_on_startup(
        &mut self,
        pub_key: &[u8],
        owner: &[u8],
        data_hash: u64,
        slot: u64,
        write_version: u64,
        deleted: bool,
    ) {
        // Increment call counter and log every 10000 calls
        self.set_account_on_startup_call_count += 1;
        if self.set_account_on_startup_call_count % 10000 == 0 {
            info!(
                "set_account_on_startup called {} times, startup_received_slot size: {}, account_data_hash size: {}, slot: {}, write_version: {}, pub_key: {}, owner: {}",
                self.set_account_on_startup_call_count,
                self.startup_received_slot.len(),
                self.account_data_hash.len(),
                slot,
                write_version,
                bs58::encode(pub_key).into_string(),
                bs58::encode(owner).into_string()
            );
        }

        // Extract pub_key as fixed array using unsafe for performance
        let pub_key_fixed: [u8; 32] = unsafe {
            let mut arr = std::mem::MaybeUninit::<[u8; 32]>::uninit();
            std::ptr::copy_nonoverlapping(pub_key.as_ptr(), arr.as_mut_ptr() as *mut u8, 32);
            arr.assume_init()
        };

        // Create composite value: slot << 24 + write_version
        // Using left shift by 24 bits (~16M) which is > 10M max write_version
        let composite_value = (slot << 24) | write_version;

        // Check if we already have this account with a newer version
        if let Some(&existing_composite) = self.startup_received_slot.get(&pub_key_fixed) {
            if existing_composite >= composite_value {
                return;
            }
        }
        self.startup_received_slot
            .insert(pub_key_fixed, composite_value);

        // Extract owner as fixed array using unsafe for performance
        let owner_fixed: [u8; 32] = unsafe {
            let mut arr = std::mem::MaybeUninit::<[u8; 32]>::uninit();
            std::ptr::copy_nonoverlapping(owner.as_ptr(), arr.as_mut_ptr() as *mut u8, 32);
            arr.assume_init()
        };

        // Check if there was a previous owner for this public key
        if let Some(previous_value) = self.account_data_hash.get(&pub_key_fixed) {
            if previous_value.owner != owner_fixed {
                // Previous owner is different, will be overwritten below
            }
        }

        if deleted {
            self.account_data_hash.remove(&pub_key_fixed);
        } else {
            self.account_data_hash.insert(
                pub_key_fixed,
                AccountDataHashValue {
                    owner: owner_fixed,
                    data_hash,
                },
            );
        }
    }

    pub fn delete_startup_info(&mut self) {
        self.startup_received_slot = HashMap::default();
    }

    // set_account populates the caches for set_account
    pub fn set_account(
        &mut self,
        slot: u64,
        pub_key: &[u8],
        data: &[u8],
        owner: &[u8],
        write_version: u64,
        deleted: bool,
        data_hash: u64,
        trace: bool,
    ) {
        if let Some(last_sent) = self.last_sent_block {
            if last_sent >= slot {
                error!("Received account data for slot {} which is older than the last sent block {} (owner: {:?}, account: {:?})", slot, last_sent,  bs58::encode(pub_key).into_string(), bs58::encode(owner).into_string());
            }
        }

        // Use pubkey as the key
        let mut pub_key_fixed = [0u8; 32];
        pub_key_fixed.copy_from_slice(pub_key);

        // purge tail data on initialization
        if !self.block_account_changes.contains_key(&slot) {
            debug!("got some account data for slot {}", slot);
            let initializing = self.cursor.is_none() && self.first_block_to_process.is_none();
            if initializing {
                debug!("initializing: deleting blocks up to: {}", slot - 32);
                self.purge_blocks_up_to(slot - 32);
            }
        }

        let slot_entries = self
            .block_account_changes
            .entry(slot)
            .or_insert_with(HashMap::default);

        // skip if new change version exists
        if let Some(prev) = slot_entries.get(&pub_key_fixed) {
            if prev.write_version > write_version {
                if trace {
                    debug!(
                        "skipping slot because older version: {}, pub_key: {:?}, owner: {:?}, write_version: {}, prev_write_version: {}, deleted: {}, data_hash: {}",
                        slot, bs58::encode(pub_key).into_string(), bs58::encode(owner).into_string(), write_version, prev.write_version, deleted, data_hash
                    );
                }
                return; // skipping older write_versions
            }
        }

        if trace {
            let data_as_hex = hex::encode(&data[..10.min(data.len())]);
            debug!("handle_account_change@{}: account {:?} owner: {:?} delete: {:?} version: {:?} Data Size: {} Data Hash: {} Data Preview: {}", slot, bs58::encode(pub_key).into_string(), bs58::encode(owner).into_string(), deleted, write_version, data.len(), data_hash, data_as_hex);
        }

        let mut address = [0u8; 32];
        let mut owner_array = [0u8; 32];

        address.copy_from_slice(pub_key);
        owner_array.copy_from_slice(owner);

        let fixed_account = AccountFixed {
            address,
            owner: owner_array,
            data: data.to_vec(),
            deleted,
        };
        let awv = AccountWithWriteVersion {
            account: fixed_account,
            write_version,
            data_hash,
        };

        slot_entries.insert(pub_key_fixed, awv);
    }

    pub fn set_transaction(
        &mut self,
        slot: u64,
        transaction: ConfirmTransactionWithIndex,
        trace: bool,
    ) {
        if self.processed_slots.get(&slot).is_some() {
            error!(
                "slot {} already processed should not receive transaction for it",
                slot
            );
        }

        if let Some(txs) = self.transactions.get_mut(&slot) {
            txs.push(transaction);
        } else {
            debug!("inserting first transaction for slot {}", slot);
            let mut txs = Vec::new();
            txs.push(transaction);
            self.transactions.insert(slot, txs);
        }

        if self.is_ready(slot) {
            if self.process_upto(trace, slot).is_err() {
                panic!("poisoned mutex")
            }
        }
    }

    fn purge_blocks_up_to(&mut self, upto: u64) {
        let blocks = self
            .block_account_changes
            .keys()
            .cloned()
            .collect::<Vec<u64>>();
        for block in blocks {
            if block > upto {
                continue;
            }
            self.block_account_changes.remove(&block);
            self.block_infos.remove(&block);
        }

        let slots = self.confirmed_slots.keys().cloned().collect::<Vec<u64>>();
        for slot in slots {
            if slot <= upto {
                debug!("purging confirmed slot {}", slot);
                self.confirmed_slots.remove(&slot);
                if upto > 100 {
                    let processed_slot_remove = upto - 100;
                    self.processed_slots.remove(&processed_slot_remove);
                }
            }
        }
    }

    fn apply_changes_upto(&mut self, trace: bool, slot: u64) {
        // Find the lowest slot value in block_account_changes keys
        let first_slot = self
            .block_account_changes
            .keys()
            .min()
            .copied()
            .unwrap_or(slot);

        info!(
            "applying account cache changes for slots from {} to: {}",
            first_slot, slot
        );

        // Loop through each slot from first_slot to slot (inclusive)
        for current_slot in first_slot..=slot {
            let (_, changes) = filter_account_changes(
                self.block_account_changes.get(&current_slot),
                &self.account_data_hash,
                current_slot,
                trace,
            );
            self.apply_cache_changes(changes);
        }
    }

    pub fn process_upto(
        &mut self,
        trace: bool,
        slot: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("processing upto slot {}", slot);
        let first_block_to_process = match self.first_block_to_process {
            Some(slot) => slot,
            None => {
                debug!(
                    "No 'first_block_to_process' yet, skipping processing for slot {}",
                    slot
                );
                return Ok(());
            }
        };

        if self.first_received_blockmeta.is_none() {
            debug!(
                "No 'first_received_blockmeta' yet, skipping processing for slot {}",
                slot
            );
            return Ok(());
        }

        let lib = match self.get_lib() {
            Some(lib) => lib,
            None => {
                debug!("No 'lib' yet, skipping processing for slot {}", slot);
                return Ok(());
            }
        };

        if self.last_sent_block.is_none() {
            self.apply_changes_upto(trace, slot - 1);
            debug!("First being sent, now initialized");
            self.initialized = true;
        }

        for slot in self.ordered_confirmed_slots_upto(slot) {
            let must_send = slot >= first_block_to_process || self.dev_config.force_send;

            let block_info = match self.block_infos.get(&slot) {
                None => {
                    if must_send {
                        info!("No block info for slot {} in process_upto", slot);
                        return Ok(());
                    };
                    &BlockInfo::default()
                }
                Some(bi) => bi,
            };

            if let Some(last_sent_block) = self.last_sent_block {
                if last_sent_block < block_info.parent_slot {
                    warn!(
                            "last sent block {} is not the parent of slot {}. Expecting {}. (This is a very rare case that would create a hole). Manually adding missing slots to 'confirmed_slots', they will be sent on next loop",
                            last_sent_block,
                            slot,
                            block_info.parent_slot,
                        );

                    let success =
                        self.add_missing_slots_to_confirmed_slots(last_sent_block, slot, trace);
                    if !success {
                        warn!("Failed to add all missing slots to 'confirmed_slots' between {} and {}", last_sent_block, slot);
                    }
                    break; //
                } else if last_sent_block != slot - 1 {
                    warn!("dropped slots from {} to {}", last_sent_block + 1, slot - 1);
                }
            }

            let (effective_account_changes, cache_changes) = filter_account_changes(
                self.block_account_changes.get(&slot),
                &self.account_data_hash,
                slot,
                trace,
            );

            if must_send {
                let acc_block = create_account_block(effective_account_changes, &block_info);

                let mut transactions_with_index =
                    self.transactions.remove(&slot).unwrap_or_else(|| vec![]);

                transactions_with_index.sort_by_key(|ti| ti.index);

                let block = compose_and_purge_block(slot, &block_info, transactions_with_index);

                let printer = &mut self.block_printer;
                let result = printer.print(&block_info, lib, block, acc_block, &self.cursor_path);
                if !result.is_ok() {
                    info!("Error printing block at {}", slot);
                    return Err("Error printing block".into());
                }
                self.last_sent_block = Some(block_info.slot);
            } else {
                info!(
                    "in process_upto, not actually sending slot {}, below first_block_to_process {}. applying to cache only",
                    slot, first_block_to_process
                );
            }
            self.apply_cache_changes(cache_changes);
            self.processed_slots.insert(slot, true);
            self.purge_blocks_up_to(slot);

            if BLOCK_MUTEX.is_poisoned() || ACC_MUTEX.is_poisoned() {
                return Err("mutex poisoned".into());
            }
        }
        Ok(())
    }

    pub fn get_hash_count(&self) -> usize {
        self.account_data_hash.len()
    }

    fn apply_cache_changes(&mut self, changes: Vec<StateChange>) {
        for change in changes {
            let address_vec = change.address;
            let owner_vec = change.owner;
            let data_hash = change.data_hash;
            let deleted = change.deleted;

            // Convert to fixed-length arrays (assume exactly 32 bytes)
            let mut address_fixed = [0u8; 32];
            let mut owner_fixed = [0u8; 32];

            address_fixed.copy_from_slice(&address_vec);
            owner_fixed.copy_from_slice(&owner_vec);

            if deleted {
                self.account_data_hash.remove(&address_fixed);
            } else {
                self.account_data_hash.insert(
                    address_fixed,
                    AccountDataHashValue {
                        owner: owner_fixed,
                        data_hash,
                    },
                );
            }
        }
    }
}

struct StateChange {
    address: Vec<u8>,
    owner: Vec<u8>,
    data_hash: u64,
    deleted: bool,
}

fn filter_account_changes(
    changes: Option<&HashMap<[u8; 32], AccountWithWriteVersion>>,
    account_data_hash: &AccountDataHash,
    slot: u64,
    trace: bool,
) -> (Vec<Account>, Vec<StateChange>) {
    let mut filtered_changes: Vec<AccountFixed> = Vec::new();
    let mut state_changes: Vec<StateChange> = Vec::new();

    let mut in_block_owners: HashMap<[u8; 32], [u8; 32]> = HashMap::default();
    let mut ordered_changes: Vec<AccountWithWriteVersion> = Vec::new();

    if let Some(changes) = changes {
        for (_pubkey, account_with_version) in changes {
            ordered_changes.push(account_with_version.clone());
        }
    }
    ordered_changes.sort_by(|a, b| {
        a.account
            .address
            .cmp(&b.account.address)
            .then_with(|| a.write_version.cmp(&b.write_version))
    });

    for account_with_version in ordered_changes.into_iter() {
        let account = &account_with_version.account;
        account_with_version.write_version;

        let mut should_include = false;
        if let Some(cached_value) = account_data_hash.get(&account.address) {
            if cached_value.data_hash != account_with_version.data_hash {
                should_include = true;
            }
            if account.deleted {
                should_include = true;
            }
        } else {
            should_include = true;
        }

        let cached_owner = in_block_owners
            .get(&account.address)
            .or_else(|| account_data_hash.get(&account.address).map(|v| &v.owner))
            .copied();

        let different_previous_owner = match cached_owner {
            None => None,
            Some(owner) => {
                if owner != account.owner {
                    Some(owner)
                } else {
                    None
                }
            }
        };

        // Check for ownership change
        if let Some(cached_owner) = different_previous_owner {
            should_include = true;

            let prev_already_pushed = filtered_changes
                .iter()
                .any(|change| change.address == account.address && change.owner == cached_owner);

            if !prev_already_pushed {
                // Push the account change with the previous owner
                // it will appear before the new owner's state change
                filtered_changes.push(AccountFixed {
                    address: account.address,
                    owner: cached_owner,
                    data: account_with_version.account.data.clone(),
                    deleted: account.deleted,
                });
            }

            for change in filtered_changes.iter_mut() {
                if change.address == account.address {
                    change.deleted = account.deleted;
                    change.data = account.data.clone();
                }
            }
        }

        in_block_owners.insert(account.address, account.owner);

        if should_include {
            if trace {
                debug!("include_change@{}: account {:?} owner: {:?} delete: {:?} version: {:?} Data hash: {}", slot, bs58::encode(&account.address).into_string(), bs58::encode(&account.owner).into_string(), &account.deleted, account_with_version.write_version, account_with_version.data_hash);
            }
            filtered_changes.push(account.clone());

            if let Some(cached_owner) = different_previous_owner {
                state_changes.push(StateChange {
                    address: account.address.to_vec(),
                    owner: cached_owner.to_vec(),
                    data_hash: 0,
                    deleted: true, // we delete the version with the old owner from the cache
                });
            }

            state_changes.push(StateChange {
                address: account.address.to_vec(),
                owner: account.owner.to_vec(),
                data_hash: account_with_version.data_hash,
                deleted: account.deleted,
            });

            // save owner in case it gets changed in same slot
            in_block_owners.insert(account.address, account.owner);
        } else {
            if trace {
                debug!("exclude_change@{}: account {:?} owner: {:?} delete: {:?} version: {:?} Data hash: {}", slot, bs58::encode(&account.address).into_string(), bs58::encode(&account.owner).into_string(), &account.deleted, account_with_version.write_version, account_with_version.data_hash);
            }
        }
    }

    // Convert AccountFixed to Account for output compatibility
    let account_changes: Vec<Account> = filtered_changes
        .into_iter()
        .map(|fixed_account| fixed_account.to_account())
        .collect();

    (account_changes, state_changes)
}

#[cfg(test)]
mod tests {
    use super::*;

    use gxhash::gxhash64;
    use pretty_assertions::assert_eq;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // Helper function to convert Vec<u8> to fixed arrays - expects exactly 32 bytes
    fn vec_to_fixed_32(v: &[u8]) -> [u8; 32] {
        assert_eq!(
            v.len(),
            32,
            "Expected exactly 32 bytes for Solana address/owner"
        );
        let mut arr = [0u8; 32];
        arr.copy_from_slice(v);
        arr
    }

    // Helper function to create composite keys matching the logic in apply_cache_changes
    fn create_composite_key(owner: &[u8], address: &[u8]) -> [u8; 64] {
        let mut owner_fixed = [0u8; 32];
        let mut address_fixed = [0u8; 32];

        // Assume owner and address are exactly 32 bytes
        owner_fixed.copy_from_slice(owner);
        address_fixed.copy_from_slice(address);

        let mut composite_key = [0u8; 64];
        composite_key[..32].copy_from_slice(&owner_fixed);
        composite_key[32..].copy_from_slice(&address_fixed);
        composite_key
    }

    fn create_test_account(
        address: Vec<u8>,
        owner: Vec<u8>,
        data: Vec<u8>,
        deleted: bool,
    ) -> AccountFixed {
        let mut address_fixed = [0u8; 32];
        let mut owner_fixed = [0u8; 32];

        // Assume address and owner are exactly 32 bytes
        address_fixed.copy_from_slice(&address);
        owner_fixed.copy_from_slice(&owner);

        AccountFixed {
            address: address_fixed,
            owner: owner_fixed,
            data,
            deleted,
        }
    }

    fn create_test_account_with_version(
        account: AccountFixed,
        write_version: u64,
        data_hash: u64,
    ) -> AccountWithWriteVersion {
        AccountWithWriteVersion {
            account,
            write_version,
            data_hash,
        }
    }

    #[test]
    fn test_filter_account_changes_empty_changes() {
        let account_data_hash: AccountDataHash = HashMap::default();

        let (filtered_changes, state_changes) =
            filter_account_changes(None, &account_data_hash, 0, false);

        assert!(filtered_changes.is_empty());
        assert!(state_changes.is_empty());
    }

    #[test]
    fn test_filter_account_changes_new_account() {
        let mut changes = HashMap::default();
        let account_data_hash = HashMap::default();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data = vec![7, 8, 9];
        let account = create_test_account(address.clone(), owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        let address_fixed = vec_to_fixed_32(&address);
        changes.insert(address_fixed, account_with_version);

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, 0, false);

        assert_eq!(filtered_changes.len(), 1);
        assert_eq!(state_changes.len(), 1);

        let filtered_account = &filtered_changes[0];
        assert_eq!(filtered_account.address, address);
        assert_eq!(filtered_account.owner, owner);
        assert_eq!(filtered_account.data, data);
        assert_eq!(filtered_account.deleted, false);

        let state_change = &state_changes[0];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, owner);
        assert_eq!(state_change.data_hash, 123);
        assert_eq!(state_change.deleted, false);
    }

    #[test]
    fn test_filter_account_changes_same_data_hash_not_deleted() {
        let mut changes = HashMap::default();
        let mut account_data_hash: AccountDataHash = HashMap::default();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data = vec![7, 8, 9];
        let account = create_test_account(address.clone(), owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        let address_fixed = vec_to_fixed_32(&address);
        let owner_fixed = vec_to_fixed_32(&owner);
        changes.insert(address_fixed, account_with_version);
        account_data_hash.insert(
            address_fixed,
            AccountDataHashValue {
                owner: owner_fixed,
                data_hash: 123,
            },
        ); // Same hash

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, 0, false);

        // Should be filtered out because hash is the same and not deleted
        assert!(filtered_changes.is_empty());
        assert!(state_changes.is_empty());
    }

    #[test]
    fn test_filter_account_changes_different_data_hash() {
        let mut changes = HashMap::default();
        let mut account_data_hash: AccountDataHash = HashMap::default();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data = vec![7, 8, 9];
        let account = create_test_account(address.clone(), owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        let address_fixed = vec_to_fixed_32(&address);
        let owner_fixed = vec_to_fixed_32(&owner);
        changes.insert(address_fixed, account_with_version);
        account_data_hash.insert(
            address_fixed,
            AccountDataHashValue {
                owner: owner_fixed,
                data_hash: 456,
            },
        ); // Different hash

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, 0, false);

        // Should be included because hash is different
        assert_eq!(filtered_changes.len(), 1);
        assert_eq!(filtered_changes[0].address, address);

        assert_eq!(state_changes.len(), 1);
        assert_eq!(state_changes[0].data_hash, 123);
    }

    #[test]
    fn test_filter_account_changes_deleted_account() {
        let mut changes = HashMap::default();
        let mut account_data_hash: AccountDataHash = HashMap::default();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data = vec![7, 8, 9];
        let account = create_test_account(address.clone(), owner.clone(), data.clone(), true);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        let address_fixed = vec_to_fixed_32(&address);
        let owner_fixed = vec_to_fixed_32(&owner);
        changes.insert(address_fixed, account_with_version);
        account_data_hash.insert(
            address_fixed,
            AccountDataHashValue {
                owner: owner_fixed,
                data_hash: 123,
            },
        ); // Same hash but deleted

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, 0, false);

        // Should be included because account is deleted even with same hash
        assert_eq!(filtered_changes.len(), 1);
        assert_eq!(filtered_changes[0].address, address);
        assert!(filtered_changes[0].deleted);

        assert_eq!(state_changes.len(), 1);
        assert!(state_changes[0].deleted);
    }

    #[test]
    fn test_filter_account_changes_ownership_change() {
        let mut changes = HashMap::default();
        let mut account_data_hash: AccountDataHash = HashMap::default();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let old_owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let new_owner = vec![
            7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
        ];
        let data = vec![10, 11, 12];
        let account = create_test_account(address.clone(), new_owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        let address_fixed = vec_to_fixed_32(&address);
        let old_owner_fixed = vec_to_fixed_32(&old_owner);
        changes.insert(address_fixed, account_with_version);
        account_data_hash.insert(
            address_fixed,
            AccountDataHashValue {
                owner: old_owner_fixed,
                data_hash: 456,
            },
        ); // Different owner

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, 0, false);

        // Should have 2 accounts: one with old owner, one with new owner
        assert_eq!(filtered_changes.len(), 2);
        assert_eq!(state_changes.len(), 2);

        // First account should have the old owner
        let first_account = &filtered_changes[0];
        assert_eq!(first_account.address, address);
        assert_eq!(first_account.owner, old_owner);
        assert_eq!(first_account.data, data);

        // Second account should have the new owner
        let second_account = &filtered_changes[1];
        assert_eq!(second_account.address, address);
        assert_eq!(second_account.owner, new_owner);
        assert_eq!(second_account.data, data);

        // State change should delete the old owner
        let state_change = &state_changes[0];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, old_owner);
        assert_eq!(state_change.deleted, true);

        // State change should reflect the new owner
        let state_change = &state_changes[1];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, new_owner);
    }

    #[test]
    fn test_filter_account_changes_create_changeowner_delete() {
        let mut changes = HashMap::default();
        let account_data_hash: AccountDataHash = HashMap::default();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let old_owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let new_owner = vec![
            7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
        ];
        let data = vec![10, 11, 12];

        let account = create_test_account(address.clone(), old_owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);
        let address_fixed = vec_to_fixed_32(&address);
        changes.insert(address_fixed, account_with_version);

        let account2 = create_test_account(address.clone(), new_owner.clone(), vec![], true);
        let account2_with_version = create_test_account_with_version(account2, 2, 0);
        // This will overwrite the first entry since same address
        changes.insert(address_fixed, account2_with_version);

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, 0, false);

        // Should only have 1 account (new owner, since it overwrites)
        assert_eq!(filtered_changes.len(), 1);
        // Only one state change for the new owner
        assert_eq!(state_changes.len(), 1);

        let first_account = &filtered_changes[0];
        assert_eq!(first_account.address, address);
        assert_eq!(first_account.owner, new_owner);
        assert_eq!(first_account.data.len(), 0);
        assert_eq!(first_account.deleted, true);

        let state_change = &state_changes[0];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, new_owner);
        assert_eq!(state_change.deleted, true);
    }

    #[test]
    fn test_filter_account_changes_multiple_accounts_sorted() {
        let mut changes = HashMap::default();
        let account_data_hash: AccountDataHash = HashMap::default();

        // Create accounts with addresses that will test sorting
        let address1 = vec![
            3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ]; // Higher value
        let address2 = vec![
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ]; // Lower value
        let address3 = vec![
            2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ]; // Middle value

        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data = vec![7, 8, 9];

        let account1 = create_test_account(address1.clone(), owner.clone(), data.clone(), false);
        let account2 = create_test_account(address2.clone(), owner.clone(), data.clone(), false);
        let account3 = create_test_account(address3.clone(), owner.clone(), data.clone(), false);

        let account_with_version1 = create_test_account_with_version(account1, 1, 123);
        let account_with_version2 = create_test_account_with_version(account2, 2, 456);
        let account_with_version3 = create_test_account_with_version(account3, 3, 789);

        changes.insert(vec_to_fixed_32(&address1), account_with_version1);
        changes.insert(vec_to_fixed_32(&address2), account_with_version2);
        changes.insert(vec_to_fixed_32(&address3), account_with_version3);

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, 0, false);

        assert_eq!(filtered_changes.len(), 3);
        assert_eq!(state_changes.len(), 3);

        // Results should be sorted by address
        assert_eq!(filtered_changes[0].address, address2); // [1, 0, 0]
        assert_eq!(filtered_changes[1].address, address3); // [2, 0, 0]
        assert_eq!(filtered_changes[2].address, address1); // [3, 0, 0]
    }

    #[test]
    fn test_filter_account_changes_complex_scenario() {
        let mut changes = HashMap::default();
        let mut account_data_hash: AccountDataHash = HashMap::default();

        // Account 1: New account (should be included)
        let address1 = vec![
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner1 = vec![
            10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data1 = vec![100, 0, 0];
        let account1 = create_test_account(address1.clone(), owner1.clone(), data1.clone(), false);
        let account_with_version1 = create_test_account_with_version(account1, 1, 111);
        let address_fixed1 = vec_to_fixed_32(&address1);
        changes.insert(address_fixed1, account_with_version1);

        // Account 2: Same data hash, not deleted (should be filtered out)
        let address2 = vec![
            2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner2 = vec![
            20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data2 = vec![200, 0, 0];
        let account2 = create_test_account(address2.clone(), owner2.clone(), data2.clone(), false);
        let account_with_version2 = create_test_account_with_version(account2, 2, 222);
        let address_fixed2 = vec_to_fixed_32(&address2);
        changes.insert(address_fixed2, account_with_version2);
        account_data_hash.insert(
            address_fixed2,
            AccountDataHashValue {
                owner: vec_to_fixed_32(&owner2),
                data_hash: 222,
            },
        ); // Same hash

        // Account 3: Ownership change (should include both old and new owner versions)
        let address3 = vec![
            3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let old_owner3 = vec![
            30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let new_owner3 = vec![
            31, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data3 = vec![150, 0, 0];
        let account3 =
            create_test_account(address3.clone(), new_owner3.clone(), data3.clone(), false);
        let account_with_version3 = create_test_account_with_version(account3, 3, 333);
        let address_fixed3 = vec_to_fixed_32(&address3);
        changes.insert(address_fixed3, account_with_version3);
        account_data_hash.insert(
            address_fixed3,
            AccountDataHashValue {
                owner: vec_to_fixed_32(&old_owner3),
                data_hash: 999,
            },
        );

        // Account 4: Deleted account with same hash (should be included)
        let address4 = vec![
            4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner4 = vec![
            40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data4 = vec![200, 0, 0];
        let account4 = create_test_account(address4.clone(), owner4.clone(), data4.clone(), true);
        let account_with_version4 = create_test_account_with_version(account4, 4, 444);
        let address_fixed4 = vec_to_fixed_32(&address4);
        changes.insert(address_fixed4, account_with_version4);
        account_data_hash.insert(
            address_fixed4,
            AccountDataHashValue {
                owner: vec_to_fixed_32(&owner4),
                data_hash: 444,
            },
        ); // Same hash but deleted

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, 0, false);

        // Should have: Account1, Account3 (old owner), Account3 (new owner), Account4
        assert_eq!(filtered_changes.len(), 4);
        // Should have state changes for: Account1, Account3, (account3: delete), Account4
        assert_eq!(state_changes.len(), 4);

        // Verify the accounts are sorted by address
        assert_eq!(filtered_changes[0].address, address1); // Account 1
        assert_eq!(filtered_changes[1].address, address3); // Account 3 (old owner)
        assert_eq!(filtered_changes[1].owner, old_owner3);
        assert_eq!(filtered_changes[2].address, address3); // Account 3 (new owner)
        assert_eq!(filtered_changes[2].owner, new_owner3);
        assert_eq!(filtered_changes[3].address, address4); // Account 4 (deleted)
        assert_eq!(filtered_changes[3].deleted, true);
    }

    #[test]
    fn test_apply_cache_changes_empty_changes() {
        let mut state = create_test_state();
        let changes = Vec::new();

        state.apply_cache_changes(changes);

        assert!(state.account_data_hash.is_empty());
    }

    #[test]
    fn test_apply_cache_changes_single_non_deleted_account() {
        let mut state = create_test_state();
        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data_hash = 123u64;

        let change = StateChange {
            address: address.clone(),
            owner: owner.clone(),
            data_hash,
            deleted: false,
        };

        state.apply_cache_changes(vec![change]);

        // Check that account_data_hash is updated
        let address_fixed = vec_to_fixed_32(&address);
        let owner_fixed = vec_to_fixed_32(&owner);
        let value = state.account_data_hash.get(&address_fixed);
        assert!(value.is_some());
        let value = value.unwrap();
        assert_eq!(value.owner, owner_fixed);
        assert_eq!(value.data_hash, data_hash);
    }

    #[test]
    fn test_apply_cache_changes_single_deleted_account() {
        let mut state = create_test_state();
        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data_hash = 123u64;

        // First add an account
        let address_fixed = vec_to_fixed_32(&address);
        let owner_fixed = vec_to_fixed_32(&owner);
        state.account_data_hash.insert(
            address_fixed,
            AccountDataHashValue {
                owner: owner_fixed,
                data_hash: 42,
            },
        );

        // Verify it's there
        assert!(state.account_data_hash.contains_key(&address_fixed));

        // Now delete it
        let change = StateChange {
            address: address.clone(),
            owner: owner.clone(),
            data_hash,
            deleted: true,
        };

        state.apply_cache_changes(vec![change]);

        // Check that it's removed
        assert!(!state.account_data_hash.contains_key(&address_fixed));
    }

    #[test]
    fn test_apply_cache_changes_multiple_accounts() {
        let mut state = create_test_state();

        let address1 = vec![
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner1 = vec![
            10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data_hash1 = 111u64;

        let address2 = vec![
            2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner2 = vec![
            20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data_hash2 = 222u64;

        let address3 = vec![
            3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner3 = vec![
            30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data_hash3 = 333u64;

        let changes = vec![
            StateChange {
                address: address1.clone(),
                owner: owner1.clone(),
                data_hash: data_hash1,
                deleted: false,
            },
            StateChange {
                address: address2.clone(),
                owner: owner2.clone(),
                data_hash: data_hash2,
                deleted: false,
            },
            StateChange {
                address: address3.clone(),
                owner: owner3.clone(),
                data_hash: data_hash3,
                deleted: false,
            },
        ];

        state.apply_cache_changes(changes);

        // Check all accounts are added
        let address1_fixed = vec_to_fixed_32(&address1);
        let owner1_fixed = vec_to_fixed_32(&owner1);
        let address2_fixed = vec_to_fixed_32(&address2);
        let owner2_fixed = vec_to_fixed_32(&owner2);
        let address3_fixed = vec_to_fixed_32(&address3);
        let owner3_fixed = vec_to_fixed_32(&owner3);

        let value1 = state.account_data_hash.get(&address1_fixed);
        assert!(value1.is_some());
        assert_eq!(value1.unwrap().owner, owner1_fixed);
        assert_eq!(value1.unwrap().data_hash, data_hash1);

        let value2 = state.account_data_hash.get(&address2_fixed);
        assert!(value2.is_some());
        assert_eq!(value2.unwrap().owner, owner2_fixed);
        assert_eq!(value2.unwrap().data_hash, data_hash2);

        let value3 = state.account_data_hash.get(&address3_fixed);
        assert!(value3.is_some());
        assert_eq!(value3.unwrap().owner, owner3_fixed);
        assert_eq!(value3.unwrap().data_hash, data_hash3);
    }

    #[test]
    fn test_apply_cache_changes_mixed_operations() {
        let mut state = create_test_state();

        // Pre-populate with some data
        let address1 = vec![
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner1 = vec![
            10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data_hash1 = 111u64;

        let address2 = vec![
            2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner2 = vec![
            20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data_hash2 = 222u64;

        let address1_fixed = vec_to_fixed_32(&address1);
        let owner1_fixed = vec_to_fixed_32(&owner1);
        let address2_fixed = vec_to_fixed_32(&address2);
        let owner2_fixed = vec_to_fixed_32(&owner2);

        state.account_data_hash.insert(
            address1_fixed,
            AccountDataHashValue {
                owner: owner1_fixed,
                data_hash: data_hash1,
            },
        );
        state.account_data_hash.insert(
            address2_fixed,
            AccountDataHashValue {
                owner: owner2_fixed,
                data_hash: data_hash2,
            },
        );

        // Now apply mixed changes: delete one, add one, update one
        let address3 = vec![
            3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner3 = vec![
            30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data_hash3 = 333u64;

        let changes = vec![
            // Delete address1
            StateChange {
                address: address1.clone(),
                owner: owner1.clone(),
                data_hash: data_hash1,
                deleted: true,
            },
            // Add new address3
            StateChange {
                address: address3.clone(),
                owner: owner3.clone(),
                data_hash: data_hash3,
                deleted: false,
            },
            // Update address2 with new data hash
            StateChange {
                address: address2.clone(),
                owner: owner2.clone(),
                data_hash: 999u64,
                deleted: false,
            },
        ];
        state.apply_cache_changes(changes);

        // Check address1 is deleted
        assert!(!state.account_data_hash.contains_key(&address1_fixed));

        // Check address2 is updated
        let value2 = state.account_data_hash.get(&address2_fixed);
        assert!(value2.is_some());
        assert_eq!(value2.unwrap().owner, owner2_fixed);
        assert_eq!(value2.unwrap().data_hash, 999u64);

        // Check address3 is added
        let address3_fixed = vec_to_fixed_32(&address3);
        let owner3_fixed = vec_to_fixed_32(&owner3);
        let value3 = state.account_data_hash.get(&address3_fixed);
        assert!(value3.is_some());
        assert_eq!(value3.unwrap().owner, owner3_fixed);
        assert_eq!(value3.unwrap().data_hash, data_hash3);
    }

    #[test]
    fn test_apply_cache_changes_ownership_change() {
        let mut state = create_test_state();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let old_owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let new_owner = vec![
            7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
        ];
        let data_hash = 123u64;

        // Set up initial state with old owner
        let address_fixed = vec_to_fixed_32(&address);
        let old_owner_fixed = vec_to_fixed_32(&old_owner);

        state.account_data_hash.insert(
            address_fixed,
            AccountDataHashValue {
                owner: old_owner_fixed,
                data_hash,
            },
        );

        // Apply ownership change
        let change = StateChange {
            address: address.clone(),
            owner: new_owner.clone(),
            data_hash,
            deleted: false,
        };

        state.apply_cache_changes(vec![change]);

        // Check that account is updated to new owner
        let new_owner_fixed = vec_to_fixed_32(&new_owner);
        let value = state.account_data_hash.get(&address_fixed);
        assert!(value.is_some());
        assert_eq!(value.unwrap().owner, new_owner_fixed);
        assert_eq!(value.unwrap().data_hash, data_hash);

        // Note: the apply_cache should be called with a 'deleted: true' on the old state. we're testing an incomplete scenario
    }

    #[test]
    fn test_apply_cache_changes_same_address_different_owners() {
        let mut state = create_test_state();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner1 = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let owner2 = vec![
            7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
        ];
        let data_hash1 = 111u64;
        let data_hash2 = 222u64;

        // Apply changes for the same address but different owners
        let changes = vec![
            StateChange {
                address: address.clone(),
                owner: owner1.clone(),
                data_hash: data_hash1,
                deleted: true, // data for old owner should always be deleted there
            },
            StateChange {
                address: address.clone(),
                owner: owner2.clone(),
                data_hash: data_hash2,
                deleted: false,
            },
        ];

        state.apply_cache_changes(changes);

        // Verify new account is added with owner2
        let address_fixed = vec_to_fixed_32(&address);
        let owner2_fixed = vec_to_fixed_32(&owner2);

        let value = state.account_data_hash.get(&address_fixed);
        assert!(value.is_some());
        assert_eq!(value.unwrap().owner, owner2_fixed);
        assert_eq!(value.unwrap().data_hash, data_hash2);
    }

    // Helper function to create a test State instance
    fn create_test_state() -> State {
        use crate::block_printer::BlockPrinter;

        // Create temporary RPC clients (these won't be used in cache tests)
        let local_client = RpcClient::new("http://localhost:8899".to_string());
        let remote_client = RpcClient::new("http://localhost:8899".to_string());

        // Create a noop BlockPrinter
        let block_printer = BlockPrinter::new(None, None, true);

        State::new(
            local_client,
            remote_client,
            None,
            "/tmp/test_cursor".to_string(),
            block_printer,
            false,
            DevelopmentConfig::default(),
        )
    }

    /// Create test state with customizable parameters
    fn new_test_state(
        cursor: Option<u64>,
        block_printer: crate::block_printer::BlockPrinter,
    ) -> State {
        // Create temporary RPC clients (these won't be used in cache tests)
        let local_client = RpcClient::new("http://localhost:8899".to_string());
        let remote_client = RpcClient::new("http://localhost:8899".to_string());

        State::new(
            local_client,
            remote_client,
            cursor,
            "/tmp/test_cursor".to_string(),
            block_printer,
            false,
            DevelopmentConfig::default(),
        )
    }

    /// Assert that logs contain expected messages in order
    fn assert_logs_contain_ordered(expected_logs: Vec<String>) {
        testing_logger::validate(|captured_logs| {
            let log_bodies: Vec<String> =
                captured_logs.iter().map(|log| log.body.clone()).collect();

            for expected_log in &expected_logs {
                let found = log_bodies.iter().any(|log| log.contains(expected_log));
                assert!(
                    found,
                    "Expected log message '{}' not found in captured logs.\nCaptured logs: {:?}",
                    expected_log, log_bodies
                );
            }
        });
    }

    /// Setup noop block printer with logging for tests
    fn setup_noop_block_printer_with_logging(
        with_block: bool,
        with_account: bool,
    ) -> crate::block_printer::BlockPrinter {
        use crate::block_printer::BlockPrinter;
        use std::fs::File;
        use tempfile::NamedTempFile;
        use testing_logger;

        // Setup log capture
        testing_logger::setup();

        let block_file = if with_block {
            let temp_file = NamedTempFile::new().unwrap();
            Some(File::open(temp_file.path()).unwrap())
        } else {
            None
        };

        let account_file = if with_account {
            let temp_file = NamedTempFile::new().unwrap();
            Some(File::open(temp_file.path()).unwrap())
        } else {
            None
        };

        BlockPrinter::new(block_file, account_file, true)
    }

    /// Create simple BlockInfo with just slot number, parent_slot defaults to slot-1
    fn simple_block_info(slot: u64) -> BlockInfo {
        BlockInfo {
            slot,
            parent_slot: slot.saturating_sub(1),
            block_hash: format!("block_hash_{}", slot),
            parent_hash: format!("parent_hash_{}", slot.saturating_sub(1)),
            timestamp: Timestamp {
                seconds: 1000 + slot as i64,
                nanos: 0,
            },
            height: Some(slot),
            rewards: vec![],
            transaction_count: 0,
        }
    }

    fn test_block_info(slot: u64, parent_slot: u64) -> BlockInfo {
        BlockInfo {
            timestamp: Timestamp {
                seconds: 1234,
                nanos: 0,
            },
            parent_slot,
            slot,
            block_hash: "hash1".to_string(),
            parent_hash: "parent1".to_string(),
            height: Some(100),
            rewards: vec![],
            transaction_count: 0,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_set_block_info() {
        let mock_server = MockServer::start().await;
        let test_url = mock_server.uri();

        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "result": 100,
                "id": 1
            })))
            .mount(&mock_server)
            .await;

        // Initialize state with no lib and no first_received_blockmeta

        let mut state = State::new(
            RpcClient::new(test_url.clone()),
            RpcClient::new(test_url.clone()),
            None,
            "test_cursor_file".to_string(),
            BlockPrinter::new(None, None, false),
            true,
            DevelopmentConfig::default(),
        );

        // Test case 1: No lib set yet
        let block_info = test_block_info(100, 99);

        state.set_block_info(block_info.clone(), false);
        assert_eq!(state.lib, Some(100)); // From mock response
        assert_eq!(state.first_received_blockmeta, Some(100));
        assert_eq!(state.first_block_to_process, Some(100));

        // Test case 2: With cursor set, lib is before cursor
        let mut state_with_cursor = State::new(
            RpcClient::new(test_url.clone()),
            RpcClient::new(test_url.clone()),
            Some(110),
            "test_cursor_file".to_string(),
            BlockPrinter::new(None, None, false),
            true,
            DevelopmentConfig::default(),
        );

        state_with_cursor.set_block_info(block_info.clone(), false);
        assert_eq!(state_with_cursor.first_received_blockmeta, Some(100));
        assert_eq!(state_with_cursor.first_block_to_process, None); // Should not be set since cursor exists
        assert_eq!(state_with_cursor.cursor, Some(110));

        // Test case 3: With cursor set, lib is greater than cursor which will get cancelled
        let mut state_with_cursor = State::new(
            RpcClient::new(test_url.clone()),
            RpcClient::new(test_url.clone()),
            Some(90),
            "test_cursor_file".to_string(),
            BlockPrinter::new(None, None, false),
            true,
            DevelopmentConfig::default(),
        );

        state_with_cursor.set_block_info(block_info.clone(), false);
        assert_eq!(state_with_cursor.first_received_blockmeta, Some(100));
        assert_eq!(state_with_cursor.first_block_to_process, Some(100)); // gets set since cursor must be ignored
        assert_eq!(state_with_cursor.cursor, None);

        // Test case 4: Already initialized state

        state.first_received_blockmeta = Some(50);
        state.set_block_info(test_block_info(100, 99), false);

        // Check block was added without modifying first_received_blockmeta
        assert_eq!(state.first_received_blockmeta, Some(50));
        assert!(state.block_infos.contains_key(&100));
    }

    #[test]
    fn test_add_missing_slots_to_confirmed_slots() {
        let mut state = State::new(
            RpcClient::new("http://test.local"),
            RpcClient::new("http://test.remote"),
            None,
            "test_cursor.txt".to_string(),
            BlockPrinter::new(None, None, false),
            true,
            DevelopmentConfig::default(),
        );

        // Setup initial state
        state.initialized = true;
        state.last_sent_block = Some(1);

        state.block_infos.insert(1, test_block_info(1, 0));
        state.block_infos.insert(2, test_block_info(2, 1));
        state.block_infos.insert(4, test_block_info(4, 2));
        state.block_infos.insert(6, test_block_info(6, 4));

        // assume we receive confirmed_slot 7 with parent_slot 6
        let result =
            state.add_missing_slots_to_confirmed_slots(state.last_sent_block.unwrap(), 6, false);
        assert!(result);

        assert!(state.confirmed_slots.get(&1).is_none()); // was already sent

        assert!(state.confirmed_slots.get(&2).is_some());
        assert!(state.confirmed_slots.get(&4).is_some());
        assert!(state.confirmed_slots.get(&6).is_some());
    }

    const PUB_KEY_1: &[u8] = &[
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1,
    ];
    const PUB_KEY_2: &[u8] = &[
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        2, 2,
    ];
    const PUB_KEY_3: &[u8] = &[
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        3, 3,
    ];
    const OWNER_KEY_1: &[u8] = &[
        10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
        10, 10, 10, 10, 10, 10, 10, 10, 10,
    ];
    const DATA_1: &[u8] = b"data.1";
    const DATA_2: &[u8] = b"data.2";
    const DATA_3: &[u8] = b"data.2";
    const OWNER_KEY_11111111111111111111111111111111: &[u8] = [
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ]
    .as_slice();

    #[test]
    fn test_startup_value_repeated_is_still_saved() {
        let mut state = test_state_no_rpc(None);
        let slot: u64 = 1;
        let data_hash = gxhash64(b"data.1", 76);

        state.first_block_to_process = Some(slot);
        // First call with is_startup=true
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash, slot, 0, false);

        // Second call with incremented slot
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_1,
            0,
            false,
            data_hash,
            false,
        );

        // Even if it is the same data, we still save it here, because it could be changed by another slot
        assert_eq!(state.block_account_changes.get(&slot).unwrap().len(), 1);
    }

    #[test]
    fn test_set_account_on_startup_keeps_highest_slot() {
        let mut state = test_state_no_rpc(None);

        let data_hash_1 = gxhash64(b"data.1", 76);
        let data_hash_2 = gxhash64(b"data.1", 76);
        let data_hash_3 = gxhash64(b"data.1", 76);
        let data_hash_4 = gxhash64(b"data.4", 76);

        let pub_key_1_fixed = vec_to_fixed_32(PUB_KEY_1);
        let owner_1_fixed = vec_to_fixed_32(OWNER_KEY_1);

        state.first_block_to_process = Some(10);

        // set startup value with slot=8 -> must be set
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_1, 8, 1, false);

        let value = state.account_data_hash.get(&pub_key_1_fixed).unwrap();
        assert_eq!(value.owner, owner_1_fixed);
        assert_eq!(value.data_hash, data_hash_1);

        // set startup value with slot=7 -> unchanged
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_2, 7, 1, false);
        let value = state.account_data_hash.get(&pub_key_1_fixed).unwrap();
        assert_eq!(value.data_hash, data_hash_1);

        // set startup value with slot=8, higher write_version -> changed
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_2, 8, 4, false);
        let value = state.account_data_hash.get(&pub_key_1_fixed).unwrap();
        assert_eq!(value.owner, owner_1_fixed);
        assert_eq!(value.data_hash, data_hash_2);

        // set startup value with slot=9, lower write_version -> changed
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_3, 9, 0, false);
        let value = state.account_data_hash.get(&pub_key_1_fixed).unwrap();
        assert_eq!(value.data_hash, data_hash_3);

        // set startup value with slot=10, write_version=0 -> changed (higher slot)
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_4, 10, 0, false);
        let value = state.account_data_hash.get(&pub_key_1_fixed).unwrap();
        assert_eq!(value.data_hash, data_hash_4);
        assert_eq!(value.owner, owner_1_fixed);

        // set with different owner (OWNER_KEY_11111111111111111111111111111111) and deleted=true
        let different_owner = OWNER_KEY_11111111111111111111111111111111;
        state.set_account_on_startup(PUB_KEY_1, different_owner, data_hash_4, 12, 0, true);

        // should be removed
        assert!(state.account_data_hash.get(&pub_key_1_fixed).is_none());

        // set a value 'before' the block where it got deleted. it should remain deleted
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_1, 11, 0, false);
        assert!(state.account_data_hash.get(&pub_key_1_fixed).is_none());

        // set a value 'after' the block where it got deleted with higher write_version. it should be set
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_1, 12, 1, false);
        let value = state.account_data_hash.get(&pub_key_1_fixed).unwrap();
        assert_eq!(value.owner, owner_1_fixed);
        assert_eq!(value.data_hash, data_hash_1);
    }

    #[test]
    fn test_set_account_on_startup_owner_change_cleanup() {
        let mut state = test_state_no_rpc(None);
        let slot1: u64 = 10;
        let slot2: u64 = 20;

        let owner1 = &[
            11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
            11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
        ];
        let owner2 = &[
            22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22,
            22, 22, 22, 22, 22, 22, 22, 22, 22, 22,
        ];
        let data_hash1 = gxhash64(b"data.1", 76);
        let data_hash2 = gxhash64(b"data.2", 76);

        state.first_block_to_process = Some(slot1);

        // First call with owner1
        state.set_account_on_startup(PUB_KEY_1, owner1, data_hash1, slot1, 0, false);

        // Verify owner1+pubkey entry exists in account_data_hash
        let pub_key_1_fixed = vec_to_fixed_32(PUB_KEY_1);
        let owner1_fixed = vec_to_fixed_32(owner1);
        let value = state.account_data_hash.get(&pub_key_1_fixed).unwrap();
        assert_eq!(value.owner, owner1_fixed);
        assert_eq!(value.data_hash, data_hash1);

        // Second call with owner2 and higher slot number
        state.set_account_on_startup(PUB_KEY_1, owner2, data_hash2, slot2, 0, false);

        // Verify that owner2+pubkey entry exists in account_data_hash
        let owner2_fixed = vec_to_fixed_32(owner2);
        let value = state.account_data_hash.get(&pub_key_1_fixed).unwrap();
        assert_eq!(value.owner, owner2_fixed);
        assert_eq!(value.data_hash, data_hash2);
    }

    #[test]
    fn test_set_account_delete_two_owners_repeat() {
        let mut state = test_state_no_rpc(None);
        let slot: u64 = 1;
        let data_hash = 0;

        state.first_block_to_process = Some(slot);
        // create account there
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_1,
            0,
            false, // deleted
            data_hash,
            false,
        );

        // delete account here
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_11111111111111111111111111111111,
            0,
            true, // deleted
            data_hash,
            false,
        );

        // Assert that self.block_account_entries(slot_number) is empty for the slot+1
        let slot_changes = state.block_account_changes.get(&slot).unwrap();
        assert!(!slot_changes.is_empty());
        // With the new structure, same pubkey overwrites, so only 1 entry
        assert!(slot_changes.len() == 1);

        let pub_key_1_fixed = vec_to_fixed_32(PUB_KEY_1);
        assert!(slot_changes.contains_key(&pub_key_1_fixed));

        // Verify it has the last owner (11111...) with deleted=true
        let entry = slot_changes.get(&pub_key_1_fixed).unwrap();
        assert_eq!(
            entry.account.owner,
            *OWNER_KEY_11111111111111111111111111111111
        );
        assert_eq!(entry.account.deleted, true);
    }

    #[test]
    fn test_set_account_delete_before_ownerchange_repeat() {
        let mut state = test_state_no_rpc(None);
        let slot: u64 = 1;
        let data_hash = 0;

        state.first_block_to_process = Some(slot);
        // create account there
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_1,
            0,
            false, // deleted
            data_hash,
            false,
        );

        // delete account here
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_1,
            0,
            true, // deleted
            data_hash,
            false,
        );

        // delete account here
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_11111111111111111111111111111111,
            0,
            true, // deleted
            data_hash,
            false,
        );

        // Assert that self.block_account_entries(slot_number) is empty for the slot+1
        let slot_changes = state.block_account_changes.get(&slot).unwrap();
        assert!(!slot_changes.is_empty());
        // With the new structure, same pubkey overwrites, so only 1 entry
        assert!(slot_changes.len() == 1);

        let pub_key_1_fixed = vec_to_fixed_32(PUB_KEY_1);
        assert!(slot_changes.contains_key(&pub_key_1_fixed));

        // Verify it has the last owner (11111...) with deleted=true
        let entry = slot_changes.get(&pub_key_1_fixed).unwrap();
        assert_eq!(
            entry.account.owner,
            *OWNER_KEY_11111111111111111111111111111111
        );
        assert_eq!(entry.account.deleted, true);
    }

    #[test]
    fn test_set_account_delete_after_ownerchange_repeat() {
        let mut state = test_state_no_rpc(None);
        let slot: u64 = 1;
        let data_hash = 0;

        state.first_block_to_process = Some(slot);
        // create account there
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_1,
            0,
            false,
            data_hash,
            false,
        );

        // delete account here
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_11111111111111111111111111111111,
            0,
            false,
            data_hash,
            false,
        );

        // delete account here
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_11111111111111111111111111111111,
            0,
            true, // deleted
            data_hash,
            false,
        );

        // Assert that self.block_account_entries(slot_number) is empty for the slot+1
        let slot_changes = state.block_account_changes.get(&slot).unwrap();
        assert!(!slot_changes.is_empty());
        // With the new structure, same pubkey overwrites, so only 1 entry
        assert!(slot_changes.len() == 1);

        let pub_key_1_fixed = vec_to_fixed_32(PUB_KEY_1);
        assert!(slot_changes.contains_key(&pub_key_1_fixed));

        // Verify it has the last owner (OWNER_KEY_11111...) with deleted=true
        let entry = slot_changes.get(&pub_key_1_fixed).unwrap();
        assert_eq!(
            entry.account.owner,
            *OWNER_KEY_11111111111111111111111111111111
        );
        assert_eq!(entry.account.deleted, true);
    }

    fn test_state_no_rpc(cursor: Option<u64>) -> State {
        test_state(
            "http://localhost:8899".to_string(),
            "http://localhost:8899".to_string(),
            cursor,
        )
    }

    fn test_state(local_rpc: String, remote_rpc: String, cursor: Option<u64>) -> State {
        State::new(
            RpcClient::new(local_rpc),
            RpcClient::new(remote_rpc),
            cursor,
            "test_cursor.txt".to_string(),
            BlockPrinter::new(None, None, false),
            true,
            DevelopmentConfig::default(),
        )
    }

    #[test]
    fn test_integration_simple() {
        let mut state = new_test_state(Some(99), setup_noop_block_printer_with_logging(true, true));

        state.set_lib(99);

        state.set_account(100, PUB_KEY_1, DATA_1, OWNER_KEY_1, 1, false, 12345, true);
        state.set_account(101, PUB_KEY_2, DATA_2, OWNER_KEY_1, 1, false, 23456, true);
        state.set_account(102, PUB_KEY_3, DATA_3, OWNER_KEY_1, 1, false, 34567, true);

        state.set_block_info(simple_block_info(100), true);
        state.set_confirmed_slot(100, true);
        state.set_block_info(simple_block_info(101), true);
        state.set_confirmed_slot(101, true);
        state.set_block_info(simple_block_info(102), true);
        state.set_confirmed_slot(102, true);

        // Build expected log messages
        let mut expected_logs = Vec::new();
        for slot in [100, 101, 102] {
            expected_logs.push(format!("printing block {} (noop mode)", slot));
            expected_logs.push(format!("printing account_block {} (noop mode)", slot));
        }

        // Validate captured logs
        assert_logs_contain_ordered(expected_logs);
    }

    fn concat_keys(owner: &[u8], account: &[u8]) -> [u8; 64] {
        let mut result = [0u8; 64];
        result[..32].copy_from_slice(owner);
        result[32..].copy_from_slice(account);
        result
    }

    #[test]
    fn test_integration_cursor_after_start() {
        // Create state with noop BlockPrinter
        let mut state =
            new_test_state(Some(101), setup_noop_block_printer_with_logging(true, true));

        state.set_lib(99);

        state.set_account(100, PUB_KEY_1, DATA_1, OWNER_KEY_1, 1, false, 12345, true);
        state.set_account(101, PUB_KEY_2, DATA_2, OWNER_KEY_1, 1, false, 23456, true);
        state.set_account(102, PUB_KEY_3, DATA_3, OWNER_KEY_1, 1, false, 34567, true);

        state.set_block_info(simple_block_info(100), true);
        state.set_confirmed_slot(100, true);
        state.set_block_info(simple_block_info(101), true);
        state.set_confirmed_slot(101, true);
        state.set_block_info(simple_block_info(102), true);
        state.set_confirmed_slot(102, true);

        // Build expected log messages
        let mut expected_logs = Vec::new();
        for slot in [101, 102] {
            // slot 100 will not be printed
            expected_logs.push(format!("printing block {} (noop mode)", slot));
            expected_logs.push(format!("printing account_block {} (noop mode)", slot));
        }

        assert_eq!(state.account_data_hash[PUB_KEY_1].data_hash, 12345);
        assert_eq!(state.account_data_hash[PUB_KEY_2].data_hash, 23456);
        assert_eq!(state.account_data_hash[PUB_KEY_3].data_hash, 34567);

        // Validate captured logs
        assert_logs_contain_ordered(expected_logs);
    }

    #[test]
    fn test_integration_lib_after_cursor() {
        // Create state with noop BlockPrinter
        let mut state = new_test_state(Some(50), setup_noop_block_printer_with_logging(true, true));

        state.set_lib(100);

        state.set_account(100, PUB_KEY_1, DATA_1, OWNER_KEY_1, 1, false, 12345, true);
        state.set_account(101, PUB_KEY_2, DATA_2, OWNER_KEY_1, 1, false, 23456, true);
        state.set_account(102, PUB_KEY_3, DATA_3, OWNER_KEY_1, 1, false, 34567, true);

        state.set_block_info(simple_block_info(100), true);
        state.set_confirmed_slot(100, true);
        state.set_block_info(simple_block_info(101), true);
        state.set_confirmed_slot(101, true);
        state.set_block_info(simple_block_info(102), true);
        state.set_confirmed_slot(102, true);

        // Build expected log messages
        let mut expected_logs = Vec::new();
        for slot in [100, 101, 102] {
            expected_logs.push(format!("printing block {} (noop mode)", slot));
            expected_logs.push(format!("printing account_block {} (noop mode)", slot));
        }

        assert_eq!(state.account_data_hash[PUB_KEY_1].data_hash, 12345);
        assert_eq!(state.account_data_hash[PUB_KEY_2].data_hash, 23456);
        assert_eq!(state.account_data_hash[PUB_KEY_3].data_hash, 34567);

        // Validate captured logs
        assert_logs_contain_ordered(expected_logs);
    }

    #[test]
    fn test_integration_no_cursor_set_lib_delayed() {
        // Create state with noop BlockPrinter
        let mut state = new_test_state(None, setup_noop_block_printer_with_logging(true, true));

        state.set_account(100, PUB_KEY_1, DATA_1, OWNER_KEY_1, 1, false, 12345, true);
        state.set_account(101, PUB_KEY_2, DATA_2, OWNER_KEY_1, 1, false, 23456, true);
        state.set_account(102, PUB_KEY_3, DATA_3, OWNER_KEY_1, 1, false, 34567, true);
        state.set_block_info(simple_block_info(100), true);
        state.set_confirmed_slot(100, true);
        state.set_block_info(simple_block_info(101), true);
        state.set_confirmed_slot(101, true);

        state.set_lib(50);

        state.set_block_info(simple_block_info(102), true);
        state.set_confirmed_slot(102, true);

        // Build expected log messages
        let mut expected_logs = Vec::new();
        for slot in [100, 101, 102] {
            expected_logs.push(format!("printing block {} (noop mode)", slot));
            expected_logs.push(format!("printing account_block {} (noop mode)", slot));
        }

        assert_eq!(state.account_data_hash[PUB_KEY_1].data_hash, 12345);
        assert_eq!(state.account_data_hash[PUB_KEY_2].data_hash, 23456);
        assert_eq!(state.account_data_hash[PUB_KEY_3].data_hash, 34567);

        // Validate captured logs
        assert_logs_contain_ordered(expected_logs);
    }

    #[test]
    fn test_integration_no_cursor_missing_first_blockinfo() {
        // Create state with noop BlockPrinter
        let mut state = new_test_state(None, setup_noop_block_printer_with_logging(true, true));

        state.set_account(100, PUB_KEY_1, DATA_1, OWNER_KEY_1, 1, false, 12345, true);
        state.set_account(101, PUB_KEY_2, DATA_2, OWNER_KEY_1, 1, false, 23456, true);
        state.set_account(102, PUB_KEY_3, DATA_3, OWNER_KEY_1, 1, false, 34567, true);

        state.set_lib(100);

        // here we DON'T send block_info for slot 100
        state.set_confirmed_slot(100, true);
        state.set_block_info(simple_block_info(101), true);
        state.set_confirmed_slot(101, true);
        state.set_block_info(simple_block_info(102), true);
        state.set_confirmed_slot(102, true);

        // Build expected log messages
        let mut expected_logs = Vec::new();
        for slot in [101, 102] {
            // block 100 is not sent because we didn't get blockinfo for it and had no cursor
            expected_logs.push(format!("printing block {} (noop mode)", slot));
            expected_logs.push(format!("printing account_block {} (noop mode)", slot));
        }

        // this should be inserted even if we don't actually SEND the block
        assert_eq!(state.account_data_hash[PUB_KEY_1].data_hash, 12345);
        assert_eq!(state.account_data_hash[PUB_KEY_2].data_hash, 23456);
        assert_eq!(state.account_data_hash[PUB_KEY_3].data_hash, 34567);

        // Validate captured logs
        assert_logs_contain_ordered(expected_logs);
    }

    #[test]
    fn test_integration_cursor_and_lib_after_missing_first_blockinfo_and_confirmed() {
        // Create state with noop BlockPrinter
        let mut state =
            new_test_state(Some(102), setup_noop_block_printer_with_logging(true, true));

        state.set_account(100, PUB_KEY_1, DATA_1, OWNER_KEY_1, 1, false, 12345, true);
        state.set_account(101, PUB_KEY_2, DATA_2, OWNER_KEY_1, 1, false, 23456, true);
        state.set_account(102, PUB_KEY_3, DATA_3, OWNER_KEY_1, 1, false, 34567, true);

        // here we DON'T send block_info for slot 100
        // we DON'T send confirmed_slot either
        state.set_block_info(simple_block_info(101), true);
        state.set_confirmed_slot(101, true);

        state.set_lib(101);
        state.set_block_info(simple_block_info(102), true);
        state.set_confirmed_slot(102, true);

        // Build expected log messages
        let mut expected_logs = Vec::new();
        // 100 and 101 not sent because cursor is at slot 102
        for slot in [102] {
            // block 100 is not sent because we didn't get blockinfo for it and had no cursor
            expected_logs.push(format!("printing block {} (noop mode)", slot));
            expected_logs.push(format!("printing account_block {} (noop mode)", slot));
        }

        // this should be inserted even if we don't actually SEND the block
        assert_eq!(state.account_data_hash[PUB_KEY_1].data_hash, 12345);
        assert_eq!(state.account_data_hash[PUB_KEY_2].data_hash, 23456);
        assert_eq!(state.account_data_hash[PUB_KEY_3].data_hash, 34567);

        // Validate captured logs
        assert_logs_contain_ordered(expected_logs);
    }
}

fn compose_and_purge_block(
    slot: u64,
    block_info: &BlockInfo,
    transactions_with_index: Vec<ConfirmTransactionWithIndex>,
) -> Block {
    Block {
        previous_blockhash: block_info.parent_hash.clone(),
        blockhash: block_info.block_hash.clone(),
        slot,
        transactions: transactions_with_index
            .into_iter()
            .map(|ti| ti.transaction)
            .collect(),
        rewards: block_info.rewards.clone(),
        block_time: Some(UnixTimestamp {
            timestamp: block_info.timestamp.seconds,
        }),
        parent_slot: block_info.parent_slot,
        block_height: match block_info.height {
            Some(height) => Some(BlockHeight {
                block_height: height,
            }),
            None => None,
        },
    }
}
