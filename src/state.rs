use crate::block_printer::BlockPrinter;
use crate::pb;
use crate::utils::{convert_sol_timestamp, create_account_block};
use hashbrown::HashMap;
use lazy_static::lazy_static;
use pb::sf::solana::r#type::v1::Account;
use prost_types::Timestamp;
use solana_rpc_client::rpc_client::RpcClient;

type BlockAccountChanges = HashMap<u64, AccountChanges>;
pub type AccountChanges = HashMap<Vec<u8>, AccountWithWriteVersion>;
pub type AccountDataHash = HashMap<Vec<u8>, u64>;
pub type AccountOwners = HashMap<Vec<u8>, Vec<u8>>;

pub type Transactions = HashMap<u64, Vec<ConfirmTransactionWithIndex>>;
type ProcessedSlot = HashMap<u64, bool>;

type BlockInfoMap = HashMap<u64, BlockInfo>;
type ConfirmedSlotsMap = HashMap<u64, bool>;
use crate::pb::sf::solana::r#type::v1::{Block, BlockHeight, Reward, UnixTimestamp};
use crate::plugins::{to_block_rewards, ConfirmTransactionWithIndex};
use log::{debug, error, info, warn};
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::bs58;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::TransactionDetails;

#[derive(Debug)]
pub struct AccountWithWriteVersion {
    pub account: Account,
    pub write_version: u64,
    pub data_hash: u64,
    pub owner_account_key: Option<Vec<u8>>,
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
    initialized: bool, // passed the first received blockmeta

    first_received_blockmeta: Option<u64>,
    first_block_to_process: Option<u64>,

    last_sent_block: Option<u64>,

    cursor: Option<u64>,
    lib: Option<u64>,

    block_account_changes: BlockAccountChanges,

    account_data_hash: AccountDataHash, // only updated when we print the block
    account_owners: AccountOwners,      // only updated when we print the block

    block_infos: BlockInfoMap,
    confirmed_slots: ConfirmedSlotsMap,

    with_block: bool,
    //with_account: bool,
    transactions: Transactions,
    processed_slots: ProcessedSlot,

    local_rpc_client: Option<RpcClient>,
    remote_rpc_client: Option<RpcClient>,
    cursor_path: String,
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
    ) -> Self {
        State {
            cursor,
            first_block_to_process: None,
            first_received_blockmeta: None,
            lib: None,
            initialized: false,

            block_account_changes: HashMap::new(),
            account_data_hash: HashMap::new(),
            account_owners: HashMap::new(),
            block_infos: HashMap::new(),
            confirmed_slots: HashMap::new(),
            last_sent_block: None,

            transactions: HashMap::new(),
            processed_slots: HashMap::new(),

            local_rpc_client: Some(local_rpc_client),
            remote_rpc_client: Some(remote_rpc_client),
            cursor_path,
            block_printer,
            with_block,
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
                println!("Error getting lib num from rpc client: {}", e);
            }
        }
    }

    pub fn set_lib(&mut self, slot: u64) {
        self.lib = Some(slot);
    }

    fn get_lib(&self) -> Option<u64> {
        self.lib
    }

    pub fn cache_block_from_rpc(&mut self, slot: u64) {
        match self
            .local_rpc_client
            .as_ref()
            .expect("local_rpc_client not set")
            .get_block_with_config(slot, DEFAULT_RPC_BLOCK_CONFIG)
        {
            Ok(block) => {
                debug!("Block Info fetched locally for slot {}", slot);
                self.set_block_info(BlockInfo {
                    timestamp: convert_sol_timestamp(block.block_time.unwrap_or_default()),
                    parent_slot: block.parent_slot.clone(),
                    slot,
                    block_hash: block.blockhash.clone(),
                    parent_hash: block.previous_blockhash.clone(),
                    height: block.block_height,
                    rewards: to_block_rewards(&block.rewards),
                    transaction_count: block.transactions.unwrap_or_default().len() as u64,
                })
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
                        self.set_block_info(BlockInfo {
                            timestamp: convert_sol_timestamp(block.block_time.unwrap_or_default()),
                            parent_slot: block.parent_slot.clone(),
                            slot,
                            block_hash: block.blockhash.clone(),
                            parent_hash: block.previous_blockhash.clone(),
                            height: block.block_height,
                            rewards: to_block_rewards(&block.rewards),
                            transaction_count: block.transactions.unwrap_or_default().len() as u64,
                        })
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

    fn add_missing_slots_to_confirmed_slots(&mut self, last_sent: u64, parent_slot: u64) -> bool {
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
                    self.cache_block_from_rpc(i);
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
    pub fn should_skip_slot(&self, slot: u64) -> bool {
        if self.initialized {
            return false;
        }

        // if we are not initialized, we skip any block below 'cursor' or 'first_block_to_process'
        // without those numbers we accept any account_change but truncate to keep 32 blocks in memory
        if self.first_block_to_process.is_some() && slot < self.first_block_to_process.unwrap() {
            return true;
        }
        if let Some(cursor) = self.cursor {
            return slot <= cursor;
        }
        false
    }

    pub fn set_confirmed_slot(&mut self, slot: u64) {
        if self.should_skip_slot(slot) {
            debug!("skipping slot {}", slot);
            return;
        }
        if let Some(cursor) = self.cursor {
            if self.first_block_to_process.is_none() {
                if slot >= cursor {
                    self.first_block_to_process = Some(slot);
                    debug!("deleting blocks up to: {}", slot - 1);
                    self.purge_blocks_up_to(slot - 1);
                }
            }
        }
        self.confirmed_slots.insert(slot, true);
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

    pub fn set_block_info(&mut self, block_info: BlockInfo) {
        let slot = block_info.slot;
        if self.lib.is_none() {
            self.set_last_finalized_block_from_rpc();
        }
        if self.first_received_blockmeta.is_none() {
            self.first_received_blockmeta = Some(slot);
            if self.cursor.is_none() {
                debug!("setting first_block_to_process to: {}", slot);
                self.first_block_to_process = Some(slot);
                debug!("deleting blocks up to: {}", slot - 1);
                self.purge_blocks_up_to(slot - 1);
            }
        }
        debug!(
            "setting block info for slot {}, hash {}",
            slot, block_info.block_hash
        );
        self.block_infos.insert(slot, block_info);
    }

    // set_account_on_startup populates the account caches on startup
    pub fn set_account_on_startup(&mut self, pub_key: &[u8], owner: &[u8], data_hash: u64) {
        let owner_account_key = [owner, pub_key].concat();
        self.account_data_hash.insert(owner_account_key, data_hash);
        self.account_owners.insert(pub_key.to_vec(), owner.to_vec());
        return;
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

        //create a unique key from owner and account addresses
        let owner_account_key = [owner, pub_key].concat();

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
            .or_insert_with(HashMap::new);

        // skip if new change version exists
        if let Some(prev) = slot_entries.get(&owner_account_key) {
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
            debug!("handle_account_change@{}: account {:?} owner: {:?} delete: {:?} version: {:?} Data Size: {} Data: {}", slot, bs58::encode(pub_key).into_string(), bs58::encode(owner).into_string(), deleted, write_version, data.len(),data_as_hex);
        }

        let pb_account = Account {
            address: pub_key.to_vec(),
            data: data.to_vec(),
            owner: owner.to_vec(),
            deleted,
        };
        let awv = AccountWithWriteVersion {
            account: pb_account,
            write_version,
            data_hash,
            owner_account_key: None,
        };

        slot_entries.insert(owner_account_key, awv);
    }

    pub fn set_transaction(&mut self, slot: u64, transaction: ConfirmTransactionWithIndex) {
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

    pub fn process_upto(&mut self, slot: u64) -> Result<(), Box<dyn std::error::Error>> {
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

        let first_received_blockmeta = match self.first_received_blockmeta {
            Some(slot) => slot,
            None => {
                debug!(
                    "No 'first_received_blockmeta' yet, skipping processing for slot {}",
                    slot
                );
                return Ok(());
            }
        };

        let lib = match self.get_lib() {
            Some(lib) => lib,
            None => {
                debug!("No 'lib' yet, skipping processing for slot {}", slot);
                return Ok(());
            }
        };

        if slot == first_received_blockmeta {
            debug!("First block was sent, now initialized");
            self.initialized = true;
        }

        for slot in self.ordered_confirmed_slots_upto(slot) {
            if slot < first_block_to_process {
                debug!(
                    "in process_upto, skipping slot {} below first_block_to_process {}",
                    slot, first_block_to_process
                );
                continue;
            }

            let block_info = match self.block_infos.get(&slot) {
                None => {
                    info!("No block info for slot {} in process_upto", slot);
                    return Ok(());
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

                    let success = self.add_missing_slots_to_confirmed_slots(last_sent_block, slot);
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
                &self.account_owners,
            );

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
            self.purge_blocks_up_to(slot);
            self.processed_slots.insert(slot, true);
            self.apply_cache_changes(cache_changes);

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
            let address = change.address;
            let owner = change.owner;
            let data_hash = change.data_hash;
            let deleted = change.deleted;

            let owner_account_key = [owner.clone(), address.clone()].concat();
            if deleted {
                self.account_data_hash.remove(&owner_account_key);
                if let Some(cached) = self.account_owners.get(&address) {
                    if cached == &owner {
                        self.account_owners.remove(&address);
                    }
                }
            } else {
                self.account_data_hash.insert(owner_account_key, data_hash);
                self.account_owners.insert(address.clone(), owner); // last one wins
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
    changes: Option<&HashMap<Vec<u8>, AccountWithWriteVersion>>,
    account_data_hash: &AccountDataHash,
    account_owners: &AccountOwners,
) -> (Vec<Account>, Vec<StateChange>) {
    let mut filtered_changes: Vec<Account> = Vec::new();
    let mut state_changes: Vec<StateChange> = Vec::new();

    let mut in_block_owners = AccountOwners::new();
    let mut ordered_changes: Vec<AccountWithWriteVersion> = Vec::new();

    if let Some(changes) = changes {
        for (owner_account_key, account_with_version) in changes {
            let with_key = AccountWithWriteVersion {
                account: account_with_version.account.clone(),
                write_version: account_with_version.write_version,
                data_hash: account_with_version.data_hash,
                owner_account_key: Some(owner_account_key.clone()),
            };
            ordered_changes.push(with_key);
        }
    }
    ordered_changes.sort_by(|a, b| {
        a.account
            .address
            .cmp(&b.account.address)
            .then_with(|| a.write_version.cmp(&b.write_version))
    });

    for account_with_version in ordered_changes.into_iter() {
        let owner_account_key = &account_with_version.owner_account_key.unwrap();
        let account = &account_with_version.account;
        account_with_version.write_version;

        let mut should_include = false;
        if let Some(cached_hash) = account_data_hash.get(owner_account_key) {
            if *cached_hash != account_with_version.data_hash {
                should_include = true;
            }
            if account.deleted {
                should_include = true;
            }
        } else {
            should_include = true;
        }

        let different_previous_owner = match in_block_owners
            .get(&account.address)
            .or_else(|| account_owners.get(&account.address))
        {
            None => None,
            Some(owner) => {
                if owner != &account.owner {
                    Some(owner.clone())
                } else {
                    None
                }
            }
        };

        // Check for ownership change
        if let Some(cached_owner) = different_previous_owner.clone() {
            should_include = true;

            let prev_already_pushed = filtered_changes
                .iter()
                .any(|change| change.address == account.address && change.owner == *cached_owner);

            if !prev_already_pushed {
                // Push the account change with the previous owner
                // it will appear before the new owner's state change
                filtered_changes.push(Account {
                    address: account.address.clone(),
                    owner: cached_owner.clone(),
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

        if should_include {
            filtered_changes.push(account.clone());

            if let Some(cached_owner) = different_previous_owner {
                state_changes.push(StateChange {
                    address: account.address.clone(),
                    owner: cached_owner.clone(),
                    data_hash: 0,
                    deleted: true, // we delete the version with the old owner from the cache
                });
            }

            state_changes.push(StateChange {
                address: account.address.clone(),
                owner: account.owner.clone(),
                data_hash: account_with_version.data_hash,
                deleted: account.deleted,
            });

            // save owner in case it gets changed in same slot
            in_block_owners.insert(account.address.clone(), account.owner.clone());
        }
    }

    return (filtered_changes, state_changes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashbrown::HashMap;
    use pb::sf::solana::r#type::v1::Account;

    use gxhash::gxhash64;
    use pretty_assertions::assert_eq;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn create_test_account(
        address: Vec<u8>,
        owner: Vec<u8>,
        data: Vec<u8>,
        deleted: bool,
    ) -> Account {
        Account {
            address,
            owner,
            data,
            deleted,
        }
    }

    fn create_test_account_with_version(
        account: Account,
        write_version: u64,
        data_hash: u64,
    ) -> AccountWithWriteVersion {
        AccountWithWriteVersion {
            account,
            write_version,
            data_hash,
            owner_account_key: None,
        }
    }

    #[test]
    fn test_filter_account_changes_empty_changes() {
        let account_data_hash = HashMap::new();
        let account_owners = HashMap::new();

        let (filtered_changes, state_changes) =
            filter_account_changes(None, &account_data_hash, &account_owners);

        assert!(filtered_changes.is_empty());
        assert!(state_changes.is_empty());
    }

    #[test]
    fn test_filter_account_changes_new_account() {
        let mut changes = HashMap::new();
        let account_data_hash = HashMap::new();
        let account_owners = HashMap::new();

        let address = vec![1, 2, 3];
        let owner = vec![4, 5, 6];
        let data = vec![7, 8, 9];
        let account = create_test_account(address.clone(), owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        changes.insert(vec![4, 5, 6, 1, 2, 3], account_with_version);

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, &account_owners);

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
        let mut changes = HashMap::new();
        let mut account_data_hash = HashMap::new();
        let account_owners = HashMap::new();

        let address = vec![1, 2, 3];
        let owner = vec![4, 5, 6];
        let data = vec![7, 8, 9];
        let account = create_test_account(address.clone(), owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        let owner_account_key = [owner.clone(), address.clone()].concat();
        changes.insert(owner_account_key.clone(), account_with_version);
        account_data_hash.insert(owner_account_key, 123); // Same hash

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, &account_owners);

        // Should be filtered out since data hash is the same and account is not deleted
        assert!(filtered_changes.is_empty());
        assert!(state_changes.is_empty());
    }

    #[test]
    fn test_filter_account_changes_different_data_hash() {
        let mut changes = HashMap::new();
        let mut account_data_hash = HashMap::new();
        let account_owners = HashMap::new();

        let address = vec![1, 2, 3];
        let owner = vec![4, 5, 6];
        let data = vec![7, 8, 9];
        let account = create_test_account(address.clone(), owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        let owner_account_key = [owner.clone(), address.clone()].concat();
        changes.insert(owner_account_key.clone(), account_with_version);
        account_data_hash.insert(owner_account_key, 456); // Different hash

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, &account_owners);

        assert_eq!(filtered_changes.len(), 1);
        assert_eq!(state_changes.len(), 1);

        let filtered_account = &filtered_changes[0];
        assert_eq!(filtered_account.address, address);
        assert_eq!(filtered_account.owner, owner);
        assert_eq!(filtered_account.data, data);

        let state_change = &state_changes[0];
        assert_eq!(state_change.data_hash, 123);
    }

    #[test]
    fn test_filter_account_changes_deleted_account() {
        let mut changes = HashMap::new();
        let mut account_data_hash = HashMap::new();
        let account_owners = HashMap::new();

        let address = vec![1, 2, 3];
        let owner = vec![4, 5, 6];
        let data = vec![7, 8, 9];
        let account = create_test_account(address.clone(), owner.clone(), data.clone(), true);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        let owner_account_key = [owner.clone(), address.clone()].concat();
        changes.insert(owner_account_key.clone(), account_with_version);
        account_data_hash.insert(owner_account_key, 123); // Same hash but account is deleted

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, &account_owners);

        // Should be included since account is deleted
        assert_eq!(filtered_changes.len(), 1);
        assert_eq!(state_changes.len(), 1);

        let filtered_account = &filtered_changes[0];
        assert_eq!(filtered_account.deleted, true);

        let state_change = &state_changes[0];
        assert_eq!(state_change.deleted, true);
    }

    #[test]
    fn test_filter_account_changes_ownership_change() {
        let mut changes = HashMap::new();
        let account_data_hash = HashMap::new();
        let mut account_owners = HashMap::new();

        let address = vec![1, 2, 3];
        let old_owner = vec![4, 5, 6];
        let new_owner = vec![7, 8, 9];
        let data = vec![10, 11, 12];
        let account = create_test_account(address.clone(), new_owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        changes.insert(
            [new_owner.clone(), address.clone()].concat(),
            account_with_version,
        );
        account_owners.insert(address.clone(), old_owner.clone()); // Different owner

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, &account_owners);

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
        let mut changes = HashMap::new();
        let account_data_hash = HashMap::new();
        let account_owners = HashMap::new();

        let address = vec![1, 2, 3];
        let old_owner = vec![4, 5, 6];
        let new_owner = vec![7, 8, 9];
        let data = vec![10, 11, 12];

        let account = create_test_account(address.clone(), old_owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);
        let key1 = [old_owner.clone(), address.clone()].concat();
        changes.insert(key1, account_with_version);

        let account2 = create_test_account(address.clone(), new_owner.clone(), vec![], true);
        let account2_with_version = create_test_account_with_version(account2, 2, 0);
        let key2 = [new_owner.clone(), address.clone()].concat();
        changes.insert(key2, account2_with_version);

        let (mut filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, &account_owners);

        // Should have 2 accounts: one with old owner, one with new owner
        assert_eq!(filtered_changes.len(), 2);
        // state_changes are not deduped when we add a deletion after owner change, so we get 3
        assert_eq!(state_changes.len(), 3);

        // we sort because them hashes are unordered and we want deterministic tests
        filtered_changes.sort_by(|a, b| a.owner.cmp(&b.owner));

        // First account should have the new owner
        let first_account = &filtered_changes[0];
        assert_eq!(first_account.address, address);
        assert_eq!(first_account.owner, old_owner);
        assert_eq!(first_account.data.len(), 0);
        assert_eq!(first_account.deleted, true);

        // Second account should have the old owner
        let second_account = &filtered_changes[1];
        assert_eq!(second_account.address, address);
        assert_eq!(second_account.owner, new_owner);
        assert_eq!(second_account.deleted, true);
        assert_eq!(second_account.data.len(), 0);

        // State change should reflect the creation+deletion, then new owner
        let state_change = &state_changes[0];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, old_owner);

        let state_change = &state_changes[1];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, old_owner);
        assert_eq!(state_change.deleted, true);

        let state_change = &state_changes[2];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, new_owner);
    }

    #[test]
    fn test_filter_account_changes_multiple_accounts_sorted() {
        let mut changes = HashMap::new();
        let account_data_hash = HashMap::new();
        let account_owners = HashMap::new();

        // Create accounts with addresses that will test sorting
        let address1 = vec![3, 0, 0]; // Higher value
        let address2 = vec![1, 0, 0]; // Lower value
        let address3 = vec![2, 0, 0]; // Middle value

        let owner = vec![4, 5, 6];
        let data = vec![7, 8, 9];

        let account1 = create_test_account(address1.clone(), owner.clone(), data.clone(), false);
        let account2 = create_test_account(address2.clone(), owner.clone(), data.clone(), false);
        let account3 = create_test_account(address3.clone(), owner.clone(), data.clone(), false);

        let account_with_version1 = create_test_account_with_version(account1, 1, 123);
        let account_with_version2 = create_test_account_with_version(account2, 2, 456);
        let account_with_version3 = create_test_account_with_version(account3, 3, 789);

        changes.insert(
            [owner.clone(), address1.clone()].concat(),
            account_with_version1,
        );
        changes.insert(
            [owner.clone(), address2.clone()].concat(),
            account_with_version2,
        );
        changes.insert(
            [owner.clone(), address3.clone()].concat(),
            account_with_version3,
        );

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, &account_owners);

        assert_eq!(filtered_changes.len(), 3);
        assert_eq!(state_changes.len(), 3);

        // Results should be sorted by address
        assert_eq!(filtered_changes[0].address, address2); // [1, 0, 0]
        assert_eq!(filtered_changes[1].address, address3); // [2, 0, 0]
        assert_eq!(filtered_changes[2].address, address1); // [3, 0, 0]
    }

    #[test]
    fn test_filter_account_changes_complex_scenario() {
        let mut changes = HashMap::new();
        let mut account_data_hash = HashMap::new();
        let mut account_owners = HashMap::new();

        // Account 1: New account (should be included)
        let address1 = vec![1, 0, 0];
        let owner1 = vec![10, 0, 0];
        let data1 = vec![100, 0, 0];
        let account1 = create_test_account(address1.clone(), owner1.clone(), data1.clone(), false);
        let account_with_version1 = create_test_account_with_version(account1, 1, 111);
        let owner_account_key1 = [owner1, address1.clone()].concat();
        changes.insert(owner_account_key1.clone(), account_with_version1);

        // Account 2: Same data hash, not deleted (should be filtered out)
        let address2 = vec![2, 0, 0];
        let owner2 = vec![20, 0, 0];
        let data2 = vec![200, 0, 0];
        let account2 = create_test_account(address2.clone(), owner2.clone(), data2.clone(), false);
        let account_with_version2 = create_test_account_with_version(account2, 2, 222);
        let owner_account_key2 = [owner2, address2].concat();
        changes.insert(owner_account_key2.clone(), account_with_version2);
        account_data_hash.insert(owner_account_key2, 222); // Same hash

        // Account 3: Ownership change (should include both old and new owner versions)
        let address3 = vec![3, 0, 0];
        let old_owner3 = vec![30, 0, 0];
        let new_owner3 = vec![31, 0, 0];
        let data3 = vec![255, 0, 0];
        let account3 =
            create_test_account(address3.clone(), new_owner3.clone(), data3.clone(), false);
        let account_with_version3 = create_test_account_with_version(account3, 3, 333);
        let owner_account_key3 = [new_owner3.clone(), address3.clone()].concat();
        changes.insert(owner_account_key3, account_with_version3);
        account_owners.insert(address3.clone(), old_owner3.clone());

        // Account 4: Deleted account with same hash (should be included)
        let address4 = vec![4, 0, 0];
        let owner4 = vec![40, 0, 0];
        let data4 = vec![200, 0, 0];
        let account4 = create_test_account(address4.clone(), owner4.clone(), data4.clone(), true);
        let account_with_version4 = create_test_account_with_version(account4, 4, 444);
        let owner_account_key4 = [owner4.clone(), address4.clone()].concat();
        changes.insert(owner_account_key4.clone(), account_with_version4);
        account_data_hash.insert(owner_account_key4, 444); // Same hash but deleted

        let (filtered_changes, state_changes) =
            filter_account_changes(Some(&changes), &account_data_hash, &account_owners);

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
        assert!(state.account_owners.is_empty());
    }

    #[test]
    fn test_apply_cache_changes_single_non_deleted_account() {
        let mut state = create_test_state();
        let address = vec![1, 2, 3];
        let owner = vec![4, 5, 6];
        let data_hash = 123u64;

        let change = StateChange {
            address: address.clone(),
            owner: owner.clone(),
            data_hash,
            deleted: false,
        };

        state.apply_cache_changes(vec![change]);

        // Check that account_owners is updated
        assert_eq!(state.account_owners.get(&address), Some(&owner));

        // Check that account_data_hash is updated with the combined key
        let expected_key = [owner.clone(), address.clone()].concat();
        assert_eq!(state.account_data_hash.get(&expected_key), Some(&data_hash));
    }

    #[test]
    fn test_apply_cache_changes_single_deleted_account() {
        let mut state = create_test_state();
        let address = vec![1, 2, 3];
        let owner = vec![4, 5, 6];
        let data_hash = 123u64;

        // First add an account
        let owner_account_key = [owner.clone(), address.clone()].concat();
        state.account_owners.insert(address.clone(), owner.clone());
        state
            .account_data_hash
            .insert(owner_account_key.clone(), data_hash);

        // Verify it's there
        assert!(state.account_owners.contains_key(&address));
        assert!(state.account_data_hash.contains_key(&owner_account_key));

        // Now delete it
        let change = StateChange {
            address: address.clone(),
            owner: owner.clone(),
            data_hash,
            deleted: true,
        };

        state.apply_cache_changes(vec![change]);

        // Check that both are removed
        assert!(!state.account_owners.contains_key(&address));
        assert!(!state.account_data_hash.contains_key(&owner_account_key));
    }

    #[test]
    fn test_apply_cache_changes_multiple_accounts() {
        let mut state = create_test_state();

        let address1 = vec![1, 0, 0];
        let owner1 = vec![10, 0, 0];
        let data_hash1 = 111u64;

        let address2 = vec![2, 0, 0];
        let owner2 = vec![20, 0, 0];
        let data_hash2 = 222u64;

        let address3 = vec![3, 0, 0];
        let owner3 = vec![30, 0, 0];
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
        assert_eq!(state.account_owners.get(&address1), Some(&owner1));
        assert_eq!(state.account_owners.get(&address2), Some(&owner2));
        assert_eq!(state.account_owners.get(&address3), Some(&owner3));

        // Check all data hashes are added
        let key1 = [owner1.clone(), address1.clone()].concat();
        let key2 = [owner2.clone(), address2.clone()].concat();
        let key3 = [owner3.clone(), address3.clone()].concat();

        assert_eq!(state.account_data_hash.get(&key1), Some(&data_hash1));
        assert_eq!(state.account_data_hash.get(&key2), Some(&data_hash2));
        assert_eq!(state.account_data_hash.get(&key3), Some(&data_hash3));
    }

    #[test]
    fn test_apply_cache_changes_mixed_operations() {
        let mut state = create_test_state();

        // Pre-populate with some data
        let address1 = vec![1, 0, 0];
        let owner1 = vec![10, 0, 0];
        let data_hash1 = 111u64;
        let key1 = [owner1.clone(), address1.clone()].concat();

        let address2 = vec![2, 0, 0];
        let owner2 = vec![20, 0, 0];
        let data_hash2 = 222u64;
        let key2 = [owner2.clone(), address2.clone()].concat();

        state
            .account_owners
            .insert(address1.clone(), owner1.clone());
        state.account_data_hash.insert(key1.clone(), data_hash1);
        state
            .account_owners
            .insert(address2.clone(), owner2.clone());
        state.account_data_hash.insert(key2.clone(), data_hash2);

        // Now apply mixed changes: delete one, add one, update one
        let address3 = vec![3, 0, 0];
        let owner3 = vec![30, 0, 0];
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
        assert!(!state.account_owners.contains_key(&address1));
        assert!(!state.account_data_hash.contains_key(&key1));

        // Check address2 is updated
        assert_eq!(state.account_owners.get(&address2), Some(&owner2));
        assert_eq!(state.account_data_hash.get(&key2), Some(&999u64));

        // Check address3 is added
        assert_eq!(state.account_owners.get(&address3), Some(&owner3));
        let key3 = [owner3.clone(), address3.clone()].concat();
        assert_eq!(state.account_data_hash.get(&key3), Some(&data_hash3));
    }

    #[test]
    fn test_apply_cache_changes_ownership_change() {
        let mut state = create_test_state();

        let address = vec![1, 2, 3];
        let old_owner = vec![4, 5, 6];
        let new_owner = vec![7, 8, 9];
        let data_hash = 123u64;

        // Set up initial state with old owner
        let old_key = [old_owner.clone(), address.clone()].concat();
        state
            .account_owners
            .insert(address.clone(), old_owner.clone());
        state.account_data_hash.insert(old_key.clone(), data_hash);

        // Apply ownership change
        let change = StateChange {
            address: address.clone(),
            owner: new_owner.clone(),
            data_hash,
            deleted: false,
        };

        state.apply_cache_changes(vec![change]);

        // Check that account_owners is updated to new owner
        assert_eq!(state.account_owners.get(&address), Some(&new_owner));

        // Check that new key is added
        let new_key = [new_owner.clone(), address.clone()].concat();
        assert_eq!(state.account_data_hash.get(&new_key), Some(&data_hash));

        // Note: the apply_cache should be called with a 'deleted: true' on the old state. we're testing an incomplete scenario
    }

    #[test]
    fn test_apply_cache_changes_same_address_different_owners() {
        let mut state = create_test_state();

        let address = vec![1, 2, 3];
        let owner1 = vec![4, 5, 6];
        let owner2 = vec![7, 8, 9];
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

        // The last change should win for account_owners
        assert_eq!(state.account_owners.get(&address), Some(&owner2));

        // The previous owner should be removed
        let key1 = [owner1.clone(), address.clone()].concat();
        let key2 = [owner2.clone(), address.clone()].concat();

        assert_eq!(state.account_data_hash.get(&key1), None);
        assert_eq!(state.account_data_hash.get(&key2), Some(&data_hash2));
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
        )
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
        );

        // Test case 1: No lib set yet
        let block_info = test_block_info(100, 99);

        state.set_block_info(block_info.clone());
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
        );

        state_with_cursor.set_block_info(block_info.clone());
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
        );

        state_with_cursor.set_block_info(block_info.clone());
        assert_eq!(state_with_cursor.first_received_blockmeta, Some(100));
        assert_eq!(state_with_cursor.first_block_to_process, Some(100)); // gets set since cursor must be ignored
        assert_eq!(state_with_cursor.cursor, None);

        // Test case 4: Already initialized state

        state.first_received_blockmeta = Some(50);
        state.set_block_info(test_block_info(100, 99));

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
        );

        // Setup initial state
        state.initialized = true;
        state.last_sent_block = Some(1);

        state.block_infos.insert(1, test_block_info(1, 0));
        state.block_infos.insert(2, test_block_info(2, 1));
        state.block_infos.insert(4, test_block_info(4, 2));
        state.block_infos.insert(6, test_block_info(6, 4));

        // assume we receive confirmed_slot 7 with parent_slot 6
        let result = state.add_missing_slots_to_confirmed_slots(state.last_sent_block.unwrap(), 6);
        assert!(result);

        assert!(state.confirmed_slots.get(&1).is_none()); // was already sent

        assert!(state.confirmed_slots.get(&2).is_some());
        assert!(state.confirmed_slots.get(&4).is_some());
        assert!(state.confirmed_slots.get(&6).is_some());
    }

    const PUB_KEY_1: &[u8] = b"pubkey.1".as_slice();
    const OWNER_KEY_1: &[u8] = b"owner.1".as_slice();
    const DATA_1: &[u8] = b"data.1";
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
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash);

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
        assert!(slot_changes.len() == 2);

        let acc_owner_1 = [OWNER_KEY_1, PUB_KEY_1].concat();
        let acc_owner_11111 = [OWNER_KEY_11111111111111111111111111111111, PUB_KEY_1].concat();

        assert!(slot_changes.contains_key(&acc_owner_1));
        assert!(slot_changes.contains_key(&acc_owner_11111));
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
        assert!(slot_changes.len() == 2);

        let acc_owner_1 = [OWNER_KEY_1, PUB_KEY_1].concat();
        let acc_owner_11111 = [OWNER_KEY_11111111111111111111111111111111, PUB_KEY_1].concat();

        assert!(slot_changes.contains_key(&acc_owner_1));
        assert!(slot_changes.contains_key(&acc_owner_11111));
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
        assert!(slot_changes.len() == 2);

        let acc_owner_1 = [OWNER_KEY_1, PUB_KEY_1].concat();
        let acc_owner_11111 = [OWNER_KEY_11111111111111111111111111111111, PUB_KEY_1].concat();

        assert!(slot_changes.contains_key(&acc_owner_1));
        assert!(slot_changes.contains_key(&acc_owner_11111));
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
        )
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
