use crate::block_printer::BlockPrinter;
use crate::pb;
use crate::utils::{convert_sol_timestamp, create_account_block};
use lazy_static::lazy_static;
use pb::sf::solana::r#type::v1::Account;
use prost_types::Timestamp;
use solana_rpc_client::rpc_client::RpcClient;
use std::collections::HashMap;

type BlockAccountChanges = HashMap<u64, AccountChanges>;
pub type AccountChanges = HashMap<Vec<u8>, AccountWithWriteVersion>;
pub type AccountDataHash = HashMap<Vec<u8>, u64>;

pub type Transactions = HashMap<u64, Vec<ConfirmTransactionWithIndex>>;
type ProcessedSlot = HashMap<u64, bool>;

type BlockInfoMap = HashMap<u64, BlockInfo>;
type ConfirmedSlotsMap = HashMap<u64, bool>;
use crate::pb::sf::solana::r#type::v1::{Block, BlockHeight, Reward, UnixTimestamp};
use crate::plugins::{to_block_rewards, ConfirmTransactionWithIndex};
use log::{debug, error, info, warn};
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::TransactionDetails;

pub struct AccountWithWriteVersion {
    pub account: Account,
    pub write_version: u64,
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
    account_data_hash: AccountDataHash,

    block_infos: BlockInfoMap,
    confirmed_slots: ConfirmedSlotsMap,

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
    ) -> Self {
        State {
            cursor,
            first_block_to_process: None,
            first_received_blockmeta: None,
            lib: None,
            initialized: false,

            block_account_changes: HashMap::new(),
            account_data_hash: HashMap::new(),
            block_infos: HashMap::new(),
            confirmed_slots: HashMap::new(),
            last_sent_block: None,

            transactions: HashMap::new(),
            processed_slots: HashMap::new(),

            local_rpc_client: Some(local_rpc_client),
            remote_rpc_client: Some(remote_rpc_client),
            cursor_path,
            block_printer,
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

    fn get_account_changes(&self, slot: u64) -> Option<&AccountChanges> {
        self.block_account_changes.get(&slot)
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
                            slot: slot,
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
        return i == last_sent;
    }

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
        return false;
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
        return self.block_infos.get(&slot).is_some();
    }

    pub fn is_ready(&self, slot: u64) -> bool {
        if self.confirmed_slots.get(&slot).is_none() {
            return false;
        }
        match self.block_infos.get(&slot) {
            None => return false,
            Some(blk) => {
                if let Some(trxs) = self.transactions.get(&slot) {
                    if blk.transaction_count == trxs.len() as u64 {
                        return true;
                    } else {
                        debug!(
                            "slot {} has {} transactions, but {} were received, waiting for more",
                            slot,
                            blk.transaction_count,
                            trxs.len()
                        );
                        {
                            return false;
                        }
                    };
                } else {
                    debug!(
                        "slot {} has no transactions, but is confirmed, waiting for transactions",
                        slot
                    );
                    return false;
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

    pub fn set_account(
        &mut self,
        slot: u64,
        pub_key: &[u8],
        data: &[u8],
        owner: &[u8],
        write_version: u64,
        deleted: bool,
        is_startup: bool,
        data_hash: u64,
        trace: bool,
    ) {
        if is_startup {
            self.account_data_hash.insert(pub_key.to_vec(), data_hash);
            return;
        }

        if !self.block_account_changes.contains_key(&slot) {
            debug!("account data for slot {}", slot);
            if self.cursor.is_none() && self.first_block_to_process.is_none() {
                // without cursor or first_block_to_process, we only keep a few blocks in here... this happens right after is_startup but before we get a confirmed slot
                debug!("initializing: deleting blocks up to: {}", slot - 1);
                self.purge_blocks_up_to(slot - 32);
            }
        }

        let slot_entries = self
            .block_account_changes
            .entry(slot)
            .or_insert_with(HashMap::new);

        let address = pub_key.to_vec();
        if let Some(prev) = slot_entries.get(&address) {
            if prev.write_version > write_version {
                if trace {
                    debug!(
                        "skipping slot because older version: {}, pub_key: {:?}, owner: {:?}, write_version: {}, prev_write_version: {}, deleted: {}, data_hash: {}",
                        slot, hex::encode(pub_key), hex::encode(owner), write_version, prev.write_version, deleted, data_hash
                    );
                }
                return; // skipping older write_versions
            }
            // skip if the data is the same and the account is not deleted
            if !deleted {
                if let Some(h) = self.account_data_hash.get(&address) {
                    if *h == data_hash {
                        return; // skipping same data
                    }
                }
            }
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
        };

        if trace {
            debug!(
                "inserting slot: {}, pub_key: {:?}, owner: {:?}, write_version: {}, deleted: {}, data_hash: {}",
                slot, hex::encode(pub_key), hex::encode(owner), write_version, deleted, data_hash
            );
        }

        self.account_data_hash.insert(pub_key.to_vec(), data_hash);
        slot_entries.insert(address, awv);
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
                }
            }

            let account_changes = self.get_account_changes(slot);
            let acc_block = create_account_block(
                account_changes.unwrap_or(&AccountChanges::default()),
                &block_info,
            );

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

            if BLOCK_MUTEX.is_poisoned() || ACC_MUTEX.is_poisoned() {
                return Err("mutex poisoned".into());
            }
        }
        return Ok(());
    }

    pub fn get_hash_count(&self) -> usize {
        self.account_data_hash.len()
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
        rewards: block_info.rewards.clone(), //todo: clone?????
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

#[cfg(test)]
mod tests {
    use super::*;

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
}
