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

type BlockInfoMap = HashMap<u64, BlockInfo>;
type ConfirmedSlotsMap = HashMap<u64, bool>;
use crate::pb::sf::solana::r#type::v1::{Block, BlockHeight, UnixTimestamp};
use crate::plugins::ConfirmTransactionWithIndex;
use log::{debug, info};
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
}

#[derive(Default, Clone)]
pub struct BlockInfo {
    pub slot: u64,
    pub parent_slot: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub timestamp: Timestamp,
    pub height: Option<u64>,
}

const DEFAULT_RPC_BLOCK_CONFIG: RpcBlockConfig = RpcBlockConfig {
    encoding: None,
    transaction_details: Some(TransactionDetails::None),
    rewards: Some(false),
    commitment: Some(CommitmentConfig::confirmed()),
    max_supported_transaction_version: Some(0),
};

pub struct State {
    initialized: bool, // passed the first received blockmeta

    first_received_blockmeta: Option<u64>,
    first_block_to_process: Option<u64>,

    cursor: Option<u64>,
    lib: Option<u64>,

    block_account_changes: BlockAccountChanges,
    account_data_hash: AccountDataHash,

    block_infos: BlockInfoMap,
    confirmed_slots: ConfirmedSlotsMap,

    transactions: Transactions,

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
            cursor: cursor,
            first_block_to_process: None,
            first_received_blockmeta: None,
            lib: None,
            initialized: false,

            block_account_changes: HashMap::new(),
            account_data_hash: HashMap::new(),
            block_infos: HashMap::new(),
            confirmed_slots: HashMap::new(),

            transactions: HashMap::new(),

            local_rpc_client: Some(local_rpc_client),
            remote_rpc_client: Some(remote_rpc_client),
            cursor_path: cursor_path,
            block_printer: block_printer,
        }
    }

    fn set_last_finalized_block_from_rpc(&mut self) {
        let commitment_config = CommitmentConfig::finalized();
        match self
            .local_rpc_client
            .as_ref()
            .unwrap()
            .get_slot_with_commitment(commitment_config)
        {
            Ok(lib_num) => {
                println!("Block lib received from rpc client: {}", lib_num);
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

    fn has_block_info(&self, slot: u64) -> bool {
        self.block_infos.contains_key(&slot)
    }

    pub fn cache_block_from_rpc(&mut self, slot: u64, try_remote: bool) {
        match self
            .local_rpc_client
            .as_ref()
            .unwrap()
            .get_block_with_config(slot, DEFAULT_RPC_BLOCK_CONFIG)
        {
            Ok(block) => {
                debug!("Block Info fetched locally for slot {}", slot);
                self.set_block_info(
                    slot,
                    BlockInfo {
                        timestamp: convert_sol_timestamp(block.block_time.unwrap()),
                        parent_slot: block.parent_slot.clone(),
                        slot: slot,
                        block_hash: block.blockhash.clone(),
                        parent_hash: block.previous_blockhash.clone(),
                        height: block.block_height,
                    },
                )
            }
            Err(_err) => {
                if !try_remote {
                    return;
                }
                match self
                    .remote_rpc_client
                    .as_ref()
                    .unwrap()
                    .get_block_with_config(slot, DEFAULT_RPC_BLOCK_CONFIG)
                {
                    Ok(block) => {
                        debug!("Block Info fetched remotely for slot {}", slot);
                        self.set_block_info(
                            slot,
                            BlockInfo {
                                timestamp: convert_sol_timestamp(block.block_time.unwrap()),
                                parent_slot: block.parent_slot.clone(),
                                slot: slot,
                                block_hash: block.blockhash.clone(),
                                parent_hash: block.previous_blockhash.clone(),
                                height: block.block_height,
                            },
                        )
                    }
                    Err(_err) => return,
                }
            }
        }
    }

    fn ordered_confirmed_slots_upto(&self, slot: u64) -> Vec<u64> {
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

    pub fn should_skip_slot(&self, slot: u64) -> bool {
        if self.initialized {
            return false;
        }

        // if we are not initialized, we skip any block below 'cursor' or 'first_block_to_process'
        // without those numbers we accept any account_change but truncate to keep 32 blocks in memory
        if self.first_block_to_process.is_some() && slot < self.first_block_to_process.unwrap() {
            return true;
        }
        if self.cursor.is_some() && slot <= self.cursor.unwrap() {
            return true;
        };
        return false;
    }

    pub fn set_confirmed_slot(&mut self, slot: u64) {
        if self.should_skip_slot(slot) {
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

    pub fn is_slot_confirm(&self, slot: u64) -> bool {
        self.confirmed_slots.get(&slot).is_some()
    }

    pub fn set_block_info(&mut self, slot: u64, block_info: BlockInfo) {
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
            deleted: deleted,
        };

        let awv = AccountWithWriteVersion {
            account: pb_account,
            write_version: write_version,
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
        if let Some(txs) = self.transactions.get_mut(&slot) {
            txs.push(transaction);
        } else {
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
            }
        }
    }

    pub fn process_upto(&mut self, slot: u64) {
        if self.first_block_to_process.is_none() {
            debug!(
                "No 'first_block_to_process' yet, skipping processing for slot {}",
                slot
            );
            return;
        }

        if self.first_received_blockmeta.is_none() {
            debug!(
                "No 'first_received_blockmeta' yet, skipping processing for slot {}",
                slot
            );
            return;
        }

        if self.get_lib().is_none() {
            debug!("No lib found yet, skipping processing of slot {}", slot);
            return;
        };

        if slot == self.first_received_blockmeta.unwrap() {
            debug!("First block was sent, now initialized");
            self.initialized = true;
        }

        for slot in self.ordered_confirmed_slots_upto(slot) {
            if slot < self.first_block_to_process.unwrap() {
                continue;
            }

            let block_info: &BlockInfo;
            if self.has_block_info(slot) {
                block_info = self.block_infos.get(&slot).unwrap()
            } else {
                let try_remote = !self.initialized || self.confirmed_slots.len() >= 10;
                self.cache_block_from_rpc(slot, try_remote);
                block_info = match self.block_infos.get(&slot) {
                    None => {
                        return;
                    }
                    Some(bi) => bi,
                }
            };

            let account_changes = self.get_account_changes(slot);
            let acc_block = create_account_block(
                account_changes.unwrap_or(&AccountChanges::default()),
                &block_info,
            );
            let lib = self.lib.unwrap();

            let mut transactions_with_index =
                self.transactions.remove(&slot).unwrap_or_else(|| vec![]);

            transactions_with_index.sort_by_key(|ti| ti.index);

            let block = compose_and_purge_block(slot, &block_info, transactions_with_index);

            // let a_printer = &mut self.account_block_printer.write().unwrap();
            // let b_printer = &mut self.block_printer.write().unwrap();
            let printer = &mut self.block_printer;
            printer.print(&block_info, lib, block, acc_block).unwrap();

            self.purge_blocks_up_to(slot);
            write_cursor(&self.cursor_path, slot);
        }
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
        blockhash: block_info.parent_hash.clone(),
        slot,
        transactions: transactions_with_index
            .into_iter()
            .map(|ti| ti.transaction)
            .collect(),
        rewards: vec![], //todo
        block_time: Some(UnixTimestamp {
            timestamp: block_info.timestamp.seconds,
        }),
        parent_slot: block_info.parent_slot,
        block_height: Some(BlockHeight {
            block_height: block_info.height.unwrap(),
        }),
    }
}

fn write_cursor(cursor_file: &str, cursor: u64) {
    std::fs::write(cursor_file, cursor.to_string()).unwrap();
}
