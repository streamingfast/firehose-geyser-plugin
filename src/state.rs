use crate::block_printer::BlockPrinter;
use crate::pb;
use crate::utils::{convert_sol_timestamp, create_account_block};
use pb::sf::solana::r#type::v1::Account;
use prost_types::Timestamp;
use solana_rpc_client::rpc_client::RpcClient;
use std::collections::HashMap;
use twox_hash::XxHash64;

type BlockAccountChanges = HashMap<u64, AccountChanges>;
pub type AccountChanges = HashMap<Vec<u8>, AccountWithWriteVersion>;
pub type AccountDataHash = HashMap<Vec<u8>, u64>;
type BlockInfoMap = HashMap<u64, BlockInfo>;
type ConfirmedSlotsMap = HashMap<u64, bool>;
use log::{debug, info};
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::TransactionDetails;

const SEED: u64 = 1234;

pub struct AccountWithWriteVersion {
    pub account: Account,
    pub write_version: u64,
}

#[derive(Default)]
pub struct BlockInfo {
    pub slot: u64,
    pub parent_slot: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub timestamp: Timestamp,
}

const DEFAULT_RPC_BLOCK_CONFIG: RpcBlockConfig = RpcBlockConfig {
    encoding: None,
    transaction_details: Some(TransactionDetails::None),
    rewards: Some(false),
    commitment: Some(CommitmentConfig::confirmed()),
    max_supported_transaction_version: Some(0),
};

#[derive(Default)]
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

    local_rpc_client: Option<RpcClient>,
    remote_rpc_client: Option<RpcClient>,
    cursor_path: String,
    noop: bool,
}

impl State {
    pub fn new(
        local_rpc_client: RpcClient,
        remote_rpc_client: RpcClient,
        cursor: Option<u64>,
        cursor_path: String,
        noop: bool,
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

            local_rpc_client: Some(local_rpc_client),
            remote_rpc_client: Some(remote_rpc_client),
            cursor_path: cursor_path,
            noop: noop,
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

    fn get_block_info(&self, slot: u64) -> Option<&BlockInfo> {
        self.block_infos.get(&slot)
    }

    pub fn get_block_from_rpc(&self, slot: u64, try_remote: bool) -> Option<BlockInfo> {
        match self
            .local_rpc_client
            .as_ref()
            .unwrap()
            .get_block_with_config(slot, DEFAULT_RPC_BLOCK_CONFIG)
        {
            Ok(block) => {
                debug!("Block Info fetched locally for slot {}", slot);
                Some(BlockInfo {
                    timestamp: convert_sol_timestamp(block.block_time.unwrap()),
                    parent_slot: block.parent_slot.clone(),
                    slot: slot,
                    block_hash: block.blockhash.clone(),
                    parent_hash: block.previous_blockhash.clone(),
                })
            }
            Err(_err) => {
                if !try_remote {
                    return None;
                }
                match self
                    .remote_rpc_client
                    .as_ref()
                    .unwrap()
                    .get_block_with_config(slot, DEFAULT_RPC_BLOCK_CONFIG)
                {
                    Ok(block) => {
                        debug!("Block Info fetched remotely for slot {}", slot);
                        Some(BlockInfo {
                            timestamp: convert_sol_timestamp(block.block_time.unwrap()),
                            parent_slot: block.parent_slot.clone(),
                            slot: slot,
                            block_hash: block.blockhash.clone(),
                            parent_hash: block.previous_blockhash.clone(),
                        })
                    }
                    Err(_err) => None,
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

    fn should_skip_slot(&mut self, slot: u64) -> bool {
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
        self.process_upto(slot);
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
        debug!("setting block info for slot {}", slot);
        self.block_infos.insert(slot, block_info);

        if self.confirmed_slots.get(&slot).is_some() {
            self.process_upto(slot);
        }
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
    ) {
        let data_hash = if data.len() == 0 {
            0
        } else {
            XxHash64::oneshot(SEED, data)
        };

        if is_startup {
            self.account_data_hash.insert(pub_key.to_vec(), data_hash);
            return;
        }

        if self.should_skip_slot(slot) {
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

        let slot_entries = self
            .block_account_changes
            .entry(slot)
            .or_insert_with(HashMap::new);

        let address = pub_key.to_vec();
        if let Some(prev) = slot_entries.get(&address) {
            if prev.write_version > write_version {
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

        slot_entries.insert(address, awv);
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

    fn process_upto(&mut self, slot: u64) {
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

        for toproc in self.ordered_confirmed_slots_upto(slot) {
            if toproc < self.first_block_to_process.unwrap() {
                continue;
            }

            let mut _rpc_block = None; // lifetime hack for block_info
            let block_info = match self.get_block_info(toproc) {
                Some(bi) => bi,
                None => {
                    // we don't want to use remote RPC unless we have to:
                    // - sometimes early blocks metadata after a restart will never become available
                    // - if blocks start piling up
                    let try_remote = !self.initialized || self.confirmed_slots.len() >= 10 ;
                    match self.get_block_from_rpc(toproc, try_remote) {
                        Some(bi) => {
                            _rpc_block = Some(bi);
                            _rpc_block.as_ref().unwrap()
                        }
                        None => {
                            debug!(
                                "process_upto({}): block info not found for slot {}",
                                slot, toproc
                            );
                            return;
                        }
                    }
                }
            };

            let account_changes = self.get_account_changes(toproc);
            let acc_block = create_account_block(
                account_changes.unwrap_or(&AccountChanges::default()),
                block_info,
            );
            if toproc == self.first_received_blockmeta.unwrap() {
                debug!("First block was sent, now initialized");
                self.initialized = true;
            }
            if self.noop {
                debug!("printing block {} - {} entries (noop mode)", toproc, acc_block.accounts.len());
            } else {
                debug!("printing block {}", toproc);
                BlockPrinter::new(&acc_block).print(self.lib.unwrap());
            }
            self.purge_blocks_up_to(toproc);
            write_cursor(&self.cursor_path, toproc);
        }
    }

    pub fn get_hash_count(&self) -> usize {
        self.account_data_hash.len()
    }
}

fn write_cursor(cursor_file: &str, cursor: u64) {
    std::fs::write(cursor_file, cursor.to_string()).unwrap();
}
