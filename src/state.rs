use base58;
use prost_types::Timestamp;
use solana_rpc_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::{thread::sleep, time::Duration};
use pb::sf::solana::r#type::v1::Account;
use crate::pb;
use crate::utils::convert_sol_timestamp;

type BlockAccountChanges = HashMap<u64, AccountChanges>;
pub type AccountChanges = HashMap<Vec<u8>, Account>;
type BlockInfoMap = HashMap<u64, BlockInfo>;
type ConfirmedSlotsMap = HashMap<u64, bool>;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_transaction_status::TransactionDetails;

pub struct BlockInfo {
    pub slot: u64,
    pub parent_slot: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub timestamp: Timestamp,
}

#[derive(Default)]
pub struct State {
    first_blockmeta_received: bool,
    last_confirmed_block: u64,
    last_finalized_block: Option<u64>,
    last_purged_block: u64,
    block_account_changes: BlockAccountChanges,
    block_infos: BlockInfoMap,
    confirmed_slots: ConfirmedSlotsMap,
    rpc_client: Option<RpcClient>,
}

impl State {
    pub fn new(rpc_client: RpcClient) -> Self {
        State {
            first_blockmeta_received: false,
            last_confirmed_block: 0,
            last_purged_block: 0,
            block_account_changes: HashMap::new(),
            block_infos: HashMap::new(),
            last_finalized_block: None,
            confirmed_slots: HashMap::new(),
            rpc_client: Some(rpc_client),
        }
    }

    pub fn get_first_blockmeta_received(&self) -> bool {
        self.first_blockmeta_received
    }

    pub fn set_last_finalized_block(&mut self, slot: u64) {
        self.last_finalized_block = Some(slot);
    }

    pub fn is_already_purged(&mut self, slot: u64) -> bool {
        return self.last_purged_block >= slot;
    }

    pub fn get_last_finalized_block(&mut self) -> u64 {
        match self.last_finalized_block {
            None => {
                let commitment_config = CommitmentConfig::finalized();
                loop {
                    println!("Fetch current lib using rpc client");
                    match self
                        .rpc_client
                        .as_ref()
                        .unwrap()
                        .get_slot_with_commitment(commitment_config)
                    {
                        Ok(lib_num) => {
                            println!("Block lib received: {}", lib_num);
                            self.last_finalized_block = Some(lib_num);
                            break lib_num;
                        }
                        Err(e) => {
                            println!("Error getting lib num: {}", e);
                            sleep(Duration::from_millis(50));
                            break 0;
                        }
                    }
                }
            }
            Some(slot) => slot,
        }
    }

    pub fn get_account_changes(&self, slot: u64) -> Option<&AccountChanges> {
        self.block_account_changes.get(&slot)
    }

    pub fn get_block_info(&self, slot: u64) -> Option<&BlockInfo> {
        self.block_infos.get(&slot)
    }

    pub fn get_block_from_rpc(&self, slot: u64) -> BlockInfo {
            let config = RpcBlockConfig {
                encoding: None,
                transaction_details: Some(TransactionDetails::None),
                rewards: Some(false),
                commitment: None,
                max_supported_transaction_version: Some(0),
            };

            let block = self.rpc_client.as_ref().unwrap().get_block_with_config(slot, config).unwrap();
            println!("Block Info fetched for slot {}", slot);
            BlockInfo {
                    timestamp: convert_sol_timestamp(block.block_time.unwrap()),
                    parent_slot: block.parent_slot.clone(),
                    slot: slot,
                    block_hash: block.blockhash.clone(),
                    parent_hash: block.previous_blockhash.clone(),
            }
    }

    pub fn set_block_info(&mut self, slot: u64, block_info: BlockInfo) {
        self.first_blockmeta_received = true;
        self.block_infos.insert(slot, block_info);
    }

    pub fn is_confirmed_slot(&self, slot: u64) -> bool {
        self.confirmed_slots.contains_key(&slot)
    }

    pub fn ordered_confirmed_slots_below(&self, slot: u64) -> Vec<u64> {
        // Collect all keys from confirmed_slots that are less than the given slot
        let mut slots: Vec<u64> = self.confirmed_slots.keys().cloned().filter(|&x| x < slot).collect();
        slots.sort();
        slots
    }

    pub fn set_confirmed_slot(&mut self, slot: u64) {
        self.confirmed_slots.insert(slot, true);
        self.last_confirmed_block = slot;
    }

    pub fn set_account(&mut self, slot: u64, pub_key: Vec<u8>, account: Account) {
        if !self.block_account_changes.contains_key(&slot) {
            println!("account data for slot {}", slot);
        }

        self.block_account_changes
            .entry(slot)
            .or_insert_with(HashMap::new)
            .insert(pub_key, account);
    }

    pub fn purge_blocks_up_to(&mut self, slot: u64) {
        let blocks: Vec<u64> = self.block_account_changes.keys().cloned().collect();
        for block in blocks {
            if block > slot {
                continue;
            }
            // println!("purging block {}", block);
            self.block_account_changes.remove(&block);
            self.block_infos.remove(&block);
        }
        for slot in self.confirmed_slots.keys().cloned().collect::<Vec<u64>>() {
            if slot <= slot {
                self.confirmed_slots.remove(&slot);
            }
        }
        self.last_purged_block = slot;
    }

    pub fn stats(&mut self) {
        println!(
            "last_confirmed_block: {}, last_purged_block: {}, block_account_changes: {}",
            self.last_confirmed_block,
            self.last_purged_block,
            self.block_account_changes.len()
        )
    }
}
