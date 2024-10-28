use std::collections::HashMap;
use base58;
use prost_types::Timestamp;

type BlockAccountChanges = HashMap<u64, HashMap<String, Vec<u8>>>;
pub type AccountChanges = HashMap<String, Vec<u8>>;
type BlockInfoMap = HashMap<u64, BlockInfo>;
type ConfirmedSlotsMap = HashMap<u64, bool>;

#[derive(Debug)]
pub struct BlockInfo {
    pub slot: u64,
    pub parent_slot: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub timestamp: Timestamp
}

#[derive(Debug, Default)]
pub struct State {
    last_confirmed_block: u64,
    last_finalized_block: Option<u64>,
    last_purged_block: u64,
    block_account_changes: BlockAccountChanges,
    block_infos: BlockInfoMap,
    confirmed_slots: ConfirmedSlotsMap,
}

impl State {
    pub fn new() -> Self {
        State {
            last_confirmed_block: 0,
            last_purged_block: 0,
            block_account_changes: HashMap::new(),
            block_infos: HashMap::new(),
            last_finalized_block: None,
            confirmed_slots: HashMap::new(),
        }
    }

    pub fn set_last_confirmed_block(&mut self, slot: u64) {
        self.last_confirmed_block = slot;
    }

    pub fn set_last_finalized_block(&mut self, slot: u64) {
        self.last_finalized_block = Some(slot);
    }
    
    pub fn get_last_finalized_block(&self, slot: u64) -> u64 {
        match self.last_finalized_block{
            // FIXME this will break sometimes with holes
            None => {
                if slot < 32 {
                    0
                } else {
                    slot - 32
                }
            }
                Some(last_finalized_block) => last_finalized_block,
            }
    }
    
    pub fn get_account_changes(&self, slot: u64) -> Option<&HashMap<String, Vec<u8>>> {
        self.block_account_changes.get(&slot)
    }

    pub fn get_block_info(&self, slot: u64) -> Option<&BlockInfo> {
        self.block_infos.get(&slot)
    }
    
    pub fn set_block_info(&mut self, slot: u64, block_info: BlockInfo) {
        self.block_infos.insert(slot, block_info);
    }

    pub fn ordered_confirmed_slots_up_to(&self, slot: u64) -> Vec<u64> {
        // Collect all keys from confirmed_slots that are less than or equal to the given slot
        let mut slots: Vec<u64> = self.confirmed_slots.keys().cloned().filter(|&x| x <= slot).collect();
        slots.sort();
        slots
    }
    
    pub fn set_confirmed_slot(&mut self, slot: u64) {
        self.confirmed_slots.insert(slot, true);
    }

    pub fn set_account_data(&mut self, slot: u64, account: Vec<u8>, data: Vec<u8>) {
        if !self.block_account_changes.contains_key(&slot) {
          println!("account data for slot {}", slot);
        }

        self.block_account_changes
            .entry(slot)
            .or_insert_with(HashMap::new)
            .insert(base58::ToBase58::to_base58(account.as_slice()), data);

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
            self.last_confirmed_block, self.last_purged_block, self.block_account_changes.len()
        )
    }
}
