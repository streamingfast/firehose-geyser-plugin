use std::collections::HashMap;
use base58;

type BlockAccountChanges = HashMap<u64, HashMap<String, Vec<u8>>>;

#[derive(Debug, Default)]
pub struct State {
    last_confirmed_block: u64,
    last_purged_block: u64,
    block_account_changes: BlockAccountChanges,
}

impl State {
    pub fn new() -> Self {
        State {
            last_confirmed_block: 0,
            last_purged_block: 0,
            block_account_changes: HashMap::new(),
        }
    }

    pub fn set_last_confirmed_block(&mut self, block_num: u64) {
        // info!(
        //     "setting last confirmed block - new_confirm_block: {}, last_confirm_block: {}",
        //     block_num, self.last_confirmed_block
        // );
        self.last_confirmed_block = block_num;
    }

    pub fn set_account_data(&mut self, block_num: u64, account: Vec<u8>, data: Vec<u8>) {
        if block_num <= self.last_confirmed_block && self.last_confirmed_block != 0 {
            // info!(
            //     "received account data for a skipped block - skipped_block_num: {}, last_confirmed_block: {}",
            //     block_num, self.last_confirmed_block
            // );
        }

        self.block_account_changes
            .entry(block_num)
            .or_insert_with(HashMap::new)
            .insert(base58::ToBase58::to_base58(account.as_slice()), data);
    }

    pub fn purge_confirmed_blocks(&mut self, block_num: u64) {
        // info!(
        //     "purging confirmed blocks - purge_block: {}, previous_purged_block: {}",
        //     block_num, self.last_purged_block
        // );

        let blocks: Vec<u64> = self.block_account_changes.keys().cloned().collect();
        for block in blocks {
            self.block_account_changes.remove(&block);
            if self.last_confirmed_block > 0 {
                self.last_confirmed_block -= 1;
            }
        }
        self.last_purged_block = block_num;
    }

    pub fn stats(&mut self) {
        println!(
            "last_confirmed_block: {}, last_purged_block: {}, block_account_changes: {}",
            self.last_confirmed_block, self.last_purged_block, self.block_account_changes.len()
        )
    }
}
