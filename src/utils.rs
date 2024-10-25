use solana_program::clock::UnixTimestamp;
use prost_types::{Timestamp as ProstTimestamp};
use crate::pb::sf::solana::r#type::v1::{AccountBlock, Account};
use crate::state::{AccountChanges, BlockInfo};
use std::collections::HashMap;
use prost::Message;

pub fn convert_sol_timestamp(sol_timestamp: UnixTimestamp) -> ProstTimestamp {
    let seconds = sol_timestamp as i64;
    ProstTimestamp {
        seconds,
        nanos: 0,
    }
}

pub fn create_account_block(slot: u64, lib_num: u64, account_changes: &AccountChanges, block_info: &BlockInfo) -> AccountBlock {
    let accounts = account_changes.into_iter().map(|(account_key, account_data)| {
        Account {
            source_slot: slot,
            data: account_data.clone(),
            address: account_key.into(),
        }
    }).collect();

    AccountBlock {
        slot: block_info.slot,
        hash: block_info.block_hash.clone(),
        parent_hash: block_info.parent_hash.clone(),
        lib: lib_num,
        parent_slot: block_info.parent_slot,
        accounts,
        timestamp: Some(block_info.timestamp.clone()),
    }
}
