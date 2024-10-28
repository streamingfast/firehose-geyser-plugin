use crate::pb::sf::solana::r#type::v1::{Account, AccountBlock};
use crate::state::{AccountChanges, BlockInfo};
use prost::Message;
use prost_types::Timestamp as ProstTimestamp;
use solana_program::clock::UnixTimestamp;
use std::collections::HashMap;

pub fn convert_sol_timestamp(sol_timestamp: UnixTimestamp) -> ProstTimestamp {
    let seconds = sol_timestamp as i64;
    ProstTimestamp { seconds, nanos: 0 }
}

pub fn create_account_block(
    slot: u64,
    lib_num: u64,
    account_changes: &AccountChanges,
    block_info: &BlockInfo,
) -> AccountBlock {
    let accounts = account_changes
        .into_iter()
        .map(|(account_key, account_data)| Account {
            source_slot: slot,
            data: account_data.clone(),
            address: account_key.into(),
        })
        .collect();

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
