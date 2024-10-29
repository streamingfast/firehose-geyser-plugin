use crate::pb::sf::solana::r#type::v1::{Account, AccountBlock};
use crate::state::{AccountChanges, BlockInfo};
use prost_types::Timestamp as ProstTimestamp;
use solana_program::clock::UnixTimestamp;

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
    let accounts: Vec<Account> = account_changes
        .into_iter()
        .map(|(account_key, account)| account.clone())
        .collect();

    AccountBlock {
        slot: block_info.slot,
        hash: block_info.block_hash.clone(),
        parent_hash: block_info.parent_hash.clone(),
        lib: lib_num,
        parent_slot: block_info.parent_slot,
        accounts: accounts,
        timestamp: Some(block_info.timestamp.clone()),
    }
}
