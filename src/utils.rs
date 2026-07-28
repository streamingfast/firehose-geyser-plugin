use crate::pb::sf::solana::r#type::v1::{Account, AccountBlock};
use crate::state::BlockInfo;
use prost_types::Timestamp as ProstTimestamp;
use solana_clock::UnixTimestamp;

pub fn convert_sol_timestamp(sol_timestamp: UnixTimestamp) -> ProstTimestamp {
    let seconds = sol_timestamp as i64;
    ProstTimestamp { seconds, nanos: 0 }
}

pub fn create_account_block(account_changes: Vec<Account>, block_info: &BlockInfo) -> AccountBlock {
    AccountBlock {
        slot: block_info.slot,
        hash: block_info.block_hash.clone(),
        parent_hash: block_info.parent_hash.clone(),
        parent_slot: block_info.parent_slot,
        accounts: account_changes,
        timestamp: Some(block_info.timestamp.clone()),
    }
}
