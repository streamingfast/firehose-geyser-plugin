use crate::pb::sf::solana::r#type::v1::{Account, AccountBlock};
use crate::state::{AccountChanges, BlockInfo};
use base58::ToBase58;
use log::debug;
use prost_types::Timestamp as ProstTimestamp;
use solana_program::clock::UnixTimestamp;

const DERIVED_ACCOUNT: &str = "9QiiQiqg2riRns9CAuVvgFsAQ1RM6CH38EFysZ6R8Nac";

pub fn convert_sol_timestamp(sol_timestamp: UnixTimestamp) -> ProstTimestamp {
    let seconds = sol_timestamp as i64;
    ProstTimestamp { seconds, nanos: 0 }
}

pub fn create_account_block(
    account_changes: &AccountChanges,
    block_info: &BlockInfo,
) -> AccountBlock {
    let mut accounts: Vec<Account> = account_changes
        .into_iter()
        .map(|(_account_key, account)| account.account.clone())
        .collect();

    accounts.sort_by(|a, b| a.address.cmp(&b.address));
    for account in accounts.iter() {
        if account.address.to_base58() == DERIVED_ACCOUNT {
            debug!(
                "creating block data: received my account: {} (owner: {}) on slot {}",
                account.owner.to_base58(),
                account.address.to_base58(),
                block_info.slot
            );
        }
    }

    AccountBlock {
        slot: block_info.slot,
        hash: block_info.block_hash.clone(),
        parent_hash: block_info.parent_hash.clone(),
        parent_slot: block_info.parent_slot,
        accounts: accounts,
        timestamp: Some(block_info.timestamp.clone()),
    }
}
