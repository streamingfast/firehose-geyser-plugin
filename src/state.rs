use crate::pb;
use lazy_static::lazy_static;
use pb::sf::solana::r#type::v1::Account;
use prost_types::Timestamp;
use std::collections::HashMap;
use std::io::Write;

type BlockAccountChanges = HashMap<u64, AccountChanges>;
pub type AccountChanges = HashMap<Vec<u8>, AccountWithWriteVersion>;
pub type AccountDataHash = HashMap<Vec<u8>, u64>;

pub type Transactions = HashMap<u64, Vec<ConfirmTransactionWithIndex>>;
type ProcessedSlot = HashMap<u64, bool>;

type BlockInfoMap = HashMap<u64, BlockInfo>;
type ConfirmedSlotsMap = HashMap<u64, bool>;
use crate::pb::sf::solana::r#type::v1::{Block, BlockHeight, Reward, UnixTimestamp};
use crate::plugins::{to_block_rewards, ConfirmTransactionWithIndex};
use log::{debug, error, info, warn};
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
    pub static ref CURSOR_MUTEX: std::sync::Mutex<u64> = std::sync::Mutex::new(0);
}

#[derive(Default, Clone)]
pub struct BlockInfo {
    pub slot: u64,
    pub parent_slot: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub timestamp: Timestamp,
    pub height: Option<u64>,
    pub rewards: Vec<Reward>,
    pub transaction_count: u64,
}

const DEFAULT_RPC_BLOCK_CONFIG: RpcBlockConfig = RpcBlockConfig {
    encoding: None,
    transaction_details: Some(TransactionDetails::Signatures),
    rewards: Some(true),
    commitment: Some(CommitmentConfig::confirmed()),
    max_supported_transaction_version: Some(0),
};

pub struct State {
    output_file: std::fs::File,
}

impl State {
    pub fn new(output_file: std::fs::File) -> Self {
        State { output_file }
    }

    pub fn write(&mut self, data: String) -> Result<(), Box<dyn std::error::Error>> {
        let data = data + "\n";
        self.output_file.write_all(data.as_bytes())?;
        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.output_file.flush()
    }
}
