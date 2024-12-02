use crate::pb::sf::solana::r#type::v1::{AccountBlock, Block};
use rbase64;
use prost::Message;

pub struct BlockPrinter<'a> {
    block: &'a AccountBlock,
}

impl<'a> BlockPrinter<'a> {
    pub fn new(block: &'a AccountBlock) -> Self {
        BlockPrinter { block }
    }

    pub fn print(&self, lib: u64) {
        let b = self.block;
        let encoded_block = b.encode_to_vec();

        println!(
            "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}",
            slot=b.slot,
            block_hash=b.hash,
            parent_slot=b.parent_slot,
            parent_hash=b.parent_hash,
            lib=lib,
            timestamp_nano=b.timestamp.as_ref().unwrap().seconds * 1_000_000_000,
            payload=rbase64::encode(&encoded_block)
        );

    }
}

pub struct TrxBlockPrinter<'a> {
    block: &'a Block,
}

impl<'a> TrxBlockPrinter<'a> {
    pub fn new(block: &'a Block) -> Self {
        TrxBlockPrinter { block }
    }

    pub fn print(&self, lib: u64) {
        let b = self.block;
        let encoded_block = b.encode_to_vec();

        println!(
            "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}",
            slot=b.slot,
            block_hash=b.blockhash,
            parent_slot=b.parent_slot,
            parent_hash=b.previous_blockhash,
            lib=lib,
            timestamp_nano=b.block_time.as_ref().unwrap().timestamp * 1_000_000_000,
            payload=rbase64::encode(&encoded_block)
        );

    }
}
