use crate::pb::sf::solana::r#type::v1::AccountBlock;
use base64;
use log::debug;
use prost::Message;

#[derive(Default)]
pub struct BlockPrinter {
    noop: bool,
}

impl BlockPrinter {
    pub fn new(noop: bool) -> Self {
        BlockPrinter { noop }
    }

    pub fn print(
        &self,
        slot: u64,
        hash: &String,
        lib: u64,
        parent_slot: u64,
        parent_hash: &String,
        timestamp: &prost_types::Timestamp,
        block: &impl Message,
    ) {
        let encoded_block = block.encode_to_vec();
        let base64_encoded_block = base64::encode(encoded_block);

        let format = format!(
            "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}",
            slot=slot,
            block_hash=hash,
            parent_slot=parent_slot,
            parent_hash=parent_hash,
            lib=lib,
            timestamp_nano=timestamp.seconds * 1_000_000_000,
            payload= base64_encoded_block
        );
        if self.noop {
            debug!("printing block {} (noop mode)", slot);
        } else {
            println!("{}", format);
        }
    }
}
