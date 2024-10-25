use crate::pb::sf::solana::r#type::v1::AccountBlock;
use base64;
use prost::Message;

pub struct BlockPrinter <'a> {
    block: &'a AccountBlock,
}

impl <'a> BlockPrinter <'a>{
    pub fn new(block: &'a AccountBlock) -> Self {
        BlockPrinter {block}
    }

    pub fn print(&self) {
        let b = self.block;
        let encoded_block = b.encode_to_vec();
        let base64_encoded_block = base64::encode(encoded_block);


        let format = format!(
            "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}",
            slot=b.slot,
            block_hash=b.hash,
            parent_slot=b.parent_slot,
            parent_hash=b.parent_hash,
            lib=b.lib,
            timestamp_nano=b.timestamp.as_ref().unwrap().nanos.to_string(),
            payload= base64_encoded_block
        );

        println!("{}", format);
    }
}


