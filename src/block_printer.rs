use crate::state::BlockInfo;
use base64;
use log::debug;
use prost::Message;
use std::fs::File;
use std::io::Write;

pub struct BlockPrinter {
    noop: bool,
    out: File,
}

impl BlockPrinter {
    pub fn new(out: File, noop: bool) -> Self {
        BlockPrinter { noop, out }
    }

    pub fn print_init(&mut self, blocktype: &str) -> std::io::Result<()> {
        if self.noop {
            debug!("printing init for type {} (noop mode)", blocktype);
            Ok(())
        } else {
            writeln!(self.out, "INIT {blocktype}")
        }
    }

    pub fn print(
        &mut self,
        block_info: &BlockInfo,
        lib: u64,
        block: &impl Message,
    ) -> std::io::Result<()> {
        let encoded_block = block.encode_to_vec();
        let base64_encoded_block = base64::encode(encoded_block);

        if self.noop {
            debug!("printing block {} (noop mode)", block_info.slot);
            Ok(())
        } else {
            writeln!(self.out, "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}",
                slot=block_info.slot,
                block_hash=&block_info.block_hash,
                parent_slot=block_info.parent_slot,
                parent_hash=block_info.parent_hash,
                lib=lib,
                timestamp_nano=block_info.timestamp.seconds * 1_000_000_000,
                payload= base64_encoded_block
            )
        }
    }
}
