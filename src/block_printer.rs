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
        if self.noop {
            debug!("printing block {} (noop mode)", block_info.slot);
            Ok(())
        } else {
            let mut out = self.out.try_clone().unwrap();
            let slot = block_info.slot;
            let block_hash = block_info.block_hash.clone();
            let parent_slot = block_info.parent_slot;
            let parent_hash = block_info.parent_hash.clone();
            let timestamp_nano = block_info.timestamp.seconds * 1_000_000_000;
            let lib = lib;
            let encoded_block = block.encode_to_vec();
            let handle = std::thread::spawn(move || {
                let base64_encoded_block = base64::encode(encoded_block);
                let payload = base64_encoded_block;
                writeln!(out, "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}")
            });
            handle.join().unwrap()
        }
    }
}
