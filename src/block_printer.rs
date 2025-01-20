use crate::pb::sf::solana::r#type::v1::{AccountBlock, Block};
use crate::state::{BlockInfo, ACC_MUTEX, BLOCK_MUTEX, CURSOR_MUTEX};
use log::{debug, info};
use prost::Message;
use rbase64;
use std::fs::File;
use std::io::Write;

pub struct BlockPrinter {
    noop: bool,
    out_block: Option<File>,
    out_account: Option<File>,
}

impl BlockPrinter {
    pub fn new(out_block: Option<File>, out_account: Option<File>, noop: bool) -> Self {
        BlockPrinter {
            noop,
            out_block,
            out_account,
        }
    }

    pub fn print_init(
        &mut self,
        block_type: &str,
        account_block_type: &str,
    ) -> std::io::Result<()> {
        if self.noop {
            debug!(
                "printing init for type {} and {} (noop mode)",
                block_type, account_block_type
            );
            Ok(())
        } else {
            if let Some(ref mut out_block) = self.out_block {
                if let Err(e) = writeln!(out_block, "FIRE INIT 3.0 {block_type}") {
                    return Err(e);
                }
            }
            if let Some(ref mut out_account) = self.out_account {
                if let Err(e) = writeln!(out_account, "FIRE INIT 3.0 {account_block_type}") {
                    return Err(e);
                }
            }
            Ok(())
        }
    }

    pub fn print(
        &mut self,
        block_info: &BlockInfo,
        lib: u64,
        block: Block,
        account_block: AccountBlock,
        cursor_path: &str,
    ) -> std::io::Result<()> {
        let slot = block_info.slot;
        let parent_slot = block_info.parent_slot;
        let timestamp_nano = block_info.timestamp.seconds * 1_000_000_000;
        let noop = self.noop;
        if let Some(out_block) = &self.out_block {
            let mut out_block = out_block.try_clone().expect("cannot clone out_block");
            let block_hash = block_info.block_hash.clone();
            let parent_hash = block_info.parent_hash.clone();
            let cursor_path = cursor_path.to_string();

            std::thread::spawn(move || {
                let encoded_block = block.encode_to_vec();
                let base64_encoded_block = rbase64::encode(&encoded_block);
                let payload = base64_encoded_block;

                info!("printing block {} {} with transaction count of {}", block.slot, block_hash, block.transactions.len());

                if noop {
                    info!("printing block {} (noop mode)", slot);
                } else {
                    let _lock = BLOCK_MUTEX.lock().expect("block_mutex lock poisoned");
                    writeln!(out_block, "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}").expect("cannot write to out_block");
                    write_cursor(&cursor_path, slot);
                }
            });
        } else {
            write_cursor(cursor_path, slot); // must still be called twice
        }

        if let Some(out_account) = &self.out_account {
            let mut out_account = out_account.try_clone().expect("cannot clone out_account");
            let block_hash = block_info.block_hash.clone();
            let parent_hash = block_info.parent_hash.clone();
            let cursor_path = cursor_path.to_string();
            std::thread::spawn(move || {
                let encoded_account_block = account_block.encode_to_vec();

                let base64_encoded_block = rbase64::encode(&encoded_account_block);
                let payload = base64_encoded_block;
                if noop {
                    info!("printing account_block {} (noop mode)", slot);
                } else {
                    let _lock = ACC_MUTEX.lock().expect("acc_mutex lock poisoned");
                    writeln!(out_account, "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}").expect("cannot write to out_account");
                    write_cursor(&cursor_path, slot);
                }
            });
        } else {
            write_cursor(cursor_path, slot); // must still be called twice
        }

        // We are not waiting for the threads to finish, so that the plugin can be called again for the updates. The lock is only used to prevent interleaving of the output.
        // If an error occurs while writing, the expect() will make it panic and poison the mutex.
        // TODO: updating the cursor should be done with that knowledge (maybe wrapping the cursor in the mutex?)
        Ok(())
    }
}

// write_cursor writes the cursor the second time it is called with the same value
// We should normally receive 1, 1, 2, 2, 3, 3, etc.
// In case we receive 1, 1, 2, 3, 2, 3 -- we ignore a lower value, so we ignore the second '2': The cursor will be set to 1, then 3.
// If that situation persists, the worst that can happen is that the cursor moves only every other block.
// This would be less damageful that moving the cursor while one of the two blocks wasn't correctly written.
fn write_cursor(cursor_file: &str, cursor: u64) {
    let mut last = CURSOR_MUTEX.lock().expect("cursor_mutex lock poisoned");
    if *last < cursor {
        *last = cursor;
        return;
    }
    if *last == cursor {
        std::fs::write(cursor_file, cursor.to_string()).expect("cannot write cursor");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_write_cursor() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        // First pair - 1,1
        write_cursor(&path, 1);
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(content, "");
        write_cursor(&path, 1);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "1");

        // Second pair - 2,3
        write_cursor(&path, 2);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "1");
        write_cursor(&path, 3);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "1");

        // Third pair - 2,3
        write_cursor(&path, 2);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "1");
        write_cursor(&path, 3);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "3");

        // Third pair - 4,4
        write_cursor(&path, 4);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "3");
        write_cursor(&path, 4);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "4");
    }
}
