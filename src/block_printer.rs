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

                info!(
                    "printing block {} {} with transaction count of {}",
                    block.slot,
                    block_hash,
                    block.transactions.len()
                );

                if noop {
                    info!("printing block {} (noop mode)", slot);
                } else {
                    let _lock = BLOCK_MUTEX.lock().expect("block_mutex lock poisoned");
                    writeln!(out_block, "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}").expect("cannot write to out_block");
                }
                write_cursor(&cursor_path, slot);
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
                }
                write_cursor(&cursor_path, slot);
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

// write_cursor writes the cursor the second time it is called with the same value, because we must write both the account block and the normal block
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
    use crate::pb::sf::solana::r#type::v1::{Account, ConfirmedTransaction};
    use prost_types::Timestamp;

    use tempfile::NamedTempFile;

    fn create_test_block_info() -> BlockInfo {
        BlockInfo {
            slot: 12345,
            parent_slot: 12344,
            block_hash: "test_block_hash".to_string(),
            parent_hash: "test_parent_hash".to_string(),
            timestamp: Timestamp {
                seconds: 1640995200,
                nanos: 0,
            },
            height: Some(10000),
            rewards: vec![],
            transaction_count: 2,
        }
    }

    fn create_test_block() -> Block {
        Block {
            slot: 12345,
            previous_blockhash: "test_parent_hash".to_string(),
            blockhash: "test_block_hash".to_string(),
            parent_slot: 12344,
            transactions: vec![
                ConfirmedTransaction {
                    transaction: None,
                    meta: None,
                },
                ConfirmedTransaction {
                    transaction: None,
                    meta: None,
                },
            ],
            rewards: vec![],
            block_time: None,
            block_height: None,
        }
    }

    fn create_test_account_block() -> AccountBlock {
        AccountBlock {
            slot: 12345,
            hash: "test_block_hash".to_string(),
            parent_slot: 12344,
            parent_hash: "test_parent_hash".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1640995200,
                nanos: 0,
            }),
            accounts: vec![Account {
                address: vec![1, 2, 3, 4],
                owner: vec![5, 6, 7, 8],
                data: vec![10, 20, 30],
                deleted: false,
            }],
        }
    }

    #[test]
    fn test_print_with_only_block() {
        // Reset cursor state
        {
            let mut last = CURSOR_MUTEX.lock().expect("cursor_mutex lock poisoned");
            *last = 0;
        }

        let block_file = NamedTempFile::new().unwrap();
        let cursor_file = NamedTempFile::new().unwrap();

        let block_path = block_file.path().to_str().unwrap();
        let cursor_path = cursor_file.path().to_str().unwrap();

        let mut printer = BlockPrinter::new(Some(File::create(block_path).unwrap()), None, false);

        let block_info = create_test_block_info();
        let block = create_test_block();
        let account_block = create_test_account_block();

        printer
            .print(&block_info, 100, block.clone(), account_block, cursor_path)
            .unwrap();

        // Wait for threads to complete
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Check block file content
        let block_content = std::fs::read_to_string(block_path).unwrap();
        let lines: Vec<&str> = block_content.trim().split('\n').collect();
        assert_eq!(lines.len(), 1);

        let line = lines[0];
        let parts: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(parts[0], "FIRE");
        assert_eq!(parts[1], "BLOCK");
        assert_eq!(parts[2], "12345"); // slot
        assert_eq!(parts[3], "test_block_hash"); // block_hash
        assert_eq!(parts[4], "12344"); // parent_slot
        assert_eq!(parts[5], "test_parent_hash"); // parent_hash
        assert_eq!(parts[6], "100"); // lib
        assert_eq!(parts[7], "1640995200000000000"); // timestamp_nano

        let payload = parts[8];
        // Decode the base64 payload and verify it matches our original block
        let decoded_bytes = rbase64::decode(payload).unwrap();
        let decoded_block = Block::decode(&decoded_bytes[..]).unwrap();
        assert_eq!(decoded_block.slot, block.slot);
        assert_eq!(decoded_block.blockhash, block.blockhash);
        assert_eq!(decoded_block.transactions.len(), block.transactions.len());

        // Check cursor file content (should be written twice - once for each call)
        let cursor_content = std::fs::read_to_string(cursor_path).unwrap();
        assert_eq!(cursor_content, "12345");
    }

    #[test]
    fn test_print_with_only_account_block() {
        // Reset cursor state
        {
            let mut last = CURSOR_MUTEX.lock().expect("cursor_mutex lock poisoned");
            *last = 0;
        }

        let account_file = NamedTempFile::new().unwrap();
        let cursor_file = NamedTempFile::new().unwrap();

        let account_path = account_file.path().to_str().unwrap();
        let cursor_path = cursor_file.path().to_str().unwrap();

        let mut printer = BlockPrinter::new(None, Some(File::create(account_path).unwrap()), false);

        let block_info = create_test_block_info();
        let block = create_test_block();
        let account_block = create_test_account_block();

        printer
            .print(&block_info, 100, block, account_block.clone(), cursor_path)
            .unwrap();

        // Wait for threads to complete
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Check account file content
        let account_content = std::fs::read_to_string(account_path).unwrap();
        let lines: Vec<&str> = account_content.trim().split('\n').collect();
        assert_eq!(lines.len(), 1);

        let line = lines[0];
        let parts: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(parts[0], "FIRE");
        assert_eq!(parts[1], "BLOCK");
        assert_eq!(parts[2], "12345"); // slot
        assert_eq!(parts[3], "test_block_hash"); // block_hash
        assert_eq!(parts[4], "12344"); // parent_slot
        assert_eq!(parts[5], "test_parent_hash"); // parent_hash
        assert_eq!(parts[6], "100"); // lib
        assert_eq!(parts[7], "1640995200000000000"); // timestamp_nano

        let payload = parts[8];
        // Decode the base64 payload and verify it matches our original block
        let decoded_bytes = rbase64::decode(payload).unwrap();
        let decoded_block = AccountBlock::decode(&decoded_bytes[..]).unwrap();
        assert_eq!(decoded_block.slot, account_block.slot);
        assert_eq!(decoded_block.hash, account_block.hash);
        assert_eq!(decoded_block.accounts.len(), account_block.accounts.len());

        // Check cursor file content
        let cursor_content = std::fs::read_to_string(cursor_path).unwrap();
        assert_eq!(cursor_content, "12345");
    }

    #[test]
    fn test_print_with_both_files() {
        // Reset cursor state
        {
            let mut last = CURSOR_MUTEX.lock().expect("cursor_mutex lock poisoned");
            *last = 0;
        }

        let block_file = NamedTempFile::new().unwrap();
        let account_file = NamedTempFile::new().unwrap();
        let cursor_file = NamedTempFile::new().unwrap();

        let block_path = block_file.path().to_str().unwrap();
        let account_path = account_file.path().to_str().unwrap();
        let cursor_path = cursor_file.path().to_str().unwrap();

        let mut printer = BlockPrinter::new(
            Some(File::create(block_path).unwrap()),
            Some(File::create(account_path).unwrap()),
            false,
        );

        let block_info = create_test_block_info();
        let block = create_test_block();
        let account_block = create_test_account_block();

        printer
            .print(&block_info, 100, block, account_block, cursor_path)
            .unwrap();

        // Wait for threads to complete
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Check block file content
        let block_content = std::fs::read_to_string(block_path).unwrap();
        let block_lines: Vec<&str> = block_content.trim().split('\n').collect();
        assert_eq!(block_lines.len(), 1);

        let block_line = block_lines[0];
        let block_parts: Vec<&str> = block_line.split_whitespace().collect();
        assert_eq!(block_parts[0], "FIRE");
        assert_eq!(block_parts[1], "BLOCK");
        assert_eq!(block_parts[2], "12345");
        // Check account file content
        let account_content = std::fs::read_to_string(account_path).unwrap();
        let account_lines: Vec<&str> = account_content.trim().split('\n').collect();
        assert_eq!(account_lines.len(), 1);

        let account_line = account_lines[0];
        let account_parts: Vec<&str> = account_line.split_whitespace().collect();
        assert_eq!(account_parts[0], "FIRE");
        assert_eq!(account_parts[1], "BLOCK");
        assert_eq!(account_parts[2], "12345");

        // Check cursor file content (should be written after both calls complete)
        let cursor_content = std::fs::read_to_string(cursor_path).unwrap();
        assert_eq!(cursor_content, "12345");
    }

    #[test]
    fn test_print_noop_mode() {
        // Reset cursor state
        {
            let mut last = CURSOR_MUTEX.lock().expect("cursor_mutex lock poisoned");
            *last = 0;
        }

        let block_file = NamedTempFile::new().unwrap();
        let account_file = NamedTempFile::new().unwrap();
        let cursor_file = NamedTempFile::new().unwrap();

        let block_path = block_file.path().to_str().unwrap();
        let account_path = account_file.path().to_str().unwrap();
        let cursor_path = cursor_file.path().to_str().unwrap();

        let mut printer = BlockPrinter::new(
            Some(File::create(block_path).unwrap()),
            Some(File::create(account_path).unwrap()),
            true, // noop mode
        );

        let block_info = create_test_block_info();
        let block = create_test_block();
        let account_block = create_test_account_block();

        printer
            .print(&block_info, 100, block, account_block, cursor_path)
            .unwrap();

        // Wait for threads to complete
        std::thread::sleep(std::time::Duration::from_millis(100));

        // In noop mode, files should be empty (printing is skipped)
        let block_content = std::fs::read_to_string(block_path).unwrap();
        assert_eq!(block_content.trim(), "");

        let account_content = std::fs::read_to_string(account_path).unwrap();
        assert_eq!(account_content.trim(), "");

        // Cursor should still be written (cursor updates are outside noop gate)
        let cursor_content = std::fs::read_to_string(cursor_path).unwrap();
        assert_eq!(cursor_content, "12345");
    }

    #[test]
    fn test_cursor_behavior_noop_vs_normal() {
        // This test explicitly verifies that cursor updates happen in both modes
        // Reset cursor state
        {
            let mut last = CURSOR_MUTEX.lock().expect("cursor_mutex lock poisoned");
            *last = 0;
        }

        let cursor_file_noop = NamedTempFile::new().unwrap();
        let cursor_file_normal = NamedTempFile::new().unwrap();
        let block_file = NamedTempFile::new().unwrap();

        let cursor_path_noop = cursor_file_noop.path().to_str().unwrap();
        let cursor_path_normal = cursor_file_normal.path().to_str().unwrap();
        let block_path = block_file.path().to_str().unwrap();

        // Test noop mode - cursor should be updated
        let mut printer_noop = BlockPrinter::new(
            Some(File::create(block_path).unwrap()),
            None,
            true, // noop mode
        );

        let block_info = create_test_block_info();
        let block = create_test_block();
        let account_block = create_test_account_block();

        printer_noop
            .print(
                &block_info,
                100,
                block.clone(),
                account_block.clone(),
                cursor_path_noop,
            )
            .unwrap();

        // Wait for noop thread to complete
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Reset cursor for normal mode test
        {
            let mut last = CURSOR_MUTEX.lock().expect("cursor_mutex lock poisoned");
            *last = 0;
        }

        // Test normal mode - cursor should also be updated
        let mut printer_normal = BlockPrinter::new(
            Some(File::create(block_path).unwrap()),
            None,
            false, // normal mode
        );

        printer_normal
            .print(&block_info, 100, block, account_block, cursor_path_normal)
            .unwrap();

        // Wait for normal thread to complete
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Both cursors should be written with the same value
        let cursor_content_noop = std::fs::read_to_string(cursor_path_noop).unwrap();
        let cursor_content_normal = std::fs::read_to_string(cursor_path_normal).unwrap();

        assert_eq!(
            cursor_content_noop, "12345",
            "Cursor should be updated in noop mode"
        );
        assert_eq!(
            cursor_content_normal, "12345",
            "Cursor should be updated in normal mode"
        );
        assert_eq!(
            cursor_content_noop, cursor_content_normal,
            "Cursor behavior should be identical regardless of noop mode"
        );
    }
    #[test]
    fn test_write_cursor() {
        // Reset cursor state
        {
            let mut last = CURSOR_MUTEX.lock().expect("cursor_mutex lock poisoned");
            *last = 0;
        }

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
