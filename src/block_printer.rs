use crate::pb::sf::solana::r#type::v1::{AccountBlock, Block};
use crate::state::{BlockInfo, ACC_MUTEX, BLOCK_MUTEX, CURSOR_MUTEX};
use log::{debug, error, info, warn};
use prost::Message;
use rbase64;
use std::fs::File;
use std::io::Write;
use std::time::Instant;

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
                    error!("failed writing FIRE INIT for block stream: {}", e);
                    return Err(e);
                }
            }
            if let Some(ref mut out_account) = self.out_account {
                if let Err(e) = writeln!(out_account, "FIRE INIT 3.0 {account_block_type}") {
                    error!("failed writing FIRE INIT for account stream: {}", e);
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
        let account_count = account_block.accounts.len();
        let tx_count = block.transactions.len();

        info!(
            "block_printer: schedule slot {} (txs={}, accounts={}, noop={}, has_block_out={}, has_account_out={})",
            slot,
            tx_count,
            account_count,
            noop,
            self.out_block.is_some(),
            self.out_account.is_some()
        );

        if let Some(out_block) = &self.out_block {
            let mut out_block = match out_block.try_clone() {
                Ok(f) => f,
                Err(e) => {
                    error!("cannot clone out_block for slot {}: {}", slot, e);
                    return Err(e);
                }
            };
            let block_hash = block_info.block_hash.clone();
            let parent_hash = block_info.parent_hash.clone();
            let cursor_path = cursor_path.to_string();

            if noop {
                info!("printing block {} (noop mode)", slot);
                write_cursor(&cursor_path, slot);
            } else {
                std::thread::spawn(move || {
                    let started = Instant::now();
                    info!(
                        "printing block {} {} with transaction count of {} (encode starting)",
                        block.slot, block_hash, block.transactions.len()
                    );
                    let encoded_block = block.encode_to_vec();
                    let encoded_len = encoded_block.len();
                    let base64_encoded_block = rbase64::encode(&encoded_block);
                    let payload = base64_encoded_block;
                    info!(
                        "block_printer: encoded block {} (protobuf_bytes={}, base64_bytes={}) in {:?}",
                        slot,
                        encoded_len,
                        payload.len(),
                        started.elapsed()
                    );

                    let _lock = match BLOCK_MUTEX.lock() {
                        Ok(lock) => lock,
                        Err(e) => {
                            error!(
                                "block_mutex poisoned while writing block {}: {}",
                                slot, e
                            );
                            // Re-panic so callers detect poison on next check, but after a clear log line.
                            panic!("block_mutex lock poisoned while writing block {}", slot);
                        }
                    };
                    if let Err(e) = writeln!(
                        out_block,
                        "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}"
                    ) {
                        error!(
                            "cannot write block {} to out_block fifo ({}): {}",
                            slot,
                            e.kind(),
                            e
                        );
                        // Keep previous fail-fast behavior so poison is visible upstream.
                        panic!("cannot write to out_block for slot {}: {}", slot, e);
                    }
                    info!(
                        "block_printer: wrote block {} to fifo in {:?}",
                        slot,
                        started.elapsed()
                    );
                    write_cursor(&cursor_path, slot);
                });
            }
        } else {
            write_cursor(cursor_path, slot); // must still be called twice
        }

        if let Some(out_account) = &self.out_account {
            let mut out_account = match out_account.try_clone() {
                Ok(f) => f,
                Err(e) => {
                    error!("cannot clone out_account for slot {}: {}", slot, e);
                    return Err(e);
                }
            };
            let block_hash = block_info.block_hash.clone();
            let parent_hash = block_info.parent_hash.clone();
            let cursor_path = cursor_path.to_string();

            if noop {
                info!("printing account_block {} (noop mode)", slot);
                write_cursor(&cursor_path, slot);
            } else {
                std::thread::spawn(move || {
                    let started = Instant::now();
                    info!(
                        "block_printer: encoding account_block {} (accounts={})",
                        slot, account_count
                    );
                    let encoded_account_block = account_block.encode_to_vec();
                    let encoded_len = encoded_account_block.len();
                    let base64_encoded_block = rbase64::encode(&encoded_account_block);
                    let payload = base64_encoded_block;
                    info!(
                        "block_printer: encoded account_block {} (protobuf_bytes={}, base64_bytes={}) in {:?}",
                        slot,
                        encoded_len,
                        payload.len(),
                        started.elapsed()
                    );

                    let _lock = match ACC_MUTEX.lock() {
                        Ok(lock) => lock,
                        Err(e) => {
                            error!(
                                "acc_mutex poisoned while writing account_block {}: {}",
                                slot, e
                            );
                            panic!(
                                "acc_mutex lock poisoned while writing account_block {}",
                                slot
                            );
                        }
                    };
                    if let Err(e) = writeln!(
                        out_account,
                        "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}"
                    ) {
                        error!(
                            "cannot write account_block {} to out_account fifo ({}): {}",
                            slot,
                            e.kind(),
                            e
                        );
                        panic!("cannot write to out_account for slot {}: {}", slot, e);
                    }
                    info!(
                        "block_printer: wrote account_block {} to fifo in {:?}",
                        slot,
                        started.elapsed()
                    );
                    write_cursor(&cursor_path, slot);
                });
            }
        } else {
            write_cursor(cursor_path, slot); // must still be called twice
        }

        // We are not waiting for the threads to finish, so that the plugin can be called again for the updates. The lock is only used to prevent interleaving of the output.
        // If an error occurs while writing, we log then panic so the mutex is poisoned and process_upto can surface it.
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
    let mut last = match CURSOR_MUTEX.lock() {
        Ok(lock) => lock,
        Err(e) => {
            error!("cursor_mutex poisoned while writing cursor {}: {}", cursor, e);
            panic!("cursor_mutex lock poisoned while writing cursor {}", cursor);
        }
    };
    if *last < cursor {
        *last = cursor;
        return;
    }
    if *last == cursor {
        if let Err(e) = std::fs::write(cursor_file, cursor.to_string()) {
            error!(
                "cannot write cursor {} to {}: {}",
                cursor, cursor_file, e
            );
            panic!(
                "cannot write cursor {} to {}: {}",
                cursor, cursor_file, e
            );
        }
        debug!("wrote cursor {} to {}", cursor, cursor_file);
    } else {
        warn!(
            "write_cursor: ignoring stale cursor {} (last={})",
            cursor, *last
        );
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
