use crate::pb::sf::solana::r#type::v1::{AccountBlock, Block};
use crate::state::{BlockInfo, ACC_MUTEX, BLOCK_MUTEX, CURSOR_MUTEX};
use crate::stats::{PENDING_ACCOUNT_BLOCK_WRITES, PENDING_BLOCK_WRITES, PENDING_WRITE_BYTES};
use base64::Engine;
use log::{debug, error, info, warn};
use prost::Message;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;
use std::time::Instant;

/// One formatted FIFO line, produced by a per-slot encoding thread and consumed by its
/// output's writer thread once this job reaches the front of the queue.
struct EncodedLine {
    slot: u64,
    line: String,
    payload_len: usize,
    cursor_path: String,
}

/// A single long-lived thread per output (block or account block), draining jobs strictly
/// in the order `print` enqueued them. Encoding still happens on its own thread per slot, in
/// parallel, but the writer only picks up a job's line once it is that job's turn, so a
/// faster-encoding later slot can never reach the FIFO ahead of an earlier one.
struct OutputWriter {
    jobs_tx: Sender<Receiver<EncodedLine>>,
}

impl OutputWriter {
    fn spawn(
        mut file: File,
        mutex: &'static Mutex<()>,
        label: &'static str,
        pending: &'static AtomicUsize,
    ) -> Self {
        let (jobs_tx, jobs_rx) = mpsc::channel::<Receiver<EncodedLine>>();

        std::thread::spawn(move || {
            for job in jobs_rx {
                let encoded = match job.recv() {
                    Ok(encoded) => encoded,
                    Err(_) => {
                        error!("{} encoding thread dropped without a result", label);
                        panic!("{} encoding thread dropped without a result", label);
                    }
                };

                let _lock = match mutex.lock() {
                    Ok(lock) => lock,
                    Err(e) => {
                        error!(
                            "{}_mutex poisoned while writing slot {}: {}",
                            label, encoded.slot, e
                        );
                        panic!("{}_mutex lock poisoned while writing slot {}", label, encoded.slot);
                    }
                };
                if let Err(e) = writeln!(file, "{}", encoded.line) {
                    error!(
                        "cannot write {} {} to fifo ({}): {}",
                        label,
                        encoded.slot,
                        e.kind(),
                        e
                    );
                    panic!(
                        "cannot write to {} fifo for slot {}: {}",
                        label, encoded.slot, e
                    );
                }
                drop(_lock);

                PENDING_WRITE_BYTES.fetch_sub(encoded.payload_len, Ordering::Relaxed);
                pending.fetch_sub(1, Ordering::Relaxed);
                info!("block_printer: wrote {} {} to fifo", label, encoded.slot);
                write_cursor(&encoded.cursor_path, encoded.slot);
            }
        });

        OutputWriter { jobs_tx }
    }

    /// Reserves this job's place in the write order before encoding has even started.
    fn enqueue(&self, result_rx: Receiver<EncodedLine>) -> std::io::Result<()> {
        self.jobs_tx
            .send(result_rx)
            .map_err(|_| std::io::Error::other("fifo writer thread is no longer running"))
    }
}

pub struct BlockPrinter {
    noop: bool,
    out_block: Option<File>,
    out_account: Option<File>,
    block_writer: Option<OutputWriter>,
    account_writer: Option<OutputWriter>,
}

impl BlockPrinter {
    pub fn new(out_block: Option<File>, out_account: Option<File>, noop: bool) -> Self {
        let block_writer = Self::spawn_writer(
            noop,
            &out_block,
            &BLOCK_MUTEX,
            "block",
            &PENDING_BLOCK_WRITES,
        );
        let account_writer = Self::spawn_writer(
            noop,
            &out_account,
            &ACC_MUTEX,
            "account_block",
            &PENDING_ACCOUNT_BLOCK_WRITES,
        );

        BlockPrinter {
            noop,
            out_block,
            out_account,
            block_writer,
            account_writer,
        }
    }

    fn spawn_writer(
        noop: bool,
        out: &Option<File>,
        mutex: &'static Mutex<()>,
        label: &'static str,
        pending: &'static AtomicUsize,
    ) -> Option<OutputWriter> {
        if noop {
            return None;
        }
        let file = out.as_ref()?;
        match file.try_clone() {
            Ok(clone) => Some(OutputWriter::spawn(clone, mutex, label, pending)),
            Err(e) => {
                error!("cannot clone out_{} for writer thread: {}", label, e);
                None
            }
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

        if self.out_block.is_none() {
            write_cursor(cursor_path, slot); // must still be called twice
        } else if noop {
            info!("printing block {} (noop mode)", slot);
            write_cursor(cursor_path, slot);
        } else {
            let writer = self.block_writer.as_ref().ok_or_else(|| {
                std::io::Error::other(format!("out_block writer unavailable for slot {}", slot))
            })?;
            let block_hash = block_info.block_hash.clone();
            let parent_hash = block_info.parent_hash.clone();
            let cursor_path = cursor_path.to_string();

            let (result_tx, result_rx) = mpsc::channel();
            writer.enqueue(result_rx)?;
            PENDING_BLOCK_WRITES.fetch_add(1, Ordering::Relaxed);

            std::thread::spawn(move || {
                let started = Instant::now();
                info!(
                    "printing block {} {} with transaction count of {} (encode starting)",
                    block.slot,
                    block_hash,
                    block.transactions.len()
                );
                let encoded_block = block.encode_to_vec();
                let encoded_len = encoded_block.len();
                let payload = base64::engine::general_purpose::STANDARD.encode(&encoded_block);
                PENDING_WRITE_BYTES.fetch_add(payload.len(), Ordering::Relaxed);
                info!(
                    "block_printer: encoded block {} (protobuf_bytes={}, base64_bytes={}) in {:?}",
                    slot,
                    encoded_len,
                    payload.len(),
                    started.elapsed()
                );

                let line = format!(
                    "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}"
                );
                let _ = result_tx.send(EncodedLine {
                    slot,
                    line,
                    payload_len: payload.len(),
                    cursor_path,
                });
            });
        }

        if self.out_account.is_none() {
            write_cursor(cursor_path, slot); // must still be called twice
        } else if noop {
            info!("printing account_block {} (noop mode)", slot);
            write_cursor(cursor_path, slot);
        } else {
            let writer = self.account_writer.as_ref().ok_or_else(|| {
                std::io::Error::other(format!(
                    "out_account writer unavailable for slot {}",
                    slot
                ))
            })?;
            let block_hash = block_info.block_hash.clone();
            let parent_hash = block_info.parent_hash.clone();
            let cursor_path = cursor_path.to_string();

            let (result_tx, result_rx) = mpsc::channel();
            writer.enqueue(result_rx)?;
            PENDING_ACCOUNT_BLOCK_WRITES.fetch_add(1, Ordering::Relaxed);

            std::thread::spawn(move || {
                let started = Instant::now();
                info!(
                    "block_printer: encoding account_block {} (accounts={})",
                    slot, account_count
                );
                let encoded_account_block = account_block.encode_to_vec();
                let encoded_len = encoded_account_block.len();
                let payload =
                    base64::engine::general_purpose::STANDARD.encode(&encoded_account_block);
                PENDING_WRITE_BYTES.fetch_add(payload.len(), Ordering::Relaxed);
                info!(
                    "block_printer: encoded account_block {} (protobuf_bytes={}, base64_bytes={}) in {:?}",
                    slot,
                    encoded_len,
                    payload.len(),
                    started.elapsed()
                );

                let line = format!(
                    "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} {payload}"
                );
                let _ = result_tx.send(EncodedLine {
                    slot,
                    line,
                    payload_len: payload.len(),
                    cursor_path,
                });
            });
        }

        // We are not waiting for the threads to finish, so that the plugin can be called again for the updates.
        // Each output has one writer thread that drains jobs in the order `print` enqueued them, so encoding
        // stays parallel per slot while writes stay in call order. The write mutex only remains to let
        // process_upto detect a write failure (lock poisoned -> error) the way it already does.
        // The cursor only advances once both outputs have reported the same slot (see `write_cursor`).
        Ok(())
    }
}

// write_cursor is called once per output (block, account block) for a given slot. It persists
// the slot to the cursor file only once both outputs have reported it, so the cursor never
// advances past a slot that one of the two streams hasn't actually written yet.
fn write_cursor(cursor_file: &str, cursor: u64) {
    let mut state = match CURSOR_MUTEX.lock() {
        Ok(lock) => lock,
        Err(e) => {
            error!(
                "cursor_mutex poisoned while writing cursor {}: {}",
                cursor, e
            );
            panic!("cursor_mutex lock poisoned while writing cursor {}", cursor);
        }
    };

    let reports = state.pending.entry(cursor).or_insert(0);
    *reports += 1;
    if *reports < 2 {
        return;
    }
    state.pending.remove(&cursor);

    if cursor <= state.last {
        warn!(
            "write_cursor: ignoring stale cursor {} (last={})",
            cursor, state.last
        );
        return;
    }
    state.last = cursor;
    if let Err(e) = std::fs::write(cursor_file, cursor.to_string()) {
        error!("cannot write cursor {} to {}: {}", cursor, cursor_file, e);
        panic!("cannot write cursor {} to {}: {}", cursor, cursor_file, e);
    }
    debug!("wrote cursor {} to {}", cursor, cursor_file);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::sf::solana::r#type::v1::{Account, AccountBlock, Block};
    use crate::state::BlockInfo;
    use std::fs::OpenOptions;
    use std::time::{Duration, Instant};
    use tempfile::NamedTempFile;

    /// Reproduces the production bug: `print` is called for a parent slot with a large,
    /// slow-to-encode account block, immediately followed by a child slot with a tiny one.
    /// Before the fix, the child's writer thread finishes encoding first and reaches the
    /// FIFO before the parent's, flipping the order firehose-core relies on.
    #[test]
    fn test_account_blocks_are_written_in_call_order_even_when_child_encodes_faster() {
        let account_fifo = NamedTempFile::new().unwrap();
        let out_account = OpenOptions::new()
            .write(true)
            .open(account_fifo.path())
            .unwrap();

        let mut printer = BlockPrinter::new(None, Some(out_account), false);

        let cursor_file = NamedTempFile::new().unwrap();
        let cursor_path = cursor_file.path().to_str().unwrap().to_string();

        let parent_slot: u64 = 900_000_111_001;
        let child_slot: u64 = 900_000_111_002;

        let parent_info = BlockInfo {
            slot: parent_slot,
            parent_slot: parent_slot - 1,
            block_hash: "parent_hash".to_string(),
            parent_hash: "grandparent_hash".to_string(),
            ..Default::default()
        };
        let child_info = BlockInfo {
            slot: child_slot,
            parent_slot,
            block_hash: "child_hash".to_string(),
            parent_hash: "parent_hash".to_string(),
            ..Default::default()
        };

        // Large enough that encoding + base64 takes measurably longer than the tiny child.
        let slow_accounts = (0..20_000)
            .map(|i| Account {
                address: vec![i as u8; 32],
                owner: vec![0u8; 32],
                data: vec![0u8; 256],
                deleted: false,
            })
            .collect();
        let parent_account_block = AccountBlock {
            slot: parent_slot,
            accounts: slow_accounts,
            ..Default::default()
        };
        let child_account_block = AccountBlock {
            slot: child_slot,
            accounts: vec![Account {
                address: vec![1u8; 32],
                owner: vec![0u8; 32],
                data: vec![0u8; 4],
                deleted: false,
            }],
            ..Default::default()
        };

        printer
            .print(
                &parent_info,
                0,
                Block::default(),
                parent_account_block,
                &cursor_path,
            )
            .unwrap();
        printer
            .print(
                &child_info,
                0,
                Block::default(),
                child_account_block,
                &cursor_path,
            )
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while PENDING_ACCOUNT_BLOCK_WRITES.load(Ordering::Relaxed) != 0 {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for account block writes to finish"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        let content = std::fs::read_to_string(account_fifo.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "expected exactly two FIRE BLOCK lines");
        assert!(
            lines[0].starts_with(&format!("FIRE BLOCK {parent_slot} ")),
            "parent slot {} must be written before child slot {}, got:\n{}",
            parent_slot,
            child_slot,
            content
        );
        assert!(lines[1].starts_with(&format!("FIRE BLOCK {child_slot} ")));
    }

    #[test]
    fn test_write_cursor() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        // Use large, test-unique slot numbers: CURSOR_MUTEX is a process-wide global shared
        // with every other test that calls write_cursor, so small numbers risk colliding.
        let s1: u64 = 920_000_000_001;
        let s2: u64 = 920_000_000_002;
        let s3: u64 = 920_000_000_003;

        // Only the block output has reported slot s1: the account output hasn't, so nothing
        // persists yet.
        write_cursor(&path, s1);
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(content, "");

        // The account output reports s1 too: both outputs are done, cursor advances.
        write_cursor(&path, s1);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, s1.to_string());

        // The block output races ahead through s2 and s3 before the account output catches up.
        write_cursor(&path, s2);
        write_cursor(&path, s3);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, s1.to_string(), "s2 and s3 only have one report each so far");

        // The account output catches up on s2: cursor advances to s2, not straight to s3.
        write_cursor(&path, s2);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, s2.to_string());

        // The account output finishes s3: cursor advances to s3.
        write_cursor(&path, s3);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, s3.to_string());

        // A stale, already-passed slot reported twice (e.g. a late straggler) is ignored
        // rather than regressing the cursor file.
        write_cursor(&path, s2);
        write_cursor(&path, s2);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, s3.to_string());
    }
}
