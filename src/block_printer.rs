use crate::pb::sf::solana::r#type::v1::{AccountBlock, Block};
use crate::state::{BlockInfo, ACC_MUTEX, BLOCK_MUTEX};
use crate::stats::{PENDING_ACCOUNT_BLOCK_WRITES, PENDING_BLOCK_WRITES, PENDING_WRITE_BYTES};
use base64::Engine;
use log::{debug, error, info};
use prost::Message;
use std::any::Any;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;

#[derive(Clone, Copy)]
enum Output {
    Block,
    AccountBlock,
}

impl Output {
    fn label(self) -> &'static str {
        match self {
            Output::Block => "block",
            Output::AccountBlock => "account_block",
        }
    }

    fn mutex(self) -> &'static Mutex<()> {
        match self {
            Output::Block => &BLOCK_MUTEX,
            Output::AccountBlock => &ACC_MUTEX,
        }
    }

    fn pending(self) -> &'static AtomicUsize {
        match self {
            Output::Block => &PENDING_BLOCK_WRITES,
            Output::AccountBlock => &PENDING_ACCOUNT_BLOCK_WRITES,
        }
    }
}

/// The last slot each output has written. Each output writes in slot order, so every slot up
/// to the lower of the two is written to both, and `write_cursor` persists that lower value.
#[derive(Default)]
struct CursorState {
    block: u64,
    account: u64,
    persisted: u64,
}

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
    output: Output,
    jobs_tx: Sender<JoinHandle<EncodedLine>>,
}

impl OutputWriter {
    fn spawn(mut file: File, output: Output, cursor: Arc<Mutex<CursorState>>) -> Self {
        let label = output.label();
        let (jobs_tx, jobs_rx) = mpsc::channel::<JoinHandle<EncodedLine>>();

        std::thread::spawn(move || {
            for job in jobs_rx {
                // Held while waiting on the encoding thread too, so a panic there poisons the
                // mutex, which update_slot_status and process_upto check.
                let _lock = match output.mutex().lock() {
                    Ok(lock) => lock,
                    Err(e) => {
                        error!("{}_mutex poisoned: {}", label, e);
                        panic!("{}_mutex lock poisoned", label);
                    }
                };
                let encoded = match job.join() {
                    Ok(encoded) => encoded,
                    Err(cause) => {
                        let cause = panic_message(cause.as_ref());
                        error!("{} encoding thread panicked: {}", label, cause);
                        panic!("{} encoding thread panicked: {}", label, cause);
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
                output.pending().fetch_sub(1, Ordering::Relaxed);
                info!("block_printer: wrote {} {} to fifo", label, encoded.slot);
                write_cursor(&cursor, &encoded.cursor_path, output, encoded.slot);
            }
        });

        OutputWriter { output, jobs_tx }
    }

    /// Queues an encoding thread's result to be written after every job enqueued before it.
    fn enqueue(&self, job: JoinHandle<EncodedLine>) -> std::io::Result<()> {
        let pending = self.output.pending();
        pending.fetch_add(1, Ordering::Relaxed);
        self.jobs_tx.send(job).map_err(|_| {
            pending.fetch_sub(1, Ordering::Relaxed);
            std::io::Error::other("fifo writer thread is no longer running")
        })
    }
}

fn panic_message(cause: &(dyn Any + Send)) -> &str {
    if let Some(s) = cause.downcast_ref::<&str>() {
        s
    } else if let Some(s) = cause.downcast_ref::<String>() {
        s
    } else {
        "unknown panic"
    }
}

/// Encodes `message` as base64 directly after `header`, in a single allocation sized for the
/// full line. Returns the line, the protobuf size and the base64 payload size.
fn encode_line(header: String, message: impl Message) -> (String, usize, usize) {
    let encoded = message.encode_to_vec();
    drop(message);

    let header_len = header.len();
    let mut line = header;
    line.reserve_exact(base64::encoded_len(encoded.len(), true).unwrap_or(0));
    base64::engine::general_purpose::STANDARD.encode_string(&encoded, &mut line);
    let payload_len = line.len() - header_len;
    (line, encoded.len(), payload_len)
}

pub struct BlockPrinter {
    noop: bool,
    out_block: Option<File>,
    out_account: Option<File>,
    block_writer: Option<OutputWriter>,
    account_writer: Option<OutputWriter>,
    cursor: Arc<Mutex<CursorState>>,
}

impl BlockPrinter {
    pub fn new(out_block: Option<File>, out_account: Option<File>, noop: bool) -> Self {
        let cursor = Arc::new(Mutex::new(CursorState::default()));
        let block_writer = Self::spawn_writer(noop, &out_block, Output::Block, &cursor);
        let account_writer = Self::spawn_writer(noop, &out_account, Output::AccountBlock, &cursor);

        BlockPrinter {
            noop,
            out_block,
            out_account,
            block_writer,
            account_writer,
            cursor,
        }
    }

    fn spawn_writer(
        noop: bool,
        out: &Option<File>,
        output: Output,
        cursor: &Arc<Mutex<CursorState>>,
    ) -> Option<OutputWriter> {
        if noop {
            return None;
        }
        let file = out.as_ref()?;
        match file.try_clone() {
            Ok(clone) => Some(OutputWriter::spawn(clone, output, cursor.clone())),
            Err(e) => {
                error!(
                    "cannot clone out_{} for writer thread: {}",
                    output.label(),
                    e
                );
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
        let header = format!(
            "FIRE BLOCK {slot} {} {parent_slot} {} {lib} {timestamp_nano} ",
            block_info.block_hash, block_info.parent_hash
        );

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
            write_cursor(&self.cursor, cursor_path, Output::Block, slot);
        } else if noop {
            info!("printing block {} (noop mode)", slot);
            write_cursor(&self.cursor, cursor_path, Output::Block, slot);
        } else {
            let writer = self.block_writer.as_ref().ok_or_else(|| {
                std::io::Error::other(format!("out_block writer unavailable for slot {}", slot))
            })?;
            let block_hash = block_info.block_hash.clone();
            let header = header.clone();
            let cursor_path = cursor_path.to_string();

            writer.enqueue(std::thread::spawn(move || {
                let started = Instant::now();
                info!(
                    "printing block {} {} with transaction count of {} (encode starting)",
                    slot, block_hash, tx_count
                );
                let (line, encoded_len, payload_len) = encode_line(header, block);
                PENDING_WRITE_BYTES.fetch_add(payload_len, Ordering::Relaxed);
                info!(
                    "block_printer: encoded block {} (protobuf_bytes={}, base64_bytes={}) in {:?}",
                    slot,
                    encoded_len,
                    payload_len,
                    started.elapsed()
                );
                EncodedLine {
                    slot,
                    line,
                    payload_len,
                    cursor_path,
                }
            }))?;
        }

        if self.out_account.is_none() {
            write_cursor(&self.cursor, cursor_path, Output::AccountBlock, slot);
        } else if noop {
            info!("printing account_block {} (noop mode)", slot);
            write_cursor(&self.cursor, cursor_path, Output::AccountBlock, slot);
        } else {
            let writer = self.account_writer.as_ref().ok_or_else(|| {
                std::io::Error::other(format!("out_account writer unavailable for slot {}", slot))
            })?;
            let cursor_path = cursor_path.to_string();

            writer.enqueue(std::thread::spawn(move || {
                let started = Instant::now();
                info!(
                    "block_printer: encoding account_block {} (accounts={})",
                    slot, account_count
                );
                let (line, encoded_len, payload_len) = encode_line(header, account_block);
                PENDING_WRITE_BYTES.fetch_add(payload_len, Ordering::Relaxed);
                info!(
                    "block_printer: encoded account_block {} (protobuf_bytes={}, base64_bytes={}) in {:?}",
                    slot,
                    encoded_len,
                    payload_len,
                    started.elapsed()
                );
                EncodedLine {
                    slot,
                    line,
                    payload_len,
                    cursor_path,
                }
            }))?;
        }

        // We are not waiting for the threads to finish, so that the plugin can be called again for the updates.
        // Each output has one writer thread that drains jobs in the order `print` enqueued them, so encoding
        // stays parallel per slot while writes stay in call order. A failed encode or write poisons the
        // output's mutex, which process_upto checks.
        Ok(())
    }
}

/// Records that `output` has written `slot`, then persists the highest slot both outputs have
/// written, so the cursor never advances past a slot one of the two streams hasn't written yet.
fn write_cursor(state: &Mutex<CursorState>, cursor_file: &str, output: Output, slot: u64) {
    let mut state = match state.lock() {
        Ok(lock) => lock,
        Err(e) => {
            error!("cursor_mutex poisoned while writing cursor {}: {}", slot, e);
            panic!("cursor_mutex lock poisoned while writing cursor {}", slot);
        }
    };

    match output {
        Output::Block => state.block = state.block.max(slot),
        Output::AccountBlock => state.account = state.account.max(slot),
    }
    let cursor = state.block.min(state.account);
    if cursor <= state.persisted {
        return;
    }
    state.persisted = cursor;
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

    /// `print` is called for a parent slot with a large, slow-to-encode account block,
    /// immediately followed by a child slot with a tiny one. The child finishes encoding
    /// first, but the parent must still reach the FIFO first: firehose-core relies on that order.
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
        let state = Mutex::new(CursorState::default());
        let read = || std::fs::read_to_string(&path).unwrap_or_default();

        // Only the block output has written slot 1: nothing persists yet.
        write_cursor(&state, &path, Output::Block, 1);
        assert_eq!(read(), "");

        // The account output writes slot 1 too: cursor advances.
        write_cursor(&state, &path, Output::AccountBlock, 1);
        assert_eq!(read(), "1");

        // The block output races ahead through 2 and 3 before the account output catches up.
        write_cursor(&state, &path, Output::Block, 2);
        write_cursor(&state, &path, Output::Block, 3);
        assert_eq!(read(), "1");

        // The account output catches up on 2: cursor advances to 2, not straight to 3.
        write_cursor(&state, &path, Output::AccountBlock, 2);
        assert_eq!(read(), "2");

        // The account output finishes 3: cursor advances to 3.
        write_cursor(&state, &path, Output::AccountBlock, 3);
        assert_eq!(read(), "3");

        // A late report for an older slot does not move the cursor back.
        write_cursor(&state, &path, Output::Block, 2);
        write_cursor(&state, &path, Output::AccountBlock, 2);
        assert_eq!(read(), "3");
    }

    #[test]
    fn test_write_cursor_waits_for_both_outputs_when_one_reports_a_slot_twice() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        let state = Mutex::new(CursorState::default());

        write_cursor(&state, &path, Output::Block, 5);
        write_cursor(&state, &path, Output::Block, 5);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "");

        write_cursor(&state, &path, Output::AccountBlock, 5);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "5");
    }

    #[test]
    fn test_encode_line_matches_header_plus_base64() {
        let block = Block {
            slot: 42,
            ..Default::default()
        };
        let expected_payload =
            base64::engine::general_purpose::STANDARD.encode(block.encode_to_vec());

        let (line, encoded_len, payload_len) =
            encode_line("FIRE BLOCK 42 h 41 p 40 0 ".to_string(), block.clone());

        assert_eq!(
            line,
            format!("FIRE BLOCK 42 h 41 p 40 0 {expected_payload}")
        );
        assert_eq!(encoded_len, block.encode_to_vec().len());
        assert_eq!(payload_len, expected_payload.len());
    }
}
