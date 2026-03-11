use crate::pb::sf::solana::r#type::v1::{AccountBlock, Block};
use crate::state::{BlockInfo, CURSOR_MUTEX};
use log::{debug, info};
use rbase64;
use std::fs::File;
use std::io::Write;
use std::sync::mpsc::{channel, Sender};

// ---------------------------------------------------------------------------
// Internal message types
// ---------------------------------------------------------------------------

// Sent from the encode thread to the writer thread.
enum WriteMsg {
    // Complete pre-formatted line (used for FIRE INIT — small, no payload).
    Line(Vec<u8>),
    // Split write: three separate write_all calls to avoid a large allocation.
    // prefix: "FIRE BLOCK <slot> <hash> ... " (~200 B)
    // payload: base64 bytes moved zero-copy from the rbase64 String (~107 MB)
    // writer appends b"\n" itself.
    Split {
        prefix:  Vec<u8>,
        payload: Vec<u8>,
        cursor:  Option<(String, u64)>,
    },
}

// Sent from print() to the encode thread.
enum PipelineMsg<T> {
    // A pre-formatted line that bypasses encoding (used for FIRE INIT).
    Init(Vec<u8>),
    // A proto object to encode + base64, then forward to the writer thread.
    Data {
        proto:          T,
        slot:           u64,
        block_hash:     String,
        parent_slot:    u64,
        parent_hash:    String,
        lib:            u64,
        timestamp_nano: i64,
        cursor_path:    String,
    },
}

// ---------------------------------------------------------------------------
// Pipeline: encode thread + writer thread, one pair per output file
//
// Memory lifecycle per block:
//   1. T (Block / AccountBlock) lives in encode_rx queue
//   2. Encode thread: encode_to_vec() → drop(T) → rbase64 → drop(proto bytes)
//                     → format small prefix → b64.into_bytes() (zero-copy)
//                     → send Split { prefix (~200B), payload (~107MB) } to write_rx
//   3. Only the base64 payload Vec<u8> remains queued (no extra copy vs. the old single-line format!)
//   4. Writer thread: write_all(prefix) + write_all(payload) + write_all(b"\n")
//      (may block here; Split messages accumulate in write_rx)
// ---------------------------------------------------------------------------

fn spawn_pipeline<T>(mut file: File) -> Sender<PipelineMsg<T>>
where
    T: prost::Message + Send + 'static,
{
    let (encode_out, encode_rx) = channel::<PipelineMsg<T>>();
    let (write_out, write_rx)   = channel::<WriteMsg>();

    // Writer thread: owns the file, may block on slow FIFO reads.
    std::thread::spawn(move || {
        for msg in write_rx {
            let cursor = match msg {
                WriteMsg::Line(line) => {
                    file.write_all(&line).expect("cannot write to fifo");
                    None
                }
                WriteMsg::Split { prefix, payload, cursor } => {
                    file.write_all(&prefix) .expect("cannot write prefix to fifo");
                    file.write_all(&payload).expect("cannot write payload to fifo");
                    file.write_all(b"\n")   .expect("cannot write newline to fifo");
                    cursor
                }
            };
            if let Some((cursor_path, slot)) = cursor {
                write_cursor(&cursor_path, slot);
            }
        }
    });

    // Encode thread: receives T, encodes, drops intermediates ASAP, forwards line.
    std::thread::spawn(move || {
        for msg in encode_rx {
            match msg {
                PipelineMsg::Init(line) => {
                    write_out.send(WriteMsg::Line(line))
                        .expect("write channel disconnected on init");
                }
                PipelineMsg::Data { proto, slot, block_hash, parent_slot, parent_hash, lib, timestamp_nano, cursor_path } => {
                    // Stage 1: proto struct → raw bytes, drop the struct immediately.
                    let raw = proto.encode_to_vec();
                    drop(proto);

                    // Stage 2: raw bytes → base64 string, drop raw bytes immediately.
                    let b64 = rbase64::encode(&raw);
                    drop(raw);

                    // Stage 3: build the small header prefix (~200 B), then move b64
                    // into the payload via into_bytes() — zero-copy, no giant allocation.
                    let prefix  = format!("FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} ")
                        .into_bytes();
                    let payload = b64.into_bytes();

                    write_out.send(WriteMsg::Split { prefix, payload, cursor: Some((cursor_path, slot)) })
                        .expect("write channel disconnected");
                }
            }
        }
    });

    encode_out
}

// ---------------------------------------------------------------------------
// BlockPrinter
// ---------------------------------------------------------------------------

pub struct BlockPrinter {
    noop:        bool,
    block_out:   Option<Sender<PipelineMsg<Block>>>,
    account_out: Option<Sender<PipelineMsg<AccountBlock>>>,
}

impl BlockPrinter {
    pub fn new(out_block: Option<File>, out_account: Option<File>, noop: bool) -> Self {
        BlockPrinter {
            noop,
            block_out:   out_block.map(spawn_pipeline),
            account_out: out_account.map(spawn_pipeline),
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
            return Ok(());
        }
        if let Some(ref out) = self.block_out {
            let line = format!("FIRE INIT 3.0 {block_type}\n").into_bytes();
            out.send(PipelineMsg::Init(line))
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "block pipeline gone"))?;
        }
        if let Some(ref out) = self.account_out {
            let line = format!("FIRE INIT 3.0 {account_block_type}\n").into_bytes();
            out.send(PipelineMsg::Init(line))
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "account pipeline gone"))?;
        }
        Ok(())
    }

    pub fn print(
        &mut self,
        block_info: &BlockInfo,
        lib: u64,
        block: Block,
        account_block: AccountBlock,
        cursor_path: &str,
    ) -> std::io::Result<()> {
        let slot           = block_info.slot;
        let parent_slot    = block_info.parent_slot;
        let timestamp_nano = block_info.timestamp.seconds * 1_000_000_000;

        if let Some(ref out) = self.block_out {
            if self.noop {
                info!("printing block {} (noop mode)", slot);
                write_cursor(cursor_path, slot);
            } else {
                out.send(PipelineMsg::Data {
                    proto:          block,
                    slot,
                    block_hash:     block_info.block_hash.clone(),
                    parent_slot,
                    parent_hash:    block_info.parent_hash.clone(),
                    lib,
                    timestamp_nano,
                    cursor_path:    cursor_path.to_string(),
                }).expect("block encode channel disconnected");
            }
        } else {
            write_cursor(cursor_path, slot); // must still be called twice
        }

        if let Some(ref out) = self.account_out {
            if self.noop {
                info!("printing account_block {} (noop mode)", slot);
                write_cursor(cursor_path, slot);
            } else {
                out.send(PipelineMsg::Data {
                    proto:          account_block,
                    slot,
                    block_hash:     block_info.block_hash.clone(),
                    parent_slot,
                    parent_hash:    block_info.parent_hash.clone(),
                    lib,
                    timestamp_nano,
                    cursor_path:    cursor_path.to_string(),
                }).expect("account encode channel disconnected");
            }
        } else {
            write_cursor(cursor_path, slot); // must still be called twice
        }

        Ok(())
    }
}

// write_cursor writes the cursor the second time it is called with the same value.
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
