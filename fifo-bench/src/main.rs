//! fifo-bench — benchmark writing large AccountBlock protos to a FIFO
//!
//! Usage:
//!   fifo-bench [FIFO_PATH] [OPTIONS]
//!
//! Start a consumer first, e.g.:
//!   mkfifo /tmp/firehose.bench.fifo
//!   dd if=/tmp/firehose.bench.fifo of=/dev/null bs=4M &

use std::env;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::process::Command;
use std::time::{Duration, Instant};

use prost::Message;

// ---------------------------------------------------------------------------
// Inline proto types — must match field tags in sf.solana.type.v1
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, prost::Message)]
struct Account {
    #[prost(bytes = "vec", tag = "1")]
    address: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    owner: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    data: Vec<u8>,
    #[prost(bool, tag = "7")]
    deleted: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
struct AccountBlock {
    #[prost(uint64, tag = "1")]
    slot: u64,
    #[prost(string, tag = "2")]
    hash: String,
    #[prost(uint64, tag = "3")]
    parent_slot: u64,
    #[prost(string, tag = "4")]
    parent_hash: String,
    #[prost(message, optional, tag = "6")]
    timestamp: Option<prost_types::Timestamp>,
    #[prost(message, repeated, tag = "7")]
    accounts: Vec<Account>,
}

// ---------------------------------------------------------------------------
// Block construction
// ---------------------------------------------------------------------------

/// Build a template AccountBlock whose accounts are fixed (only slot/hash
/// change per iteration). Accounts are pre-built and cloned each iteration.
fn build_template(target_proto_bytes: usize) -> AccountBlock {
    // Each Account encodes to roughly: 2 tag/len pairs for address+owner (32B each)
    // + 1 tag/len pair for data + data_size bytes.  Overhead ~70 B per account.
    let data_size: usize = 9_500;
    let bytes_per_account = data_size + 70;
    let num_accounts = (target_proto_bytes / bytes_per_account).max(1);

    eprintln!(
        "Building template: {} accounts × ~{} B ≈ {:.1} MB proto",
        num_accounts,
        bytes_per_account,
        num_accounts as f64 * bytes_per_account as f64 / 1024.0 / 1024.0,
    );

    let accounts: Vec<Account> = (0u64..num_accounts as u64)
        .map(|i| {
            let mut addr = vec![0u8; 32];
            addr[..8].copy_from_slice(&i.to_le_bytes());
            Account {
                address: addr,
                owner: vec![0xABu8; 32],
                data: vec![(i & 0xFF) as u8; data_size],
                deleted: false,
            }
        })
        .collect();

    AccountBlock {
        slot: 0,
        hash: String::new(),
        parent_slot: 0,
        parent_hash: "11111111111111111111111111111111".to_string(),
        timestamp: Some(prost_types::Timestamp { seconds: 1_700_000_000, nanos: 0 }),
        accounts,
    }
}

/// Cheap deterministic hash string from a slot number (64 hex chars).
#[inline]
fn slot_to_hash(slot: u64) -> String {
    let a = slot.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(0x6c62272e07bb0142);
    let b = a.wrapping_mul(0x517cc1b727220a95);
    let c = a.wrapping_add(0xdeadbeefcafebabe);
    let d = a.rotate_left(32).wrapping_mul(0xbf58476d1ce4e5b9);
    format!("{a:016x}{b:016x}{c:016x}{d:016x}")
}

// ---------------------------------------------------------------------------
// Duration parsing
// ---------------------------------------------------------------------------

fn parse_duration(s: &str) -> Option<Duration> {
    if let Some(rest) = s.strip_suffix('s') {
        rest.parse::<u64>().ok().map(Duration::from_secs)
    } else if let Some(rest) = s.strip_suffix('m') {
        rest.parse::<u64>().ok().map(|m| Duration::from_secs(m * 60))
    } else if let Some(rest) = s.strip_suffix('h') {
        rest.parse::<u64>().ok().map(|h| Duration::from_secs(h * 3600))
    } else {
        // bare number treated as seconds
        s.parse::<u64>().ok().map(Duration::from_secs)
    }
}

// ---------------------------------------------------------------------------
// Platform pipe-size helper
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn set_pipe_size(fd: std::os::unix::io::RawFd, size: i32) {
    let actual = unsafe { libc::fcntl(fd, libc::F_SETPIPE_SZ, size) };
    if actual < 0 {
        eprintln!("Warning: F_SETPIPE_SZ failed (errno {})", unsafe { *libc::__errno_location() });
    } else {
        eprintln!("Pipe buffer set to {} bytes ({} KB)", actual, actual / 1024);
    }
}

#[cfg(not(target_os = "linux"))]
fn set_pipe_size(_fd: i32, _size: i32) {
    eprintln!("Warning: --pipe-size is a no-op on non-Linux platforms");
}

// ---------------------------------------------------------------------------
// Write modes
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Debug)]
enum WriteMode {
    /// Format entire FIRE BLOCK line into one Vec, one write() call.
    /// Mirrors the current production writeln!() approach.
    Single,
    /// Three separate write() calls: prefix bytes, payload bytes, newline.
    /// Avoids a large allocation by writing the already-encoded payload in-place.
    Split,
}

impl WriteMode {
    fn name(self) -> &'static str {
        match self {
            WriteMode::Single => "single",
            WriteMode::Split  => "split",
        }
    }
}

#[inline(always)]
fn write_line(writer: &mut dyn Write, mode: WriteMode, prefix: &[u8], payload: &[u8]) {
    match mode {
        WriteMode::Single => {
            let mut line = Vec::with_capacity(prefix.len() + payload.len() + 1);
            line.extend_from_slice(prefix);
            line.extend_from_slice(payload);
            line.push(b'\n');
            writer.write_all(&line).expect("write failed");
        }
        WriteMode::Split => {
            writer.write_all(prefix).expect("write prefix failed");
            writer.write_all(payload).expect("write payload failed");
            writer.write_all(b"\n").expect("write newline failed");
        }
    }
}

// ---------------------------------------------------------------------------
// Benchmark loop
// ---------------------------------------------------------------------------

struct Stats {
    blocks:        u64,
    proto_bytes:   u64,
    fifo_bytes:    u64,
    t_encode_vec:  Duration,
    t_base64:      Duration,
    t_write:       Duration,
    t_total:       Duration,
}

fn run_bench(
    writer:       &mut dyn Write,
    mode:         WriteMode,
    template:     &AccountBlock,
    duration:     Duration,
) -> Stats {
    // Fixed parts of the FIRE BLOCK line (lib=0, timestamp=constant)
    // Slot/hash change every iteration so the prefix is rebuilt each time.
    // The invariant suffix parts (parent_slot, parent_hash, lib, timestamp_nano)
    // are constant and concatenated into the prefix for speed.
    let parent_slot      = template.parent_slot;
    let parent_hash      = &template.parent_hash;
    let lib: u64         = 0;
    let timestamp_nano: i64 = 1_700_000_000_000_000_000;

    let mut s = Stats {
        blocks:       0,
        proto_bytes:  0,
        fifo_bytes:   0,
        t_encode_vec: Duration::ZERO,
        t_base64:     Duration::ZERO,
        t_write:      Duration::ZERO,
        t_total:      Duration::ZERO,
    };

    let deadline = Instant::now() + duration;

    let mut slot: u64 = 1;
    loop {
        if Instant::now() >= deadline {
            break;
        }

        let t0 = Instant::now();

        // Update mutable fields on the template by constructing a thin wrapper
        // (accounts Vec is reused by reference via clone-on-write semantics;
        // since AccountBlock derives Clone we clone the whole thing but the
        // account data bytes are heap-allocated and will be copied — acceptable
        // for a realistic benchmark that must call encode_to_vec() anyway).
        let block_hash   = slot_to_hash(slot);
        let prev_hash    = slot_to_hash(slot.saturating_sub(1));

        // Encode to proto bytes — this is the main CPU work
        let t1 = Instant::now();
        let proto = {
            // Mutate slot/hash in a local copy that shares the accounts Vec allocation
            let mut b = template.clone();
            b.slot        = slot;
            b.hash        = block_hash.clone();
            b.parent_slot = slot.saturating_sub(1);
            b.parent_hash = prev_hash.clone();
            b.encode_to_vec()
        };
        let t2 = Instant::now();

        // Base64 encode
        let b64 = rbase64::encode(&proto);
        let t3 = Instant::now();

        // Build the "FIRE BLOCK <slot> <hash> <parent_slot> <parent_hash> <lib> <ts> "
        // prefix (small allocation, dominated by payload size)
        let prefix = format!(
            "FIRE BLOCK {slot} {block_hash} {parent_slot} {parent_hash} {lib} {timestamp_nano} "
        );
        let fifo_line_len = prefix.len() + b64.len() + 1; // +1 for \n

        // Write to FIFO
        write_line(writer, mode, prefix.as_bytes(), b64.as_bytes());
        let t4 = Instant::now();

        s.blocks      += 1;
        s.proto_bytes += proto.len() as u64;
        s.fifo_bytes  += fifo_line_len as u64;
        s.t_encode_vec += t2 - t1;
        s.t_base64     += t3 - t2;
        s.t_write      += t4 - t3;
        s.t_total      += t4 - t0;

        slot += 1;
    }

    s
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn print_report(s: &Stats, mode: WriteMode, pipe_size: Option<i32>, target_mb: usize) {
    let elapsed  = s.t_total.as_secs_f64();
    let blk_f    = s.blocks as f64;
    let proto_mb = s.proto_bytes as f64 / 1024.0 / 1024.0;
    let fifo_mb  = s.fifo_bytes  as f64 / 1024.0 / 1024.0;

    let ms = |d: Duration| -> f64 { d.as_secs_f64() * 1000.0 / blk_f.max(1.0) };

    println!();
    println!("=== fifo-bench results ===");
    println!("  write-mode  : {}", mode.name());
    println!("  pipe-size   : {}", pipe_size.map(|v| format!("{v} bytes")).unwrap_or_else(|| "default (not set)".to_string()));
    println!("  target size : {} MB proto / block", target_mb);
    println!();
    println!("  blocks      : {}", s.blocks);
    println!("  duration    : {:.3} s", elapsed);
    println!();
    println!("  blocks/sec  : {:.3}", blk_f / elapsed);
    println!("  proto MB/s  : {:.1}  ({:.1} MB/block)", proto_mb / elapsed, proto_mb / blk_f.max(1.0));
    println!("  fifo  MB/s  : {:.1}  ({:.1} MB/block)", fifo_mb  / elapsed, fifo_mb  / blk_f.max(1.0));
    println!();
    println!("  time per block (avg):");
    println!("    encode_to_vec : {:8.1} ms", ms(s.t_encode_vec));
    println!("    base64 encode : {:8.1} ms", ms(s.t_base64));
    println!("    fifo write    : {:8.1} ms", ms(s.t_write));
    println!("    total         : {:8.1} ms", ms(s.t_total));
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("Usage: {} [FIFO_PATH] [OPTIONS]", args[0]);
        println!();
        println!("Options:");
        println!("  --duration <val>     run time: 30s, 5m, 1h, etc.  (default: 30s)");
        println!("  --size <MB>          target AccountBlock proto size (default: 80)");
        println!("  --write-mode <mode>  single|split                  (default: single)");
        println!("                         single  one Vec allocation + one write() per block");
        println!("                         split   prefix/payload/newline as separate write()s");
        println!("  --pipe-size <bytes>  call fcntl(F_SETPIPE_SZ, bytes)  (Linux only)");
        println!("                       not called by default");
        println!();
        println!("Consumer example:");
        println!("  mkfifo /tmp/firehose.bench.fifo");
        println!("  dd if=/tmp/firehose.bench.fifo of=/dev/null bs=4M &");
        println!("  {} /tmp/firehose.bench.fifo --duration 30s --write-mode split", args[0]);
        return;
    }

    // --- parse args ---------------------------------------------------------

    let fifo_path = if args.get(1).map(|s| !s.starts_with('-')).unwrap_or(false) {
        args[1].clone()
    } else {
        "/tmp/firehose.bench.fifo".to_string()
    };

    let mut duration   = Duration::from_secs(30);
    let mut target_mb: usize = 80;
    let mut write_mode = WriteMode::Single;
    let mut pipe_size: Option<i32> = None;

    let mut i = if fifo_path == "/tmp/firehose.bench.fifo" { 1 } else { 2 };
    while i < args.len() {
        match args[i].as_str() {
            "--duration" => {
                i += 1;
                duration = args.get(i)
                    .and_then(|s| parse_duration(s))
                    .unwrap_or_else(|| { eprintln!("bad --duration value"); std::process::exit(1); });
            }
            "--size" => {
                i += 1;
                target_mb = args.get(i).and_then(|s| s.parse().ok()).unwrap_or_else(|| {
                    eprintln!("bad --size value"); std::process::exit(1);
                });
            }
            "--write-mode" => {
                i += 1;
                write_mode = match args.get(i).map(String::as_str) {
                    Some("single") => WriteMode::Single,
                    Some("split")  => WriteMode::Split,
                    other => { eprintln!("unknown --write-mode: {:?}", other); std::process::exit(1); }
                };
            }
            "--pipe-size" => {
                i += 1;
                pipe_size = Some(args.get(i).and_then(|s| s.parse().ok()).unwrap_or_else(|| {
                    eprintln!("bad --pipe-size value"); std::process::exit(1);
                }));
            }
            s if i == 1 && !s.starts_with('-') => {} // positional already handled
            other => { eprintln!("unknown arg: {}", other); std::process::exit(1); }
        }
        i += 1;
    }

    // --- build template block -----------------------------------------------

    let template = build_template(target_mb * 1024 * 1024);

    // Probe actual encoded size with slot=1
    {
        let mut b = template.clone();
        b.slot = 1;
        b.hash = slot_to_hash(1);
        let proto = b.encode_to_vec();
        let b64   = rbase64::encode(&proto);
        eprintln!(
            "Actual sizes: proto={:.1} MB, base64={:.1} MB (+{:.1}%)",
            proto.len() as f64 / 1024.0 / 1024.0,
            b64.len()   as f64 / 1024.0 / 1024.0,
            (b64.len() as f64 / proto.len() as f64 - 1.0) * 100.0,
        );
    }

    eprintln!("Config: write-mode={}, duration={:.0}s, pipe-size={:?}",
        write_mode.name(), duration.as_secs_f64(), pipe_size);

    // --- open FIFO ----------------------------------------------------------

    let path = std::path::Path::new(&fifo_path);
    if !path.exists() {
        eprintln!("Creating FIFO at {fifo_path}");
        let ok = Command::new("mkfifo").arg(&fifo_path).status()
            .expect("mkfifo failed").success();
        if !ok { eprintln!("mkfifo failed"); std::process::exit(1); }
    }

    eprintln!("Waiting for reader on {fifo_path}...");
    let file = OpenOptions::new().write(true).open(&fifo_path)
        .expect("cannot open FIFO for writing");

    if let Some(size) = pipe_size {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            set_pipe_size(file.as_raw_fd(), size);
        }
        #[cfg(not(target_os = "linux"))]
        set_pipe_size(0, size);
    }

    eprintln!("Connected. Running for {}s...", duration.as_secs());

    // --- run ----------------------------------------------------------------

    let stats = match write_mode {
        WriteMode::Single => {
            let mut w = file;
            run_bench(&mut w, write_mode, &template, duration)
        }
        WriteMode::Split => {
            // BufWriter coalesces the three small writes (prefix/newline)
            // into one syscall for the header; the large payload still goes
            // through directly once the buffer fills.
            let mut w = BufWriter::with_capacity(256 * 1024, file);
            run_bench(&mut w, write_mode, &template, duration)
        }
    };

    print_report(&stats, write_mode, pipe_size, target_mb);
}
