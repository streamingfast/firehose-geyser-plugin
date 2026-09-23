//! Throughput of the plugin under a mainnet-like load, driven through its Geyser callbacks.
//! Ignored by default; run with:
//!
//! cargo test --release --test throughput_test -- --ignored --nocapture

use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaAccountInfoV3, ReplicaAccountInfoVersions, ReplicaBlockInfoV4,
    ReplicaBlockInfoVersions, ReplicaTransactionInfoV3, ReplicaTransactionInfoVersions, SlotStatus,
};
use firehose_geyser_plugin::plugins::Plugin;
use solana_hash::Hash;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::{RewardsAndNumPartitions, TransactionStatusMeta};
use std::io::Write;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const STARTUP_ACCOUNTS: u64 = 20_000_000;
const OWNERS: u64 = 2_000;
const SLOTS: u64 = 300;
const UPDATE_THREADS: u64 = 8;
const UPDATES_PER_THREAD_PER_SLOT: u64 = 300;
const TRANSACTIONS_PER_SLOT: u64 = 1_000;

fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn pubkey(i: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    for word in 0..4 {
        out[word * 8..word * 8 + 8].copy_from_slice(&splitmix(i * 4 + word as u64).to_le_bytes());
    }
    out
}

/// Account data: mostly token-account sized, some larger, a few very large.
fn data_pool() -> Vec<Vec<u8>> {
    (0..256u64)
        .map(|i| {
            let len = match i % 32 {
                0 => 100_000,
                1..=4 => 10_000,
                5..=8 => 1_000,
                _ => 165,
            };
            (0..len)
                .map(|j| splitmix(i * 1_000_003 + j) as u8)
                .collect()
        })
        .collect()
}

fn rss_mb() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    String::from_utf8(out.stdout)
        .unwrap()
        .trim()
        .parse::<u64>()
        .unwrap()
        / 1024
}

/// User + system CPU time of the whole process, printer threads included.
fn cpu_seconds() -> f64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "time=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    // [[dd-]hh:]mm:ss.ss
    let text = String::from_utf8(out.stdout).unwrap();
    text.trim()
        .replace('-', ":")
        .split(':')
        .fold(0.0, |total, part| {
            total * 60.0 + part.parse::<f64>().unwrap()
        })
}

fn update_account(
    plugin: &Plugin,
    slot: u64,
    address: &[u8; 32],
    owner: &[u8; 32],
    data: &[u8],
    write_version: u64,
    is_startup: bool,
) {
    let info = ReplicaAccountInfoV3 {
        pubkey: address,
        lamports: 1,
        owner,
        executable: false,
        rent_epoch: 0,
        data,
        write_version,
        txn: None,
    };
    let account = ReplicaAccountInfoVersions::V0_0_3(&info);
    if is_startup {
        plugin.update_account_from_snapshot(account, slot).unwrap();
    } else {
        plugin.update_account_for_bank(account, slot, 0).unwrap();
    }
}

#[test]
#[ignore]
fn test_throughput() {
    let cursor_file = NamedTempFile::new().unwrap();
    let mut config_file = NamedTempFile::new().unwrap();
    write!(
        config_file,
        r#"{{
            "log": {{ "level": "warn" }},
            "local_rpc_client": {{ "endpoint": "http://127.0.0.1:1" }},
            "remote_rpc_client": {{ "endpoint": "http://127.0.0.1:1" }},
            "cursor_file": "{}",
            "block_destination_file": "/dev/null",
            "account_block_destination_file": "/dev/null",
            "send_processed": false,
            "noop": false
        }}"#,
        cursor_file.path().to_str().unwrap(),
    )
    .unwrap();
    config_file.flush().unwrap();

    let mut plugin = Plugin::new(false, false);
    plugin
        .on_load(config_file.path().to_str().unwrap(), false)
        .unwrap();

    let data = data_pool();
    let owners: Vec<[u8; 32]> = (0..OWNERS).map(|i| pubkey(u64::MAX / 2 + i)).collect();
    let rss_before = rss_mb();

    let started = Instant::now();
    for i in 0..STARTUP_ACCOUNTS {
        let r = splitmix(i);
        // Mostly small accounts at startup, like the snapshot
        let data = &data[(r % 256) as usize];
        let data = if data.len() > 1_000 {
            &data[..165]
        } else {
            data
        };
        update_account(
            &plugin,
            r % 1_000,
            &pubkey(i),
            &owners[(r >> 16) as usize % owners.len()],
            data,
            r >> 40,
            true,
        );
    }
    let startup_load = started.elapsed();
    let startup_rss = rss_mb() - rss_before;
    let started = Instant::now();
    plugin.notify_end_of_startup().unwrap();
    let end_of_startup = started.elapsed();
    let running_rss = rss_mb() - rss_before;

    let first_slot = 1_000u64;
    plugin
        .update_bank_status(first_slot - 40, None, &SlotStatus::Rooted, 0)
        .unwrap();

    let rewards = RewardsAndNumPartitions {
        rewards: vec![],
        num_partitions: None,
    };
    let signature = Signature::default();
    let message_hash = Hash::default();
    let transaction = VersionedTransaction::default();
    let meta = TransactionStatusMeta::default();

    let mut update_time = Duration::ZERO;
    let mut block_time = Duration::ZERO;
    let cpu_before = cpu_seconds();
    let started = Instant::now();
    for slot in first_slot..first_slot + SLOTS {
        let updates_started = Instant::now();
        std::thread::scope(|scope| {
            for thread in 0..UPDATE_THREADS {
                let plugin = &plugin;
                let data = &data;
                let owners = &owners;
                scope.spawn(move || {
                    for n in 0..UPDATES_PER_THREAD_PER_SLOT {
                        let i = (slot * UPDATE_THREADS + thread) * UPDATES_PER_THREAD_PER_SLOT + n;
                        let r = splitmix(i ^ 0xABCD);
                        // Mostly existing accounts, some new ones
                        let account = if r % 10 == 0 {
                            STARTUP_ACCOUNTS + i
                        } else {
                            r % STARTUP_ACCOUNTS
                        };
                        let owner = &owners[(splitmix(account) >> 16) as usize % owners.len()];
                        update_account(
                            plugin,
                            slot,
                            &pubkey(account),
                            owner,
                            &data[(r >> 8) as usize % data.len()],
                            i,
                            false,
                        );
                    }
                });
            }
        });
        update_time += updates_started.elapsed();

        let block_started = Instant::now();
        for index in 0..TRANSACTIONS_PER_SLOT {
            let info = ReplicaTransactionInfoV3 {
                signature: &signature,
                message_hash: &message_hash,
                is_vote: false,
                transaction: &transaction,
                transaction_status_meta: &meta,
                index: index as usize,
            };
            plugin
                .notify_transaction_for_bank(ReplicaTransactionInfoVersions::V0_0_3(&info), slot, 0)
                .unwrap();
        }
        let blockhash = format!("hash{}", slot);
        let parent_blockhash = format!("hash{}", slot - 1);
        let block_info = ReplicaBlockInfoV4 {
            parent_slot: slot - 1,
            parent_blockhash: &parent_blockhash,
            slot,
            blockhash: &blockhash,
            rewards: &rewards,
            block_time: Some(1_700_000_000 + slot as i64),
            block_height: Some(slot),
            executed_transaction_count: TRANSACTIONS_PER_SLOT,
            entry_count: 1,
        };
        plugin
            .notify_block_metadata_for_bank(ReplicaBlockInfoVersions::V0_0_4(&block_info), 0)
            .unwrap();
        plugin
            .update_bank_status(slot - 32, None, &SlotStatus::Rooted, 0)
            .unwrap();
        plugin
            .update_bank_status(slot, Some(slot - 1), &SlotStatus::Confirmed, 0)
            .unwrap();
        block_time += block_started.elapsed();
    }
    let live = started.elapsed();
    // Let the printer threads of the last blocks finish
    std::thread::sleep(Duration::from_secs(1));
    let live_cpu = cpu_seconds() - cpu_before;
    let updates = SLOTS * UPDATE_THREADS * UPDATES_PER_THREAD_PER_SLOT;

    println!(
        "THROUGHPUT startup_load={:.2?} ({:.0} ns/account) end_of_startup={:.2?} startup_rss_mb={} running_rss_mb={}",
        startup_load,
        startup_load.as_nanos() as f64 / STARTUP_ACCOUNTS as f64,
        end_of_startup,
        startup_rss,
        running_rss,
    );
    println!(
        "THROUGHPUT live={:.2?} cpu={:.2}s updates={:.2?} ({:.0} ns/update) blocks={:.2?} ({:.2?}/slot) rss_mb={}",
        live,
        live_cpu,
        update_time,
        update_time.as_nanos() as f64 / updates as f64,
        block_time,
        block_time / SLOTS as u32,
        rss_mb() - rss_before,
    );
}
