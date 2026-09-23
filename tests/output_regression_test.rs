//! Drives the plugin through its Geyser callbacks with a seeded random workload and checks a
//! digest of both output streams, so that changes to the plugin internals cannot silently
//! change what gets written.

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

const VOTE_PROGRAM: [u8; 32] = [
    0x07, 0x61, 0x48, 0x1d, 0x35, 0x74, 0x74, 0xbb, 0x7c, 0x4d, 0x76, 0x24, 0xeb, 0xd3, 0xbd, 0xb3,
    0xd8, 0x35, 0x5e, 0x73, 0xd1, 0x10, 0x43, 0xfc, 0x0d, 0xa3, 0x53, 0x80, 0x00, 0x00, 0x00, 0x00,
];

const FIRST_SLOT: u64 = 1_000;
const LAST_SLOT: u64 = 1_400;
const ADDRESSES: u64 = 3_000;
const OWNERS: u64 = 6;

/// Expected `fnv1a64` digest of the sorted lines of the block and account block streams.
const EXPECTED_BLOCKS_DIGEST: u64 = 0x7139eefc95fd8769;
const EXPECTED_ACCOUNT_BLOCKS_DIGEST: u64 = 0xbc16c256d3e4caf9;

struct Rng(u64);

impl Rng {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next(100) < percent
    }
}

fn address(i: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[..8].copy_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
    out[8..16].copy_from_slice(&i.to_le_bytes());
    out[31] = 0xAA;
    out
}

fn owner(rng: &mut Rng) -> [u8; 32] {
    if rng.chance(3) {
        VOTE_PROGRAM
    } else {
        [1 + rng.next(OWNERS) as u8; 32]
    }
}

/// Mostly a few shared values, so that unchanged data is common, plus some larger payloads.
fn data(rng: &mut Rng) -> Vec<u8> {
    match rng.next(10) {
        0 => vec![],
        1 => (0..(1 + rng.next(4_000))).map(|i| (i * 7) as u8).collect(),
        n => vec![n as u8; 16],
    }
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// Waits for the printer threads to stop writing, then returns the digest of the sorted lines.
fn sorted_lines_digest(path: &str) -> (usize, u64) {
    let mut last_len = u64::MAX;
    let mut stable_since = Instant::now();
    loop {
        let len = std::fs::metadata(path).unwrap().len();
        if len != last_len {
            last_len = len;
            stable_since = Instant::now();
        } else if stable_since.elapsed() > Duration::from_millis(500) {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let content = std::fs::read_to_string(path).unwrap();
    let mut lines: Vec<&str> = content.lines().collect();
    lines.sort();
    (lines.len(), fnv1a64(lines.join("\n").as_bytes()))
}

fn update_account(
    plugin: &Plugin,
    slot: u64,
    address: &[u8; 32],
    owner: &[u8; 32],
    data: &[u8],
    lamports: u64,
    write_version: u64,
    is_startup: bool,
) {
    let info = ReplicaAccountInfoV3 {
        pubkey: address,
        lamports,
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
fn test_output_matches_recorded_digest() {
    let cursor_file = NamedTempFile::new().unwrap();
    let block_file = NamedTempFile::new().unwrap();
    let account_block_file = NamedTempFile::new().unwrap();
    let block_path = block_file.path().to_str().unwrap().to_string();
    let account_block_path = account_block_file.path().to_str().unwrap().to_string();

    let mut config_file = NamedTempFile::new().unwrap();
    write!(
        config_file,
        r#"{{
            "log": {{ "level": "warn" }},
            "local_rpc_client": {{ "endpoint": "http://127.0.0.1:1" }},
            "remote_rpc_client": {{ "endpoint": "http://127.0.0.1:1" }},
            "cursor_file": "{}",
            "block_destination_file": "{}",
            "account_block_destination_file": "{}",
            "send_processed": false,
            "noop": false
        }}"#,
        cursor_file.path().to_str().unwrap(),
        block_path,
        account_block_path,
    )
    .unwrap();
    config_file.flush().unwrap();

    let mut plugin = Plugin::new(false, false);
    plugin
        .on_load(config_file.path().to_str().unwrap(), false)
        .unwrap();

    let mut rng = Rng(0x2545_F491_4F6C_DD1D);

    // Snapshot load: several versions of the same account, some older than one already seen
    for _ in 0..ADDRESSES * 2 {
        let address = address(rng.next(ADDRESSES));
        let owner = owner(&mut rng);
        let data = data(&mut rng);
        let lamports = if rng.chance(10) { 0 } else { 1 };
        let slot = rng.next(FIRST_SLOT);
        let write_version = rng.next(1_000);
        update_account(
            &plugin,
            slot,
            &address,
            &owner,
            &data,
            lamports,
            write_version,
            true,
        );
    }
    plugin.notify_end_of_startup().unwrap();

    // Rooted before the first block so the plugin never asks RPC for the LIB
    plugin
        .update_bank_status(FIRST_SLOT - 40, None, &SlotStatus::Rooted, 0)
        .unwrap();

    let rewards = RewardsAndNumPartitions {
        rewards: vec![],
        num_partitions: None,
    };
    let signature = Signature::default();
    let message_hash = Hash::default();
    let transaction = VersionedTransaction::default();
    let meta = TransactionStatusMeta::default();

    let mut write_version = 1_000u64;
    let mut parent = FIRST_SLOT - 1;
    for slot in FIRST_SLOT..=LAST_SLOT {
        // Skipped slot: no block, the next one keeps the same parent
        if slot > FIRST_SLOT && rng.chance(5) {
            continue;
        }
        // Fork slot: account updates and transactions for a slot that is never confirmed
        let is_fork = slot > FIRST_SLOT && rng.chance(4);

        for _ in 0..rng.next(60) {
            let address = address(rng.next(ADDRESSES + (slot - FIRST_SLOT) * 5));
            let owner = owner(&mut rng);
            let data = data(&mut rng);
            let lamports = if rng.chance(8) { 0 } else { 1 };
            // Mostly increasing write versions, with some going backwards within the slot
            write_version += 1;
            let version = if rng.chance(5) {
                write_version - rng.next(5)
            } else {
                write_version
            };
            update_account(
                &plugin, slot, &address, &owner, &data, lamports, version, false,
            );
        }

        // Late update for a slot that was already sent
        if slot > FIRST_SLOT + 5 && rng.chance(2) {
            write_version += 1;
            let address = address(rng.next(ADDRESSES));
            update_account(
                &plugin,
                slot - 5,
                &address,
                &[1; 32],
                &[9],
                1,
                write_version,
                false,
            );
        }

        let transaction_count = rng.next(4);
        for index in 0..transaction_count {
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

        if is_fork {
            continue;
        }

        let blockhash = format!("hash{}", slot);
        let parent_blockhash = format!("hash{}", parent);
        let block_info = ReplicaBlockInfoV4 {
            parent_slot: parent,
            parent_blockhash: &parent_blockhash,
            slot,
            blockhash: &blockhash,
            rewards: &rewards,
            block_time: Some(1_700_000_000 + slot as i64),
            block_height: Some(slot),
            executed_transaction_count: transaction_count,
            entry_count: 1,
        };
        plugin
            .notify_block_metadata_for_bank(ReplicaBlockInfoVersions::V0_0_4(&block_info), 0)
            .unwrap();
        plugin
            .update_bank_status(slot - 32, None, &SlotStatus::Rooted, 0)
            .unwrap();
        plugin
            .update_bank_status(slot, Some(parent), &SlotStatus::Confirmed, 0)
            .unwrap();
        parent = slot;
    }

    let (blocks, blocks_digest) = sorted_lines_digest(&block_path);
    let (account_blocks, account_blocks_digest) = sorted_lines_digest(&account_block_path);
    println!(
        "blocks={} digest={:#x}, account_blocks={} digest={:#x}",
        blocks, blocks_digest, account_blocks, account_blocks_digest
    );
    assert!(blocks > 300, "only {} block lines written", blocks);
    assert_eq!(blocks, account_blocks);
    assert_eq!(blocks_digest, EXPECTED_BLOCKS_DIGEST);
    assert_eq!(account_blocks_digest, EXPECTED_ACCOUNT_BLOCKS_DIGEST);
}
