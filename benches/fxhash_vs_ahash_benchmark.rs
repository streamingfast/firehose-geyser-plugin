/// Benchmark: FxHashMap vs AHashMap for the actual key types used in state.rs
///
/// state.rs uses:
///   AccountChanges           = FxHashMap<[u8; 64], AccountWithWriteVersion>
///   AccountState             = FxHashMap<[u8; 32], ([u8; 32], u64)>
///   StartupAccountReceivedSlot = FxHashMap<[u8; 32], u64>
///
/// We model set_account and set_account_on_startup as closely as possible.
use ahash::AHashMap;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rustc_hash::FxHashMap;

// ---------------------------------------------------------------------------
// Minimal AccountWithWriteVersion (mirrors the real struct)
// ---------------------------------------------------------------------------
#[derive(Clone)]
struct AccountWithWriteVersion {
    address: [u8; 32],
    owner: [u8; 32],
    data: Vec<u8>,
    deleted: bool,
    write_version: u64,
    data_hash: u64,
}

// ---------------------------------------------------------------------------
// FxHashMap implementations (current production code)
// ---------------------------------------------------------------------------
struct StateFx {
    // BlockAccountChanges outer map not included — benchmarking the inner maps
    account_changes: FxHashMap<[u8; 64], AccountWithWriteVersion>,
    account_state: FxHashMap<[u8; 32], ([u8; 32], u64)>,
    startup_received_slot: FxHashMap<[u8; 32], u64>,
}

impl StateFx {
    fn new() -> Self {
        Self {
            account_changes: FxHashMap::default(),
            account_state: FxHashMap::default(),
            startup_received_slot: FxHashMap::default(),
        }
    }

    fn set_account(
        &mut self,
        pub_key: &[u8; 32],
        owner: &[u8; 32],
        data: &[u8],
        write_version: u64,
        deleted: bool,
        data_hash: u64,
    ) {
        let mut owner_account_key = [0u8; 64];
        owner_account_key[..32].copy_from_slice(owner);
        owner_account_key[32..].copy_from_slice(pub_key);

        if let Some(prev) = self.account_changes.get(&owner_account_key) {
            if prev.write_version > write_version {
                return;
            }
        }

        self.account_changes.insert(
            owner_account_key,
            AccountWithWriteVersion {
                address: *pub_key,
                owner: *owner,
                data: data.to_vec(),
                deleted,
                write_version,
                data_hash,
            },
        );
    }

    fn set_account_on_startup(
        &mut self,
        pub_key: &[u8; 32],
        owner: &[u8; 32],
        data_hash: u64,
        slot: u64,
        write_version: u64,
    ) {
        let composite_value = (slot << 25) | write_version;

        if let Some(&existing) = self.startup_received_slot.get(pub_key) {
            if existing >= composite_value {
                return;
            }
        }
        self.startup_received_slot.insert(*pub_key, composite_value);

        if let Some((previous_owner, _)) = self.account_state.get(pub_key) {
            if previous_owner != owner {
                self.account_state.remove(pub_key);
            }
        }

        self.account_state.insert(*pub_key, (*owner, data_hash));
    }
}

// ---------------------------------------------------------------------------
// AHashMap implementations (candidate)
// ---------------------------------------------------------------------------
struct StateAhash {
    account_changes: AHashMap<[u8; 64], AccountWithWriteVersion>,
    account_state: AHashMap<[u8; 32], ([u8; 32], u64)>,
    startup_received_slot: AHashMap<[u8; 32], u64>,
}

impl StateAhash {
    fn new() -> Self {
        Self {
            account_changes: AHashMap::default(),
            account_state: AHashMap::default(),
            startup_received_slot: AHashMap::default(),
        }
    }

    fn set_account(
        &mut self,
        pub_key: &[u8; 32],
        owner: &[u8; 32],
        data: &[u8],
        write_version: u64,
        deleted: bool,
        data_hash: u64,
    ) {
        let mut owner_account_key = [0u8; 64];
        owner_account_key[..32].copy_from_slice(owner);
        owner_account_key[32..].copy_from_slice(pub_key);

        if let Some(prev) = self.account_changes.get(&owner_account_key) {
            if prev.write_version > write_version {
                return;
            }
        }

        self.account_changes.insert(
            owner_account_key,
            AccountWithWriteVersion {
                address: *pub_key,
                owner: *owner,
                data: data.to_vec(),
                deleted,
                write_version,
                data_hash,
            },
        );
    }

    fn set_account_on_startup(
        &mut self,
        pub_key: &[u8; 32],
        owner: &[u8; 32],
        data_hash: u64,
        slot: u64,
        write_version: u64,
    ) {
        let composite_value = (slot << 25) | write_version;

        if let Some(&existing) = self.startup_received_slot.get(pub_key) {
            if existing >= composite_value {
                return;
            }
        }
        self.startup_received_slot.insert(*pub_key, composite_value);

        if let Some((previous_owner, _)) = self.account_state.get(pub_key) {
            if previous_owner != owner {
                self.account_state.remove(pub_key);
            }
        }

        self.account_state.insert(*pub_key, (*owner, data_hash));
    }
}

// ---------------------------------------------------------------------------
// Test data generation
// ---------------------------------------------------------------------------
fn make_key(i: u64) -> [u8; 32] {
    let mut k = [0u8; 32];
    k[..8].copy_from_slice(&i.to_le_bytes());
    k
}

fn make_owner(i: u64) -> [u8; 32] {
    let mut k = [0u8; 32];
    k[..8].copy_from_slice(&(i % 200).to_le_bytes()); // 200 distinct owners
    k[8] = 0xAB;
    k
}

struct AccountEntry {
    pub_key: [u8; 32],
    owner: [u8; 32],
    data: Vec<u8>,
    write_version: u64,
    data_hash: u64,
    slot: u64,
}

fn generate_set_account_data(n: usize) -> Vec<AccountEntry> {
    (0..n)
        .map(|i| AccountEntry {
            pub_key: make_key(i as u64),
            owner: make_owner(i as u64),
            data: vec![i as u8; 64], // 64-byte account data
            write_version: 1,
            data_hash: i as u64,
            slot: (i / 10) as u64,
        })
        .collect()
}

fn generate_startup_data(n: usize) -> Vec<([u8; 32], [u8; 32], u64, u64, u64)> {
    let mut v: Vec<([u8; 32], [u8; 32], u64, u64, u64)> = (0..n)
        .map(|i| {
            (
                make_key(i as u64),
                make_owner(i as u64),
                i as u64,       // data_hash
                (i / 10) as u64, // slot
                1u64,           // write_version
            )
        })
        .collect();
    // Add 20% duplicates with lower write_version (should be skipped)
    for i in 0..(n / 5) {
        v.push((make_key(i as u64), make_owner(i as u64), 9999, (i / 10) as u64, 0));
    }
    v
}

// ---------------------------------------------------------------------------
// Benchmarks: set_account (AccountChanges + [u8;64] key)
// ---------------------------------------------------------------------------
fn bench_set_account_fx(c: &mut Criterion) {
    let data = generate_set_account_data(10_000);
    c.bench_function("fx_set_account_10k", |b| {
        b.iter(|| {
            let mut state = StateFx::new();
            for e in &data {
                state.set_account(
                    black_box(&e.pub_key),
                    black_box(&e.owner),
                    black_box(&e.data),
                    black_box(e.write_version),
                    black_box(false),
                    black_box(e.data_hash),
                );
            }
            black_box(state.account_changes.len())
        })
    });
}

fn bench_set_account_ahash(c: &mut Criterion) {
    let data = generate_set_account_data(10_000);
    c.bench_function("ahash_set_account_10k", |b| {
        b.iter(|| {
            let mut state = StateAhash::new();
            for e in &data {
                state.set_account(
                    black_box(&e.pub_key),
                    black_box(&e.owner),
                    black_box(&e.data),
                    black_box(e.write_version),
                    black_box(false),
                    black_box(e.data_hash),
                );
            }
            black_box(state.account_changes.len())
        })
    });
}

// ---------------------------------------------------------------------------
// Benchmarks: set_account_on_startup (AccountState + StartupReceivedSlot, [u8;32] keys)
// ---------------------------------------------------------------------------
fn bench_startup_fx(c: &mut Criterion) {
    let data = generate_startup_data(10_000);
    c.bench_function("fx_set_account_on_startup_10k", |b| {
        b.iter(|| {
            let mut state = StateFx::new();
            for &(pub_key, owner, data_hash, slot, wv) in &data {
                state.set_account_on_startup(
                    black_box(&pub_key),
                    black_box(&owner),
                    black_box(data_hash),
                    black_box(slot),
                    black_box(wv),
                );
            }
            black_box(state.account_state.len())
        })
    });
}

fn bench_startup_ahash(c: &mut Criterion) {
    let data = generate_startup_data(10_000);
    c.bench_function("ahash_set_account_on_startup_10k", |b| {
        b.iter(|| {
            let mut state = StateAhash::new();
            for &(pub_key, owner, data_hash, slot, wv) in &data {
                state.set_account_on_startup(
                    black_box(&pub_key),
                    black_box(&owner),
                    black_box(data_hash),
                    black_box(slot),
                    black_box(wv),
                );
            }
            black_box(state.account_state.len())
        })
    });
}

// ---------------------------------------------------------------------------
// Benchmarks: lookup-heavy (simulates repeated reads on hot maps)
// ---------------------------------------------------------------------------
fn bench_lookup_fx(c: &mut Criterion) {
    let data = generate_set_account_data(10_000);
    let mut state = StateFx::new();
    for e in &data {
        state.set_account(&e.pub_key, &e.owner, &e.data, e.write_version, false, e.data_hash);
    }
    c.bench_function("fx_lookup_10k", |b| {
        b.iter(|| {
            let mut found = 0usize;
            for e in &data {
                let mut key = [0u8; 64];
                key[..32].copy_from_slice(&e.owner);
                key[32..].copy_from_slice(&e.pub_key);
                if state.account_changes.get(black_box(&key)).is_some() {
                    found += 1;
                }
            }
            black_box(found)
        })
    });
}

fn bench_lookup_ahash(c: &mut Criterion) {
    let data = generate_set_account_data(10_000);
    let mut state = StateAhash::new();
    for e in &data {
        state.set_account(&e.pub_key, &e.owner, &e.data, e.write_version, false, e.data_hash);
    }
    c.bench_function("ahash_lookup_10k", |b| {
        b.iter(|| {
            let mut found = 0usize;
            for e in &data {
                let mut key = [0u8; 64];
                key[..32].copy_from_slice(&e.owner);
                key[32..].copy_from_slice(&e.pub_key);
                if state.account_changes.get(black_box(&key)).is_some() {
                    found += 1;
                }
            }
            black_box(found)
        })
    });
}

criterion_group!(
    benches,
    bench_set_account_fx,
    bench_set_account_ahash,
    bench_startup_fx,
    bench_startup_ahash,
    bench_lookup_fx,
    bench_lookup_ahash,
);
criterion_main!(benches);
