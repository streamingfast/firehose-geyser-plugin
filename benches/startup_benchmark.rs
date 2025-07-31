use ahash::AHashMap;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use hashbrown::HashMap as HashbrownHashMap;

// Type definitions for hashbrown implementation
pub type AccountDataHashHashbrown = HashbrownHashMap<Vec<u8>, u64>;
pub type AccountOwnersHashbrown = HashbrownHashMap<Vec<u8>, Vec<u8>>;
pub type StartupAccountReceivedSlotHashbrown = HashbrownHashMap<Vec<u8>, (u64, u64)>;

// Type definitions for ahash implementation
pub type AccountDataHashAhash = AHashMap<Vec<u8>, u64>;
pub type AccountOwnersAhash = AHashMap<Vec<u8>, Vec<u8>>;
pub type StartupAccountReceivedSlotAhash = AHashMap<Vec<u8>, (u64, u64)>;

// Simplified State struct using hashbrown
#[derive(Default)]
pub struct StateHashbrown {
    account_data_hash: AccountDataHashHashbrown,
    account_owners: AccountOwnersHashbrown,
    startup_received_slot: StartupAccountReceivedSlotHashbrown,
}

impl StateHashbrown {
    pub fn new() -> Self {
        Self {
            account_data_hash: HashbrownHashMap::new(),
            account_owners: HashbrownHashMap::new(),
            startup_received_slot: HashbrownHashMap::new(),
        }
    }

    pub fn set_account_on_startup(
        &mut self,
        pub_key: &[u8],
        owner: &[u8],
        data_hash: u64,
        slot: u64,
        write_version: u64,
    ) {
        let pub_key_vec = pub_key.to_vec();

        if let Some((existing_slot, existing_write_version)) =
            self.startup_received_slot.get(&pub_key_vec)
        {
            if *existing_slot > slot
                || (*existing_slot == slot && *existing_write_version > write_version)
            {
                return;
            }
        }
        self.startup_received_slot
            .insert(pub_key_vec.clone(), (slot, write_version));

        // Check if there was a previous owner for this public key
        if let Some(previous_owner) = self.account_owners.get(&pub_key_vec) {
            if previous_owner != owner {
                // Previous owner is different, so delete the old entry from account_data_hash
                let mut previous_owner_account_key =
                    Vec::with_capacity(previous_owner.len() + pub_key.len());
                previous_owner_account_key.extend_from_slice(previous_owner);
                previous_owner_account_key.extend_from_slice(pub_key);
                self.account_data_hash.remove(&previous_owner_account_key);
            }
        }

        // Pre-allocate with known capacity to avoid reallocation
        let mut owner_account_key = Vec::with_capacity(owner.len() + pub_key.len());
        owner_account_key.extend_from_slice(owner);
        owner_account_key.extend_from_slice(pub_key);

        self.account_data_hash.insert(owner_account_key, data_hash);
        self.account_owners.insert(pub_key_vec, owner.to_vec());
    }
}

// Simplified State struct using ahash
#[derive(Default)]
pub struct StateAhash {
    account_data_hash: AccountDataHashAhash,
    account_owners: AccountOwnersAhash,
    startup_received_slot: StartupAccountReceivedSlotAhash,
}

impl StateAhash {
    pub fn new() -> Self {
        Self {
            account_data_hash: AHashMap::new(),
            account_owners: AHashMap::new(),
            startup_received_slot: AHashMap::new(),
        }
    }

    pub fn set_account_on_startup(
        &mut self,
        pub_key: &[u8],
        owner: &[u8],
        data_hash: u64,
        slot: u64,
        write_version: u64,
    ) {
        let pub_key_vec = pub_key.to_vec();

        if let Some((existing_slot, existing_write_version)) =
            self.startup_received_slot.get(&pub_key_vec)
        {
            if *existing_slot > slot
                || (*existing_slot == slot && *existing_write_version > write_version)
            {
                return;
            }
        }
        self.startup_received_slot
            .insert(pub_key_vec.clone(), (slot, write_version));

        // Check if there was a previous owner for this public key
        if let Some(previous_owner) = self.account_owners.get(&pub_key_vec) {
            if previous_owner != owner {
                // Previous owner is different, so delete the old entry from account_data_hash
                let mut previous_owner_account_key =
                    Vec::with_capacity(previous_owner.len() + pub_key.len());
                previous_owner_account_key.extend_from_slice(previous_owner);
                previous_owner_account_key.extend_from_slice(pub_key);
                self.account_data_hash.remove(&previous_owner_account_key);
            }
        }

        // Pre-allocate with known capacity to avoid reallocation
        let mut owner_account_key = Vec::with_capacity(owner.len() + pub_key.len());
        owner_account_key.extend_from_slice(owner);
        owner_account_key.extend_from_slice(pub_key);

        self.account_data_hash.insert(owner_account_key, data_hash);
        self.account_owners.insert(pub_key_vec, owner.to_vec());
    }
}

// Test data generation
fn generate_test_data() -> Vec<(Vec<u8>, Vec<u8>, u64, u64, u64)> {
    let mut data = Vec::with_capacity(10000);

    // Generate 8000 unique entries
    for i in 0..8000 {
        let pub_key = format!("pubkeypubkeypubkeypubkey{:08}", i)
            .as_bytes()
            .to_vec();
        let owner = format!("owner_owner_owner_ownerowner{:04}", i % 100)
            .as_bytes()
            .to_vec(); // 100 different owners
        let data_hash = i as u64;
        let slot = i as u64;
        let write_version = 1;

        data.push((pub_key, owner, data_hash, slot, write_version));
    }

    // Add 2000 duplicates (these should be skipped due to existing slot/write_version)
    for i in 0..2000 {
        let pub_key = format!("pubkeypubkeypubkeypubkey{:08}", i)
            .as_bytes()
            .to_vec();
        let owner = format!("owner_owner_owner_ownerowner{:04}", i % 100)
            .as_bytes()
            .to_vec();
        let data_hash = (i + 10000) as u64; // Different data hash
        let slot = i as u64; // Same slot
        let write_version = 0; // Lower write version (should be skipped)

        data.push((pub_key, owner, data_hash, slot, write_version));
    }

    data
}

fn benchmark_hashbrown_detailed(c: &mut Criterion) {
    let test_data = generate_test_data();

    c.bench_function("hashbrown_detailed", |b| {
        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let mut state = StateHashbrown::new();
                for (pub_key, owner, data_hash, slot, write_version) in &test_data {
                    state.set_account_on_startup(
                        black_box(pub_key),
                        black_box(owner),
                        black_box(*data_hash),
                        black_box(*slot),
                        black_box(*write_version),
                    );
                }
                black_box(state);
            }
            start.elapsed()
        })
    });
}

fn benchmark_ahash_detailed(c: &mut Criterion) {
    let test_data = generate_test_data();

    c.bench_function("ahash_detailed", |b| {
        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let mut state = StateAhash::new();
                for (pub_key, owner, data_hash, slot, write_version) in &test_data {
                    state.set_account_on_startup(
                        black_box(pub_key),
                        black_box(owner),
                        black_box(*data_hash),
                        black_box(*slot),
                        black_box(*write_version),
                    );
                }
                black_box(state);
            }
            start.elapsed()
        })
    });
}

fn benchmark_hashbrown(c: &mut Criterion) {
    let test_data = generate_test_data();

    c.bench_function("hashbrown_set_account_on_startup", |b| {
        b.iter(|| {
            let mut state = StateHashbrown::new();
            for (pub_key, owner, data_hash, slot, write_version) in &test_data {
                state.set_account_on_startup(
                    black_box(pub_key),
                    black_box(owner),
                    black_box(*data_hash),
                    black_box(*slot),
                    black_box(*write_version),
                );
            }
            black_box(state)
        })
    });
}

fn benchmark_ahash(c: &mut Criterion) {
    let test_data = generate_test_data();

    c.bench_function("ahash_set_account_on_startup", |b| {
        b.iter(|| {
            let mut state = StateAhash::new();
            for (pub_key, owner, data_hash, slot, write_version) in &test_data {
                state.set_account_on_startup(
                    black_box(pub_key),
                    black_box(owner),
                    black_box(*data_hash),
                    black_box(*slot),
                    black_box(*write_version),
                );
            }
            black_box(state)
        })
    });
}

// Verification tests to ensure both implementations work correctly
fn verify_implementations() {
    let test_data = vec![
        (b"pubkey1".to_vec(), b"owner1".to_vec(), 100u64, 1u64, 1u64),
        (b"pubkey2".to_vec(), b"owner2".to_vec(), 200u64, 2u64, 1u64),
        (b"pubkey1".to_vec(), b"owner1".to_vec(), 150u64, 1u64, 0u64), // Should be skipped (lower write_version)
        (b"pubkey3".to_vec(), b"owner3".to_vec(), 300u64, 3u64, 1u64),
        (
            b"pubkey1".to_vec(),
            b"owner_new".to_vec(),
            180u64,
            2u64,
            1u64,
        ), // Owner change
    ];

    // Test hashbrown implementation
    let mut state_hashbrown = StateHashbrown::new();
    for (pub_key, owner, data_hash, slot, write_version) in &test_data {
        state_hashbrown.set_account_on_startup(pub_key, owner, *data_hash, *slot, *write_version);
    }

    // Test ahash implementation
    let mut state_ahash = StateAhash::new();
    for (pub_key, owner, data_hash, slot, write_version) in &test_data {
        state_ahash.set_account_on_startup(pub_key, owner, *data_hash, *slot, *write_version);
    }

    // Verify both implementations have the same results
    assert_eq!(
        state_hashbrown.startup_received_slot.len(),
        state_ahash.startup_received_slot.len()
    );
    assert_eq!(
        state_hashbrown.account_owners.len(),
        state_ahash.account_owners.len()
    );
    assert_eq!(
        state_hashbrown.account_data_hash.len(),
        state_ahash.account_data_hash.len()
    );

    // Verify specific entries
    let pubkey1 = b"pubkey1".to_vec();
    let pubkey2 = b"pubkey2".to_vec();
    let pubkey3 = b"pubkey3".to_vec();

    // pubkey1 should have slot=2, write_version=1 (from owner change)
    assert_eq!(
        state_hashbrown.startup_received_slot.get(&pubkey1),
        Some(&(2u64, 1u64))
    );
    assert_eq!(
        state_ahash.startup_received_slot.get(&pubkey1),
        Some(&(2u64, 1u64))
    );

    // pubkey2 should have slot=2, write_version=1
    assert_eq!(
        state_hashbrown.startup_received_slot.get(&pubkey2),
        Some(&(2u64, 1u64))
    );
    assert_eq!(
        state_ahash.startup_received_slot.get(&pubkey2),
        Some(&(2u64, 1u64))
    );

    // pubkey3 should have slot=3, write_version=1
    assert_eq!(
        state_hashbrown.startup_received_slot.get(&pubkey3),
        Some(&(3u64, 1u64))
    );
    assert_eq!(
        state_ahash.startup_received_slot.get(&pubkey3),
        Some(&(3u64, 1u64))
    );

    println!("✓ All verification tests passed!");
}

fn benchmark_verification(c: &mut Criterion) {
    // Run verification once at start
    verify_implementations();

    c.bench_function("verify_implementations", |b| {
        b.iter(|| {
            // Just run a minimal test to avoid spam
            let mut state_hashbrown = StateHashbrown::new();
            let mut state_ahash = StateAhash::new();

            state_hashbrown.set_account_on_startup(b"test", b"owner", 1, 1, 1);
            state_ahash.set_account_on_startup(b"test", b"owner", 1, 1, 1);

            assert_eq!(state_hashbrown.account_owners.len(), 1);
            assert_eq!(state_ahash.account_owners.len(), 1);
        })
    });
}

criterion_group!(
    benches,
    benchmark_hashbrown,
    benchmark_ahash,
    benchmark_hashbrown_detailed,
    benchmark_ahash_detailed,
    benchmark_verification
);
criterion_main!(benches);
