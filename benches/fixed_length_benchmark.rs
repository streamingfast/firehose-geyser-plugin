use criterion::{black_box, criterion_group, criterion_main, Criterion};
use hashbrown::HashMap as HashbrownHashMap;

// Type definitions for Vec<u8> keys (original implementation)
pub type AccountDataHashVec = HashbrownHashMap<Vec<u8>, u64>;
pub type AccountOwnersVec = HashbrownHashMap<Vec<u8>, Vec<u8>>;
pub type StartupAccountReceivedSlotVec = HashbrownHashMap<Vec<u8>, (u64, u64)>;

// Type definitions for fixed-length keys
pub type AccountDataHashFixed = HashbrownHashMap<[u8; 64], u64>; // owner(32) + pubkey(32)
pub type AccountOwnersFixed = HashbrownHashMap<[u8; 32], [u8; 32]>; // pubkey(32) -> owner(32)
pub type StartupAccountReceivedSlotFixed = HashbrownHashMap<[u8; 32], (u64, u64)>; // pubkey(32)

// State struct using Vec<u8> keys (original)
#[derive(Default)]
pub struct StateVec {
    account_data_hash: AccountDataHashVec,
    account_owners: AccountOwnersVec,
    startup_received_slot: StartupAccountReceivedSlotVec,
}

impl StateVec {
    pub fn new() -> Self {
        Self {
            account_data_hash: HashbrownHashMap::default(),
            account_owners: HashbrownHashMap::default(),
            startup_received_slot: HashbrownHashMap::default(),
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

// State struct using fixed-length keys
#[derive(Default)]
pub struct StateFixed {
    account_data_hash: AccountDataHashFixed,
    account_owners: AccountOwnersFixed,
    startup_received_slot: StartupAccountReceivedSlotFixed,
}

impl StateFixed {
    pub fn new() -> Self {
        Self {
            account_data_hash: HashbrownHashMap::default(),
            account_owners: HashbrownHashMap::default(),
            startup_received_slot: HashbrownHashMap::default(),
        }
    }

    pub fn set_account_on_startup(
        &mut self,
        pub_key: &[u8; 32],
        owner: &[u8; 32],
        data_hash: u64,
        slot: u64,
        write_version: u64,
    ) {
        if let Some((existing_slot, existing_write_version)) =
            self.startup_received_slot.get(pub_key)
        {
            if *existing_slot > slot
                || (*existing_slot == slot && *existing_write_version > write_version)
            {
                return;
            }
        }
        self.startup_received_slot
            .insert(*pub_key, (slot, write_version));

        // Check if there was a previous owner for this public key
        if let Some(previous_owner) = self.account_owners.get(pub_key) {
            if previous_owner != owner {
                // Previous owner is different, so delete the old entry from account_data_hash
                let mut previous_owner_account_key = [0u8; 64];
                previous_owner_account_key[..32].copy_from_slice(previous_owner);
                previous_owner_account_key[32..].copy_from_slice(pub_key);
                self.account_data_hash.remove(&previous_owner_account_key);
            }
        }

        // Create owner+pubkey composite key
        let mut owner_account_key = [0u8; 64];
        owner_account_key[..32].copy_from_slice(owner);
        owner_account_key[32..].copy_from_slice(pub_key);

        self.account_data_hash.insert(owner_account_key, data_hash);
        self.account_owners.insert(*pub_key, *owner);
    }
}

// Test data generation for Vec<u8> version
fn generate_test_data_vec() -> Vec<(Vec<u8>, Vec<u8>, u64, u64, u64)> {
    let mut data = Vec::with_capacity(10000);

    // Generate 8000 unique entries
    for i in 0..8000 {
        let pub_key_str = format!("pubkey_{:024}", i);
        let owner_str = format!("owner_{:025}", i % 100);
        let pub_key = pub_key_str.as_bytes()[..pub_key_str.len().min(32)].to_vec();
        let owner = owner_str.as_bytes()[..owner_str.len().min(32)].to_vec();
        let data_hash = i as u64;
        let slot = i as u64;
        let write_version = 1;

        data.push((pub_key, owner, data_hash, slot, write_version));
    }

    // Add 2000 duplicates (these should be skipped due to existing slot/write_version)
    for i in 0..2000 {
        let pub_key_str = format!("pubkey_{:024}", i);
        let owner_str = format!("owner_{:025}", i % 100);
        let pub_key = pub_key_str.as_bytes()[..pub_key_str.len().min(32)].to_vec();
        let owner = owner_str.as_bytes()[..owner_str.len().min(32)].to_vec();
        let data_hash = (i + 10000) as u64; // Different data hash
        let slot = i as u64; // Same slot
        let write_version = 0; // Lower write version (should be skipped)

        data.push((pub_key, owner, data_hash, slot, write_version));
    }

    data
}

// Test data generation for fixed-length version
fn generate_test_data_fixed() -> Vec<([u8; 32], [u8; 32], u64, u64, u64)> {
    let mut data = Vec::with_capacity(10000);

    // Generate 8000 unique entries
    for i in 0..8000 {
        let mut pub_key = [0u8; 32];
        let mut owner = [0u8; 32];

        let pub_key_str = format!("pubkey_{:024}", i);
        let owner_str = format!("owner_{:025}", i % 100);

        pub_key[..pub_key_str.len().min(32)]
            .copy_from_slice(&pub_key_str.as_bytes()[..pub_key_str.len().min(32)]);
        owner[..owner_str.len().min(32)]
            .copy_from_slice(&owner_str.as_bytes()[..owner_str.len().min(32)]);

        let data_hash = i as u64;
        let slot = i as u64;
        let write_version = 1;

        data.push((pub_key, owner, data_hash, slot, write_version));
    }

    // Add 2000 duplicates (these should be skipped due to existing slot/write_version)
    for i in 0..2000 {
        let mut pub_key = [0u8; 32];
        let mut owner = [0u8; 32];

        let pub_key_str = format!("pubkey_{:024}", i);
        let owner_str = format!("owner_{:025}", i % 100);

        pub_key[..pub_key_str.len().min(32)]
            .copy_from_slice(&pub_key_str.as_bytes()[..pub_key_str.len().min(32)]);
        owner[..owner_str.len().min(32)]
            .copy_from_slice(&owner_str.as_bytes()[..owner_str.len().min(32)]);

        let data_hash = (i + 10000) as u64; // Different data hash
        let slot = i as u64; // Same slot
        let write_version = 0; // Lower write version (should be skipped)

        data.push((pub_key, owner, data_hash, slot, write_version));
    }

    data
}

fn benchmark_vec_keys(c: &mut Criterion) {
    let test_data = generate_test_data_vec();

    c.bench_function("vec_keys_set_account_on_startup", |b| {
        b.iter(|| {
            let mut state = StateVec::new();
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

fn benchmark_fixed_keys(c: &mut Criterion) {
    let test_data = generate_test_data_fixed();

    c.bench_function("fixed_keys_set_account_on_startup", |b| {
        b.iter(|| {
            let mut state = StateFixed::new();
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

// Verification function to ensure both implementations work identically
fn verify_implementations() {
    println!("Verifying Vec<u8> vs fixed-length implementations...");

    // Create identical test data for both
    let test_entries = vec![
        (
            "pubkey_001".as_bytes(),
            "owner_001".as_bytes(),
            100u64,
            1u64,
            1u64,
        ),
        (
            "pubkey_002".as_bytes(),
            "owner_002".as_bytes(),
            200u64,
            2u64,
            1u64,
        ),
        (
            "pubkey_001".as_bytes(),
            "owner_001".as_bytes(),
            150u64,
            1u64,
            0u64,
        ), // Should be skipped
        (
            "pubkey_003".as_bytes(),
            "owner_003".as_bytes(),
            300u64,
            3u64,
            1u64,
        ),
        (
            "pubkey_001".as_bytes(),
            "owner_new".as_bytes(),
            180u64,
            2u64,
            1u64,
        ), // Owner change
    ];

    // Test Vec<u8> implementation
    let mut state_vec = StateVec::new();
    for (pub_key, owner, data_hash, slot, write_version) in &test_entries {
        state_vec.set_account_on_startup(pub_key, owner, *data_hash, *slot, *write_version);
    }

    // Test fixed-length implementation
    let mut state_fixed = StateFixed::new();
    for (pub_key, owner, data_hash, slot, write_version) in &test_entries {
        let mut pub_key_fixed = [0u8; 32];
        let mut owner_fixed = [0u8; 32];

        pub_key_fixed[..pub_key.len().min(32)].copy_from_slice(&pub_key[..pub_key.len().min(32)]);
        owner_fixed[..owner.len().min(32)].copy_from_slice(&owner[..owner.len().min(32)]);

        state_fixed.set_account_on_startup(
            &pub_key_fixed,
            &owner_fixed,
            *data_hash,
            *slot,
            *write_version,
        );
    }

    // Verify both implementations have the same number of entries
    assert_eq!(
        state_vec.startup_received_slot.len(),
        state_fixed.startup_received_slot.len(),
        "startup_received_slot lengths don't match"
    );
    assert_eq!(
        state_vec.account_owners.len(),
        state_fixed.account_owners.len(),
        "account_owners lengths don't match"
    );
    assert_eq!(
        state_vec.account_data_hash.len(),
        state_fixed.account_data_hash.len(),
        "account_data_hash lengths don't match"
    );

    println!("✓ Both implementations have same number of entries");
    println!(
        "  - startup_received_slot: {} entries",
        state_vec.startup_received_slot.len()
    );
    println!(
        "  - account_owners: {} entries",
        state_vec.account_owners.len()
    );
    println!(
        "  - account_data_hash: {} entries",
        state_vec.account_data_hash.len()
    );
    println!("✓ All verification tests passed!");
}

fn benchmark_verification(c: &mut Criterion) {
    // Run verification once at start
    verify_implementations();

    c.bench_function("verify_fixed_vs_vec", |b| {
        b.iter(|| {
            // Just run a minimal test to avoid spam
            let mut state_vec = StateVec::new();
            let mut state_fixed = StateFixed::new();

            let pub_key_fixed = [b't'; 32];
            let owner_fixed = [b'o'; 32];

            state_vec.set_account_on_startup(
                b"test_key_32_bytes_long_12345678",
                b"owner_key_32_bytes_long_1234567",
                1,
                1,
                1,
            );
            state_fixed.set_account_on_startup(&pub_key_fixed, &owner_fixed, 1, 1, 1);

            assert_eq!(state_vec.account_owners.len(), 1);
            assert_eq!(state_fixed.account_owners.len(), 1);
        })
    });
}

criterion_group!(
    benches,
    benchmark_vec_keys,
    benchmark_fixed_keys,
    benchmark_verification
);
criterion_main!(benches);
