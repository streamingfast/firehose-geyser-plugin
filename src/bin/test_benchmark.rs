use ahash::AHashMap;
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

fn main() {
    println!("Testing hashbrown vs ahash implementations...\n");

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
    println!("Testing hashbrown implementation...");
    let mut state_hashbrown = StateHashbrown::new();
    for (pub_key, owner, data_hash, slot, write_version) in &test_data {
        state_hashbrown.set_account_on_startup(pub_key, owner, *data_hash, *slot, *write_version);
    }

    // Test ahash implementation
    println!("Testing ahash implementation...");
    let mut state_ahash = StateAhash::new();
    for (pub_key, owner, data_hash, slot, write_version) in &test_data {
        state_ahash.set_account_on_startup(pub_key, owner, *data_hash, *slot, *write_version);
    }

    // Verify both implementations have the same results
    println!("\nVerifying results match...");

    assert_eq!(
        state_hashbrown.startup_received_slot.len(),
        state_ahash.startup_received_slot.len(),
        "startup_received_slot lengths don't match"
    );
    assert_eq!(
        state_hashbrown.account_owners.len(),
        state_ahash.account_owners.len(),
        "account_owners lengths don't match"
    );
    assert_eq!(
        state_hashbrown.account_data_hash.len(),
        state_ahash.account_data_hash.len(),
        "account_data_hash lengths don't match"
    );

    println!("✓ HashMap sizes match");
    println!(
        "  - startup_received_slot: {} entries",
        state_hashbrown.startup_received_slot.len()
    );
    println!(
        "  - account_owners: {} entries",
        state_hashbrown.account_owners.len()
    );
    println!(
        "  - account_data_hash: {} entries",
        state_hashbrown.account_data_hash.len()
    );

    // Verify specific entries
    let pubkey1 = b"pubkey1".to_vec();
    let pubkey2 = b"pubkey2".to_vec();
    let pubkey3 = b"pubkey3".to_vec();

    // pubkey1 should have slot=2, write_version=1 (from owner change)
    assert_eq!(
        state_hashbrown.startup_received_slot.get(&pubkey1),
        Some(&(2u64, 1u64)),
        "hashbrown pubkey1 slot/version mismatch"
    );
    assert_eq!(
        state_ahash.startup_received_slot.get(&pubkey1),
        Some(&(2u64, 1u64)),
        "ahash pubkey1 slot/version mismatch"
    );

    // pubkey2 should have slot=2, write_version=1
    assert_eq!(
        state_hashbrown.startup_received_slot.get(&pubkey2),
        Some(&(2u64, 1u64)),
        "hashbrown pubkey2 slot/version mismatch"
    );
    assert_eq!(
        state_ahash.startup_received_slot.get(&pubkey2),
        Some(&(2u64, 1u64)),
        "ahash pubkey2 slot/version mismatch"
    );

    // pubkey3 should have slot=3, write_version=1
    assert_eq!(
        state_hashbrown.startup_received_slot.get(&pubkey3),
        Some(&(3u64, 1u64)),
        "hashbrown pubkey3 slot/version mismatch"
    );
    assert_eq!(
        state_ahash.startup_received_slot.get(&pubkey3),
        Some(&(3u64, 1u64)),
        "ahash pubkey3 slot/version mismatch"
    );

    println!("✓ Slot/version values match");

    // Check owner changes worked correctly
    assert_eq!(
        state_hashbrown.account_owners.get(&pubkey1),
        Some(&b"owner_new".to_vec()),
        "hashbrown pubkey1 owner not updated"
    );
    assert_eq!(
        state_ahash.account_owners.get(&pubkey1),
        Some(&b"owner_new".to_vec()),
        "ahash pubkey1 owner not updated"
    );

    println!("✓ Owner changes handled correctly");
    println!("\n🎉 All tests passed! Both implementations work identically.");

    // Performance comparison with larger dataset
    println!("\nRunning quick performance comparison...");

    let large_test_data: Vec<_> = (0..1000)
        .map(|i| {
            (
                format!("pubkey_{:08}", i).as_bytes().to_vec(),
                format!("owner_{:04}", i % 10).as_bytes().to_vec(),
                i as u64,
                i as u64,
                1u64,
            )
        })
        .collect();

    let start = std::time::Instant::now();
    let mut state_hashbrown = StateHashbrown::new();
    for (pub_key, owner, data_hash, slot, write_version) in &large_test_data {
        state_hashbrown.set_account_on_startup(pub_key, owner, *data_hash, *slot, *write_version);
    }
    let hashbrown_time = start.elapsed();

    let start = std::time::Instant::now();
    let mut state_ahash = StateAhash::new();
    for (pub_key, owner, data_hash, slot, write_version) in &large_test_data {
        state_ahash.set_account_on_startup(pub_key, owner, *data_hash, *slot, *write_version);
    }
    let ahash_time = start.elapsed();

    println!("Performance comparison (1000 entries):");
    println!("  - hashbrown: {:?}", hashbrown_time);
    println!("  - ahash:     {:?}", ahash_time);

    if hashbrown_time < ahash_time {
        let speedup = ahash_time.as_nanos() as f64 / hashbrown_time.as_nanos() as f64;
        println!("  → hashbrown is {:.2}x faster", speedup);
    } else {
        let speedup = hashbrown_time.as_nanos() as f64 / ahash_time.as_nanos() as f64;
        println!("  → ahash is {:.2}x faster", speedup);
    }
}
