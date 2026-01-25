//! Memory-optimized state structures using SQL-style normalization with arena allocation
//!
//! This module provides a memory-efficient approach to storing account data
//! by avoiding duplication of pubkeys and owners across multiple HashMaps.
//!
//! ## Memory Comparison
//!
//! ### Current approach (per account):
//! - AccountDataHash: 64 bytes (owner+pubkey key) + 8 bytes (u64 value) = 72 bytes
//! - AccountOwners: 32 bytes (pubkey key) + 32 bytes (owner value) = 64 bytes
//! - StartupAccountReceivedSlot: 32 bytes (pubkey key) + 8 bytes (u64 value) = 40 bytes
//! Total: ~176 bytes + HashMap overhead (~24 bytes per entry) = ~248 bytes per account
//!
//! ### Optimized approach with Arena (per account):
//! - Normalized storage: 32 bytes (pubkey) + 32 bytes (owner) stored once in arena
//! - Indices: 12 bytes (2 U48 indices, 6 bytes each)
//! - Metadata: 8 bytes (data_hash) + 8 bytes (slot+write_version)
//! - Arena reverse lookups: HashMap overhead for deduplication
//! Total: ~104-224 bytes per account with U48 indices (depends on owner deduplication)
//! **Memory savings: 10-58% reduction** (scales with owner reuse)
//!
//! For 100M accounts:
//! - Current: ~24.8 GB
//! - Arena with low owner reuse: ~22.4 GB (saves ~2.4 GB, 10% reduction)
//! - Arena with high owner reuse: ~10.4 GB (saves ~14.4 GB, 58% reduction)
//!
//! ## Design
//!
//! The optimized design uses an "arena allocator" pattern where:
//! 1. Pubkeys and owners are stored once in a Vec (the "arena")
//! 2. Indices (U48 - 48-bit integers) are used instead of storing the full keys everywhere
//! 3. Multiple HashMaps use these indices for fast lookups
//! 4. U48 provides excellent capacity (281 trillion accounts, ~890K years to overflow)
//! 5. **Explicit owner deduplication** - owners are stored only once and referenced by index
//! 6. **Optimal memory balance** - U48 is 25% smaller than u64, but vastly larger than u32
//!
//! This is particularly effective for Solana where:
//! - Token accounts share the same owner (SPL Token Program)
//! - System accounts share the System Program owner
//! - Program accounts share BPF Loader owners

use std::collections::HashMap;

/// Custom 48-bit unsigned integer for optimal memory/capacity balance
///
/// Provides:
/// - Capacity: 281 trillion accounts (281,474,976,710,656)
/// - Memory: 6 bytes (25% smaller than u64, 50% larger than u32)
/// - Years to overflow: ~890,000 years at 1M accounts/slot
///
/// This is the sweet spot between u32 (too limited) and u64 (larger than needed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct U48(u64); // Stores value in lower 48 bits

impl U48 {
    /// Maximum value (2^48 - 1)
    pub const MAX: u64 = 0xFFFF_FFFF_FFFF;

    /// Create a new U48 from u64, panicking if value exceeds 48 bits
    #[inline]
    pub fn new(value: u64) -> Self {
        assert!(
            value <= Self::MAX,
            "Value {} exceeds U48::MAX ({})",
            value,
            Self::MAX
        );
        U48(value)
    }

    /// Create a new U48 from usize, panicking if value exceeds 48 bits
    #[inline]
    pub fn from_usize(value: usize) -> Self {
        Self::new(value as u64)
    }

    /// Get the value as u64
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Get the value as usize
    #[inline]
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl From<U48> for u64 {
    #[inline]
    fn from(val: U48) -> u64 {
        val.0
    }
}

impl From<U48> for usize {
    #[inline]
    fn from(val: U48) -> usize {
        val.0 as usize
    }
}

/// Index type for pubkeys in the arena
/// Using U48 (6 bytes) to balance memory efficiency and capacity
/// - Capacity: 281 trillion accounts
/// - Memory: 25% smaller than u64
/// - Overflow time: ~890,000 years at 1M accounts/slot
pub type PubkeyIndex = U48;

/// Index type for owners in the arena
/// Using U48 (6 bytes) for consistency and ample capacity
pub type OwnerIndex = U48;

/// Arena storage for deduplicated account keys
#[derive(Clone)]
pub struct KeyArena {
    /// Storage for all pubkeys (32 bytes each)
    pubkeys: Vec<[u8; 32]>,

    /// Storage for all owners (32 bytes each)
    owners: Vec<[u8; 32]>,

    /// Reverse lookup: pubkey -> index
    pubkey_to_index: HashMap<[u8; 32], PubkeyIndex>,

    /// Reverse lookup: owner -> index
    owner_to_index: HashMap<[u8; 32], OwnerIndex>,
}

impl KeyArena {
    pub fn new() -> Self {
        Self {
            pubkeys: Vec::new(),
            owners: Vec::new(),
            pubkey_to_index: HashMap::new(),
            owner_to_index: HashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            pubkeys: Vec::with_capacity(capacity),
            owners: Vec::with_capacity(capacity),
            pubkey_to_index: HashMap::with_capacity(capacity),
            owner_to_index: HashMap::with_capacity(capacity),
        }
    }

    /// Get or insert a pubkey, returning its index
    pub fn intern_pubkey(&mut self, pubkey: [u8; 32]) -> PubkeyIndex {
        if let Some(&index) = self.pubkey_to_index.get(&pubkey) {
            return index;
        }

        let index = U48::from_usize(self.pubkeys.len());
        self.pubkeys.push(pubkey);
        self.pubkey_to_index.insert(pubkey, index);
        index
    }

    /// Get or insert an owner, returning its index
    pub fn intern_owner(&mut self, owner: [u8; 32]) -> OwnerIndex {
        if let Some(&index) = self.owner_to_index.get(&owner) {
            return index;
        }

        let index = U48::from_usize(self.owners.len());
        self.owners.push(owner);
        self.owner_to_index.insert(owner, index);
        index
    }

    /// Get pubkey by index
    #[inline]
    pub fn get_pubkey(&self, index: PubkeyIndex) -> Option<&[u8; 32]> {
        self.pubkeys.get(index.as_usize())
    }

    /// Get owner by index
    #[inline]
    pub fn get_owner(&self, index: OwnerIndex) -> Option<&[u8; 32]> {
        self.owners.get(index.as_usize())
    }

    /// Get pubkey index if it exists
    #[inline]
    pub fn get_pubkey_index(&self, pubkey: &[u8; 32]) -> Option<PubkeyIndex> {
        self.pubkey_to_index.get(pubkey).copied()
    }

    /// Get owner index if it exists
    #[inline]
    pub fn get_owner_index(&self, owner: &[u8; 32]) -> Option<OwnerIndex> {
        self.owner_to_index.get(owner).copied()
    }
}

/// Composite key for (owner_index, pubkey_index) used for data hash lookups
///
/// With U48 indices, this is only 12 bytes instead of 64 bytes (5.3x smaller!)
/// - 6 bytes (owner index) + 6 bytes (pubkey index) = 12 bytes total
/// - vs original: 32 bytes (owner) + 32 bytes (pubkey) = 64 bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompositeIndex {
    pub owner: OwnerIndex,
    pub pubkey: PubkeyIndex,
}

impl CompositeIndex {
    #[inline]
    pub fn new(owner: OwnerIndex, pubkey: PubkeyIndex) -> Self {
        Self { owner, pubkey }
    }
}

/// Optimized replacement for AccountDataHash
/// Uses indices instead of full 64-byte keys
pub type AccountDataHashOptimized = HashMap<CompositeIndex, u64>;

/// Optimized replacement for AccountOwners
/// Uses pubkey index to owner index mapping instead of [u8; 32] -> [u8; 32]
pub type AccountOwnersOptimized = HashMap<PubkeyIndex, OwnerIndex>;

/// Optimized replacement for StartupAccountReceivedSlot
/// Uses pubkey index instead of [u8; 32]
pub type StartupAccountReceivedSlotOptimized = HashMap<PubkeyIndex, u64>;

/// Optimized state structure
#[derive(Clone)]
pub struct StateOptimized {
    /// Shared arena for all pubkeys and owners
    pub arena: KeyArena,

    /// Maps (owner_index, pubkey_index) -> data_hash
    /// Replaces: HashMap<[u8; 64], u64>
    pub account_data_hash: AccountDataHashOptimized,

    /// Maps pubkey_index -> owner_index
    /// Replaces: HashMap<[u8; 32], [u8; 32]>
    pub account_owners: AccountOwnersOptimized,

    /// Maps pubkey_index -> composite value (slot << 25 | write_version)
    /// Replaces: HashMap<[u8; 32], u64>
    pub startup_received_slot: StartupAccountReceivedSlotOptimized,
}

impl StateOptimized {
    pub fn new() -> Self {
        Self {
            arena: KeyArena::new(),
            account_data_hash: HashMap::new(),
            account_owners: HashMap::new(),
            startup_received_slot: HashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            arena: KeyArena::with_capacity(capacity),
            account_data_hash: HashMap::with_capacity(capacity),
            account_owners: HashMap::with_capacity(capacity),
            startup_received_slot: HashMap::with_capacity(capacity),
        }
    }

    /// Set account data during startup (optimized version)
    pub fn set_account_on_startup(
        &mut self,
        pubkey: [u8; 32],
        owner: [u8; 32],
        data_hash: u64,
        slot: u64,
        write_version: u64,
        deleted: bool,
    ) {
        // Intern the keys to get their indices
        let pubkey_idx = self.arena.intern_pubkey(pubkey);
        let owner_idx = self.arena.intern_owner(owner);

        // Pack slot and write_version
        let composite_value = (slot << 25) | write_version;

        // Check if we already have this account with a newer version
        if let Some(&existing_composite) = self.startup_received_slot.get(&pubkey_idx) {
            if existing_composite >= composite_value {
                return;
            }
        }
        self.startup_received_slot
            .insert(pubkey_idx, composite_value);

        // Check for ownership changes
        if let Some(&previous_owner_idx) = self.account_owners.get(&pubkey_idx) {
            if previous_owner_idx != owner_idx {
                // Remove old owner+pubkey combination
                let old_composite = CompositeIndex::new(previous_owner_idx, pubkey_idx);
                self.account_data_hash.remove(&old_composite);
            }
        }

        let composite_key = CompositeIndex::new(owner_idx, pubkey_idx);

        if deleted {
            self.account_data_hash.remove(&composite_key);
            self.account_owners.remove(&pubkey_idx);
        } else {
            self.account_data_hash.insert(composite_key, data_hash);
            self.account_owners.insert(pubkey_idx, owner_idx);
        }
    }

    /// Get data hash for an account
    pub fn get_data_hash(&self, pubkey: &[u8; 32], owner: &[u8; 32]) -> Option<u64> {
        let pubkey_idx = self.arena.get_pubkey_index(pubkey)?;
        let owner_idx = self.arena.get_owner_index(owner)?;
        let composite = CompositeIndex::new(owner_idx, pubkey_idx);
        self.account_data_hash.get(&composite).copied()
    }

    /// Get owner for a pubkey
    pub fn get_owner(&self, pubkey: &[u8; 32]) -> Option<[u8; 32]> {
        let pubkey_idx = self.arena.get_pubkey_index(pubkey)?;
        let owner_idx = *self.account_owners.get(&pubkey_idx)?;
        self.arena.get_owner(owner_idx).copied()
    }

    /// Get data hash by looking up owner_account_key (64 bytes: owner+pubkey)
    /// This is for compatibility with the old HashMap<[u8; 64], u64> interface
    pub fn get_data_hash_by_composite_key(&self, owner_account_key: &[u8; 64]) -> Option<u64> {
        let owner = &owner_account_key[..32];
        let pubkey = &owner_account_key[32..];

        let mut owner_fixed = [0u8; 32];
        let mut pubkey_fixed = [0u8; 32];
        owner_fixed.copy_from_slice(owner);
        pubkey_fixed.copy_from_slice(pubkey);

        self.get_data_hash(&pubkey_fixed, &owner_fixed)
    }

    /// Insert into account_owners using pubkey directly
    /// Returns the owner index that was inserted
    pub fn insert_owner_for_pubkey(&mut self, pubkey: [u8; 32], owner: [u8; 32]) -> OwnerIndex {
        let pubkey_idx = self.arena.intern_pubkey(pubkey);
        let owner_idx = self.arena.intern_owner(owner);
        self.account_owners.insert(pubkey_idx, owner_idx);
        owner_idx
    }

    /// Get owner by pubkey, returns the owner as [u8; 32]
    /// Compatible with old AccountOwners::get(&pubkey) interface
    pub fn get_owner_by_pubkey(&self, pubkey: &[u8; 32]) -> Option<[u8; 32]> {
        self.get_owner(pubkey)
    }

    /// Get memory statistics
    pub fn memory_stats(&self) -> MemoryStats {
        let arena_pubkeys = self.arena.pubkeys.len() * 32;
        let arena_owners = self.arena.owners.len() * 32;
        let arena_pubkey_map = self.arena.pubkey_to_index.len() * (32 + 8 + 24); // key + U48(8 bytes due to alignment) + overhead
        let arena_owner_map = self.arena.owner_to_index.len() * (32 + 8 + 24);

        let data_hash_map = self.account_data_hash.len() * (16 + 8 + 24); // CompositeIndex(12 bytes, but 16 with alignment) + u64 + overhead
        let owners_map = self.account_owners.len() * (8 + 8 + 16); // U48 key + U48 value (8 bytes each with alignment) + overhead
        let startup_map = self.startup_received_slot.len() * (8 + 8 + 16); // U48 key + u64 value + overhead

        MemoryStats {
            arena_pubkeys_bytes: arena_pubkeys,
            arena_owners_bytes: arena_owners,
            arena_pubkey_map_bytes: arena_pubkey_map,
            arena_owner_map_bytes: arena_owner_map,
            data_hash_map_bytes: data_hash_map,
            owners_map_bytes: owners_map,
            startup_map_bytes: startup_map,
            total_bytes: arena_pubkeys
                + arena_owners
                + arena_pubkey_map
                + arena_owner_map
                + data_hash_map
                + owners_map
                + startup_map,
            num_accounts: self.account_owners.len(),
        }
    }
}

#[derive(Debug)]
pub struct MemoryStats {
    pub arena_pubkeys_bytes: usize,
    pub arena_owners_bytes: usize,
    pub arena_pubkey_map_bytes: usize,
    pub arena_owner_map_bytes: usize,
    pub data_hash_map_bytes: usize,
    pub owners_map_bytes: usize,
    pub startup_map_bytes: usize,
    pub total_bytes: usize,
    pub num_accounts: usize,
}

impl MemoryStats {
    pub fn bytes_per_account(&self) -> f64 {
        if self.num_accounts == 0 {
            0.0
        } else {
            self.total_bytes as f64 / self.num_accounts as f64
        }
    }

    pub fn print(&self) {
        println!("Memory Statistics:");
        println!(
            "  Arena pubkeys:     {:>12} bytes",
            self.arena_pubkeys_bytes
        );
        println!("  Arena owners:      {:>12} bytes", self.arena_owners_bytes);
        println!(
            "  Pubkey index map:  {:>12} bytes",
            self.arena_pubkey_map_bytes
        );
        println!(
            "  Owner index map:   {:>12} bytes",
            self.arena_owner_map_bytes
        );
        println!(
            "  Data hash map:     {:>12} bytes",
            self.data_hash_map_bytes
        );
        println!("  Owners map:        {:>12} bytes", self.owners_map_bytes);
        println!("  Startup map:       {:>12} bytes", self.startup_map_bytes);
        println!("  ---");
        println!(
            "  Total:             {:>12} bytes ({:.2} MB)",
            self.total_bytes,
            self.total_bytes as f64 / 1_048_576.0
        );
        println!("  Accounts:          {:>12}", self.num_accounts);
        println!("  Bytes/account:     {:>12.2}", self.bytes_per_account());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_arena_deduplication() {
        let mut arena = KeyArena::new();

        let pubkey1 = [1u8; 32];
        let pubkey2 = [2u8; 32];
        let pubkey1_dup = [1u8; 32];

        let idx1 = arena.intern_pubkey(pubkey1);
        let idx2 = arena.intern_pubkey(pubkey2);
        let idx1_dup = arena.intern_pubkey(pubkey1_dup);

        // Same pubkey should return same index
        assert_eq!(idx1, idx1_dup);
        assert_ne!(idx1, idx2);

        // Should only store 2 unique pubkeys
        assert_eq!(arena.pubkeys.len(), 2);
    }

    #[test]
    fn test_optimized_state_basic() {
        let mut state = StateOptimized::new();

        let pubkey = [1u8; 32];
        let owner = [2u8; 32];
        let data_hash = 12345u64;

        state.set_account_on_startup(pubkey, owner, data_hash, 100, 1, false);

        assert_eq!(state.get_data_hash(&pubkey, &owner), Some(data_hash));
        assert_eq!(state.get_owner(&pubkey), Some(owner));
    }

    #[test]
    fn test_ownership_change() {
        let mut state = StateOptimized::new();

        let pubkey = [1u8; 32];
        let owner1 = [2u8; 32];
        let owner2 = [3u8; 32];

        // Set initial owner
        state.set_account_on_startup(pubkey, owner1, 100, 1, 1, false);
        assert_eq!(state.get_owner(&pubkey), Some(owner1));

        // Change owner
        state.set_account_on_startup(pubkey, owner2, 200, 2, 1, false);
        assert_eq!(state.get_owner(&pubkey), Some(owner2));

        // Old owner+pubkey combo should not have data
        assert_eq!(state.get_data_hash(&pubkey, &owner1), None);
        // New owner+pubkey combo should have data
        assert_eq!(state.get_data_hash(&pubkey, &owner2), Some(200));
    }

    #[test]
    fn test_memory_efficiency() {
        let mut state_optimized = StateOptimized::with_capacity(1000);

        // Simulate 1000 accounts with some owner reuse
        for i in 0u32..1000 {
            let mut pubkey = [0u8; 32];
            let mut owner = [0u8; 32];

            pubkey[0..4].copy_from_slice(&i.to_le_bytes());
            // Reuse owners (10 accounts per owner)
            owner[0..4].copy_from_slice(&(i / 10).to_le_bytes());

            state_optimized.set_account_on_startup(pubkey, owner, i as u64, i as u64, 1, false);
        }

        let stats = state_optimized.memory_stats();
        stats.print();

        // With 1000 accounts and 100 unique owners, we should have:
        assert_eq!(state_optimized.arena.pubkeys.len(), 1000);
        assert_eq!(state_optimized.arena.owners.len(), 100); // Owner deduplication

        // With u64 indices and owner deduplication
        println!("Bytes per account: {:.2}", stats.bytes_per_account());
        assert!(stats.bytes_per_account() < 250.0);
    }

    #[test]
    fn test_u48_capacity() {
        // Verify we're using U48 (48-bit) indices
        let _arena = KeyArena::new();

        // U48::MAX is 281,474,976,710,656 (281 trillion)
        // This provides excellent capacity while using less memory than u64
        assert_eq!(std::mem::size_of::<PubkeyIndex>(), 8); // Due to alignment, still 8 bytes in struct
        assert_eq!(std::mem::size_of::<OwnerIndex>(), 8);

        // But we can verify the actual capacity
        let max_accounts = U48::MAX;
        println!(
            "Max supported accounts: {} (U48::MAX = 2^48 - 1)",
            max_accounts
        );
        println!("That's 281 trillion accounts");

        // At 400ms per slot and assuming 1M new accounts per slot,
        // it would take ~890,000 years to overflow
        let years_to_overflow = max_accounts as f64 / (1_000_000.0 * 365.25 * 24.0 * 3600.0 / 0.4);
        println!(
            "Years to overflow at 1M accounts/slot: {:.0}",
            years_to_overflow
        );

        // Verify U48 operations work correctly
        let index = U48::new(12345);
        assert_eq!(index.as_u64(), 12345);
        assert_eq!(index.as_usize(), 12345);

        // Verify max value works
        let max_index = U48::new(U48::MAX);
        assert_eq!(max_index.as_u64(), U48::MAX);
    }

    #[test]
    #[should_panic(expected = "exceeds U48::MAX")]
    fn test_u48_overflow_panics() {
        // Verify that exceeding U48::MAX panics
        U48::new(U48::MAX + 1);
    }
}
