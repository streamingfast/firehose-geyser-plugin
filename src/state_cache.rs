use std::collections::HashMap;

/// A generic data structure that maintains a main hashmap and block-specific hashmaps
/// for storing data that can be merged or dropped by block number.
///
/// This structure supports tombstone-based deletions where deletions are recorded
/// at the block level and applied during merge operations, ensuring proper chronological
/// ordering of operations.
///
/// # Tombstone Deletion Pattern
///
/// Instead of directly removing entries from the main hashmap, deletions are recorded
/// as `None` values in block-specific hashmaps. When `merge_blocks_up_to()` is called,
/// these tombstone entries cause the corresponding keys to be removed from the main hashmap.
///
/// # Example
///
/// ```
/// # use std::collections::HashMap;
/// # #[derive(Debug, Clone)]
/// # pub struct BlockAwareHashMap<K, V>
/// # where
/// #     K: Clone + std::hash::Hash + Eq,
/// #     V: Clone,
/// # {
/// #     pub main: HashMap<K, V>,
/// #     pub blocks: HashMap<u64, HashMap<K, Option<V>>>,
/// # }
/// # impl<K, V> BlockAwareHashMap<K, V>
/// # where
/// #     K: Clone + std::hash::Hash + Eq,
/// #     V: Clone,
/// # {
/// #     pub fn new() -> Self { Self { main: HashMap::new(), blocks: HashMap::new() } }
/// #     pub fn insert(&mut self, key: K, value: V) { self.main.insert(key, value); }
/// #     pub fn insert_for_block(&mut self, block_number: u64, key: K, value: V) {
/// #         self.blocks.entry(block_number).or_insert_with(HashMap::new).insert(key, Some(value));
/// #     }
/// #     pub fn delete_for_block(&mut self, block_number: u64, key: K) {
/// #         self.blocks.entry(block_number).or_insert_with(HashMap::new).insert(key, None);
/// #     }
/// #     pub fn merge_blocks_up_to(&mut self, up_to_block: u64) {
/// #         let mut block_numbers: Vec<u64> = self.blocks.keys().filter(|&&block_number| block_number <= up_to_block).cloned().collect();
/// #         block_numbers.sort();
/// #         for block_number in &block_numbers {
/// #             if let Some(block_map) = self.blocks.get(block_number) {
/// #                 for (key, value_option) in block_map {
/// #                     match value_option {
/// #                         Some(value) => { self.main.insert(key.clone(), value.clone()); }
/// #                         None => { self.main.remove(key); }
/// #                     }
/// #                 }
/// #             }
/// #         }
/// #         for block_number in block_numbers { self.blocks.remove(&block_number); }
/// #     }
/// #     pub fn get(&self, key: &K) -> Option<&V> { self.main.get(key) }
/// # }
/// let mut map = BlockAwareHashMap::new();
/// let key1 = "key1".to_string();
/// let key2 = "key2".to_string();
/// let value1 = "value1".to_string();
/// let value2 = "value2".to_string();
/// let new_value1 = "new_value1".to_string();
///
/// // Set initial values in main hashmap
/// map.insert(key1.clone(), value1);
/// map.insert(key2.clone(), value2);
///
/// // Record deletion at block 100
/// map.delete_for_block(100, key1.clone());
///
/// // Record new value at block 150
/// map.insert_for_block(150, key1.clone(), new_value1);
///
/// // Record another deletion at block 200
/// map.delete_for_block(200, key2.clone());
///
/// // Apply all operations chronologically
/// map.merge_blocks_up_to(200);
/// // Result: key1 has new_value1, key2 is deleted
/// ```
#[derive(Debug, Clone)]
pub struct BlockAwareHashMap<K, V>
where
    K: Clone + std::hash::Hash + Eq,
    V: Clone,
{
    /// Main hashmap for storing data
    pub main: HashMap<K, V>,
    /// Block-specific hashmaps indexed by block number, using Option<V> where None represents deletion
    pub blocks: HashMap<u64, HashMap<K, Option<V>>>,
}

impl<K, V> BlockAwareHashMap<K, V>
where
    K: Clone + std::hash::Hash + Eq,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            main: HashMap::new(),
            blocks: HashMap::new(),
        }
    }

    /// Insert directly into the main hashmap
    pub fn insert(&mut self, key: K, value: V) {
        self.main.insert(key, value);
    }

    /// Insert into a block-specific hashmap
    pub fn insert_for_block(&mut self, block_number: u64, key: K, value: V) {
        self.blocks
            .entry(block_number)
            .or_insert_with(HashMap::new)
            .insert(key, Some(value));
    }

    /// Mark a key for deletion in a block-specific hashmap using a tombstone entry.
    ///
    /// This records a deletion operation that will be applied when `merge_blocks_up_to()`
    /// is called. The deletion is represented as a `None` value in the block-specific hashmap.
    ///
    /// # Arguments
    ///
    /// * `block_number` - The block number at which this deletion should be applied
    /// * `key` - The key to mark for deletion
    ///
    /// # Example
    ///
    /// ```
    /// # use std::collections::HashMap;
    /// # #[derive(Debug, Clone)]
    /// # pub struct BlockAwareHashMap<K, V>
    /// # where
    /// #     K: Clone + std::hash::Hash + Eq,
    /// #     V: Clone,
    /// # {
    /// #     pub main: HashMap<K, V>,
    /// #     pub blocks: HashMap<u64, HashMap<K, Option<V>>>,
    /// # }
    /// # impl<K, V> BlockAwareHashMap<K, V>
    /// # where
    /// #     K: Clone + std::hash::Hash + Eq,
    /// #     V: Clone,
    /// # {
    /// #     pub fn new() -> Self { Self { main: HashMap::new(), blocks: HashMap::new() } }
    /// #     pub fn insert(&mut self, key: K, value: V) { self.main.insert(key, value); }
    /// #     pub fn delete_for_block(&mut self, block_number: u64, key: K) {
    /// #         self.blocks.entry(block_number).or_insert_with(HashMap::new).insert(key, None);
    /// #     }
    /// #     pub fn merge_blocks_up_to(&mut self, up_to_block: u64) {
    /// #         let mut block_numbers: Vec<u64> = self.blocks.keys().filter(|&&block_number| block_number <= up_to_block).cloned().collect();
    /// #         block_numbers.sort();
    /// #         for block_number in &block_numbers {
    /// #             if let Some(block_map) = self.blocks.get(block_number) {
    /// #                 for (key, value_option) in block_map {
    /// #                     match value_option {
    /// #                         Some(value) => { self.main.insert(key.clone(), value.clone()); }
    /// #                         None => { self.main.remove(key); }
    /// #                     }
    /// #                 }
    /// #             }
    /// #         }
    /// #         for block_number in block_numbers { self.blocks.remove(&block_number); }
    /// #     }
    /// #     pub fn get(&self, key: &K) -> Option<&V> { self.main.get(key) }
    /// # }
    /// let mut map = BlockAwareHashMap::new();
    /// let key = "test_key".to_string();
    ///
    /// map.insert(key.clone(), 42);
    /// map.delete_for_block(100, key.clone());
    /// map.merge_blocks_up_to(100);
    /// assert_eq!(map.get(&key), None); // Key is now deleted
    /// ```
    pub fn delete_for_block(&mut self, block_number: u64, key: K) {
        self.blocks
            .entry(block_number)
            .or_insert_with(HashMap::new)
            .insert(key, None);
    }

    /// Get value from main hashmap
    pub fn get(&self, key: &K) -> Option<&V> {
        self.main.get(key)
    }

    /// Get the length of the main hashmap
    pub fn len(&self) -> usize {
        self.main.len()
    }

    /// Merge the content of block-specific hashmaps from the lowest block numbers up to the given block number into the main hashmap.
    ///
    /// This method processes blocks in chronological order to ensure operations are applied correctly.
    /// `Some(value)` entries update the main hashmap, while `None` entries (tombstones) delete keys from the main hashmap.
    ///
    /// # Arguments
    ///
    /// * `up_to_block` - All blocks with numbers <= this value will be merged and removed
    ///
    /// # Behavior
    ///
    /// 1. Blocks are processed in ascending order by block number
    /// 2. For each key-value pair in each block:
    ///    - `Some(value)` → Insert/update the key in main hashmap
    ///    - `None` → Remove the key from main hashmap (tombstone deletion)
    /// 3. All processed blocks are removed from the blocks hashmap
    ///
    /// # Example
    ///
    /// ```
    /// # use std::collections::HashMap;
    /// # #[derive(Debug, Clone)]
    /// # pub struct BlockAwareHashMap<K, V>
    /// # where
    /// #     K: Clone + std::hash::Hash + Eq,
    /// #     V: Clone,
    /// # {
    /// #     pub main: HashMap<K, V>,
    /// #     pub blocks: HashMap<u64, HashMap<K, Option<V>>>,
    /// # }
    /// # impl<K, V> BlockAwareHashMap<K, V>
    /// # where
    /// #     K: Clone + std::hash::Hash + Eq,
    /// #     V: Clone,
    /// # {
    /// #     pub fn new() -> Self { Self { main: HashMap::new(), blocks: HashMap::new() } }
    /// #     pub fn insert(&mut self, key: K, value: V) { self.main.insert(key, value); }
    /// #     pub fn insert_for_block(&mut self, block_number: u64, key: K, value: V) {
    /// #         self.blocks.entry(block_number).or_insert_with(HashMap::new).insert(key, Some(value));
    /// #     }
    /// #     pub fn delete_for_block(&mut self, block_number: u64, key: K) {
    /// #         self.blocks.entry(block_number).or_insert_with(HashMap::new).insert(key, None);
    /// #     }
    /// #     pub fn merge_blocks_up_to(&mut self, up_to_block: u64) {
    /// #         let mut block_numbers: Vec<u64> = self.blocks.keys().filter(|&&block_number| block_number <= up_to_block).cloned().collect();
    /// #         block_numbers.sort();
    /// #         for block_number in &block_numbers {
    /// #             if let Some(block_map) = self.blocks.get(block_number) {
    /// #                 for (key, value_option) in block_map {
    /// #                     match value_option {
    /// #                         Some(value) => { self.main.insert(key.clone(), value.clone()); }
    /// #                         None => { self.main.remove(key); }
    /// #                     }
    /// #                 }
    /// #             }
    /// #         }
    /// #         for block_number in block_numbers { self.blocks.remove(&block_number); }
    /// #     }
    /// #     pub fn get(&self, key: &K) -> Option<&V> { self.main.get(key) }
    /// # }
    /// let mut map = BlockAwareHashMap::new();
    /// let key1 = "key1".to_string();
    /// let key2 = "key2".to_string();
    ///
    /// // Main: {key1: 10, key2: 20}
    /// map.insert(key1.clone(), 10);
    /// map.insert(key2.clone(), 20);
    ///
    /// // Block 100: {key1: None}           // Delete key1
    /// map.delete_for_block(100, key1.clone());
    ///
    /// // Block 150: {key1: Some(11)}       // Set key1 = 11
    /// map.insert_for_block(150, key1.clone(), 11);
    ///
    /// // Block 200: {key2: None}           // Delete key2
    /// map.delete_for_block(200, key2.clone());
    ///
    /// map.merge_blocks_up_to(200);
    /// // Result: {key1: 11} (key2 deleted)
    /// assert_eq!(map.get(&key1), Some(&11));
    /// assert_eq!(map.get(&key2), None);
    /// ```
    pub fn merge_blocks_up_to(&mut self, up_to_block: u64) {
        // Collect block numbers to process and sort them to ensure chronological order
        let mut block_numbers: Vec<u64> = self
            .blocks
            .keys()
            .filter(|&&block_number| block_number <= up_to_block)
            .cloned()
            .collect();
        block_numbers.sort();

        // Process blocks in chronological order
        for block_number in &block_numbers {
            if let Some(block_map) = self.blocks.get(block_number) {
                for (key, value_option) in block_map {
                    match value_option {
                        Some(value) => {
                            self.main.insert(key.clone(), value.clone());
                        }
                        None => {
                            self.main.remove(key);
                        }
                    }
                }
            }
        }

        // Remove processed blocks
        for block_number in block_numbers {
            self.blocks.remove(&block_number);
        }
    }

    /// Drop a specific block number's hashmap
    pub fn drop_block(&mut self, block_number: u64) -> Option<HashMap<K, Option<V>>> {
        self.blocks.remove(&block_number)
    }
}

impl<K, V> Default for BlockAwareHashMap<K, V>
where
    K: Clone + std::hash::Hash + Eq,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Specialized type for account data hashes
pub type AccountDataHash = BlockAwareHashMap<Vec<u8>, u64>;

/// Specialized type for account owners
pub type AccountOwners = BlockAwareHashMap<Vec<u8>, Vec<u8>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_aware_hashmap_new() {
        let map: BlockAwareHashMap<Vec<u8>, u64> = BlockAwareHashMap::new();
        assert_eq!(map.len(), 0);
        assert!(map.main.is_empty());
        assert!(map.blocks.is_empty());
    }

    #[test]
    fn test_block_aware_hashmap_insert_and_get() {
        let mut map = BlockAwareHashMap::new();
        let key = vec![1, 2, 3];
        let value = 42;

        map.insert(key.clone(), value);
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&key), Some(&value));
        assert_eq!(map.get(&vec![4, 5, 6]), None);
    }

    #[test]
    fn test_block_aware_hashmap_insert_for_block() {
        let mut map = BlockAwareHashMap::new();
        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5, 6];
        let value1 = 42;
        let value2 = 84;

        // Insert into block 100
        map.insert_for_block(100, key1.clone(), value1);
        map.insert_for_block(100, key2.clone(), value2);

        // Insert into block 200
        map.insert_for_block(200, key1.clone(), value1 + 1);

        // Main hashmap should still be empty
        assert_eq!(map.len(), 0);
        assert_eq!(map.get(&key1), None);

        // Block-specific data should be there
        assert_eq!(map.blocks.len(), 2);
        assert!(map.blocks.contains_key(&100));
        assert!(map.blocks.contains_key(&200));
        assert_eq!(map.blocks.get(&100).unwrap().len(), 2);
        assert_eq!(map.blocks.get(&200).unwrap().len(), 1);
        assert_eq!(
            map.blocks.get(&100).unwrap().get(&key1),
            Some(&Some(value1))
        );
        assert_eq!(
            map.blocks.get(&200).unwrap().get(&key1),
            Some(&Some(value1 + 1))
        );
    }

    #[test]
    fn test_block_aware_hashmap_merge_blocks_up_to() {
        let mut map = BlockAwareHashMap::new();
        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5, 6];
        let key3 = vec![7, 8, 9];

        // Insert into different blocks
        map.insert_for_block(100, key1.clone(), 42);
        map.insert_for_block(150, key2.clone(), 84);
        map.insert_for_block(200, key3.clone(), 126);

        // Merge blocks up to 150
        map.merge_blocks_up_to(150);

        // Main hashmap should now contain data from blocks 100 and 150
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&key1), Some(&42));
        assert_eq!(map.get(&key2), Some(&84));
        assert_eq!(map.get(&key3), None); // Block 200 shouldn't be merged yet

        // Blocks 100 and 150 should be removed, 200 should remain
        assert_eq!(map.blocks.len(), 1);
        assert!(!map.blocks.contains_key(&100));
        assert!(!map.blocks.contains_key(&150));
        assert!(map.blocks.contains_key(&200));
    }

    #[test]
    fn test_block_aware_hashmap_drop_block() {
        let mut map = BlockAwareHashMap::new();
        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5, 6];

        // Insert into block 100
        map.insert_for_block(100, key1.clone(), 42);
        map.insert_for_block(100, key2.clone(), 84);

        assert_eq!(map.blocks.len(), 1);
        assert!(map.blocks.contains_key(&100));
        assert_eq!(map.blocks.get(&100).unwrap().len(), 2);

        // Drop block 100
        let dropped = map.drop_block(100);
        assert!(dropped.is_some());
        let dropped_map = dropped.unwrap();
        assert_eq!(dropped_map.len(), 2);
        assert_eq!(dropped_map.get(&key1), Some(&Some(42)));
        assert_eq!(dropped_map.get(&key2), Some(&Some(84)));

        // Block should be removed
        assert_eq!(map.blocks.len(), 0);
        assert!(!map.blocks.contains_key(&100));

        // Dropping non-existent block should return None
        assert!(map.drop_block(200).is_none());
    }

    #[test]
    fn test_block_aware_hashmap_delete_for_block() {
        let mut map = BlockAwareHashMap::new();
        let key = vec![1, 2, 3];
        let value = 42;

        // Insert into main
        map.insert(key.clone(), value);
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&key), Some(&value));

        // Mark for deletion in block 100
        map.delete_for_block(100, key.clone());

        // Key should still exist in main before merge
        assert_eq!(map.get(&key), Some(&value));

        // After merge, key should be deleted
        map.merge_blocks_up_to(100);
        assert_eq!(map.len(), 0);
        assert_eq!(map.get(&key), None);
    }

    #[test]
    fn test_block_aware_hashmap_comprehensive_workflow() {
        let mut map = BlockAwareHashMap::new();
        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5, 6];
        let key3 = vec![7, 8, 9];
        let key4 = vec![10, 11, 12];

        // Step 1: Insert some data directly into main hashmap
        map.insert(key1.clone(), 100);
        map.insert(key2.clone(), 200);
        assert_eq!(map.len(), 2);

        // Step 2: Insert data into different block-specific hashmaps
        map.insert_for_block(101, key3.clone(), 300);
        map.insert_for_block(101, key4.clone(), 400);
        map.insert_for_block(102, key1.clone(), 150); // Different value for same key
        map.insert_for_block(103, key2.clone(), 250); // Different value for same key

        // Verify block data exists and main hasn't changed
        assert_eq!(map.len(), 2); // Main hashmap still has 2 items
        assert_eq!(map.blocks.len(), 3); // 3 different blocks
        assert_eq!(map.get(&key1), Some(&100)); // Original value in main

        // Step 3: Merge blocks up to 102 (includes blocks 101 and 102)
        map.merge_blocks_up_to(102);

        // Verify merge results
        assert_eq!(map.len(), 4); // Main now has 4 items
        assert_eq!(map.get(&key1), Some(&150)); // Updated from block 102
        assert_eq!(map.get(&key2), Some(&200)); // Still original from main
        assert_eq!(map.get(&key3), Some(&300)); // From block 101
        assert_eq!(map.get(&key4), Some(&400)); // From block 101

        // Block 103 should still exist, others should be gone
        assert_eq!(map.blocks.len(), 1);
        let block_numbers: Vec<u64> = map.blocks.keys().cloned().collect();
        assert_eq!(block_numbers.len(), 1);
        assert!(block_numbers.contains(&103));

        // Step 4: Drop block 103 without merging
        let dropped = map.drop_block(103);
        assert!(dropped.is_some());
        let dropped_map = dropped.unwrap();
        assert_eq!(dropped_map.len(), 1);
        assert_eq!(dropped_map.get(&key2), Some(&Some(250)));

        // Verify block is gone but main is unchanged
        assert_eq!(map.blocks.len(), 0);
        assert_eq!(map.get(&key2), Some(&200)); // Still original value

        // Final state verification
        assert_eq!(map.get(&key1), Some(&150));
        assert_eq!(map.get(&key2), Some(&200));
        assert_eq!(map.get(&key3), Some(&300));
        assert_eq!(map.get(&key4), Some(&400));
    }

    #[test]
    fn test_account_data_hash_type_alias() {
        let mut hash: AccountDataHash = AccountDataHash::new();
        let key = vec![1, 2, 3];
        let value = 42u64;

        hash.insert(key.clone(), value);
        assert_eq!(hash.get(&key), Some(&value));
    }

    #[test]
    fn test_account_owners_type_alias() {
        let mut owners: AccountOwners = AccountOwners::new();
        let pub_key = vec![1, 2, 3];
        let owner_key = vec![4, 5, 6];

        owners.insert(pub_key.clone(), owner_key.clone());
        assert_eq!(owners.get(&pub_key), Some(&owner_key));
    }

    #[test]
    fn test_tombstone_deletion_workflow() {
        let mut map = BlockAwareHashMap::new();
        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5, 6];

        // Set up main hashmap
        map.insert(key1.clone(), 10);
        map.insert(key2.clone(), 20);
        assert_eq!(map.get(&key1), Some(&10));
        assert_eq!(map.get(&key2), Some(&20));

        // Block 100: mark key1 for deletion (empty value)
        map.delete_for_block(100, key1.clone());

        // Block 150: update both keys
        map.insert_for_block(150, key1.clone(), 11);
        map.insert_for_block(150, key2.clone(), 22);

        // Block 200: mark key2 for deletion (empty value)
        map.delete_for_block(200, key2.clone());

        // Before merge: main should still have original values
        assert_eq!(map.get(&key1), Some(&10));
        assert_eq!(map.get(&key2), Some(&20));

        // Merge up to block 200
        map.merge_blocks_up_to(200);

        // After merge: key1 should be deleted (last operation was deletion in block 100, but then set to 11 in block 150)
        // key2 should be deleted (set to 22 in block 150, then deleted in block 200)
        assert_eq!(map.get(&key1), Some(&11)); // key1 was deleted in block 100, then set to 11 in block 150
        assert_eq!(map.get(&key2), None); // key2 was set to 22 in block 150, then deleted in block 200
        assert_eq!(map.len(), 1);
    }
}
