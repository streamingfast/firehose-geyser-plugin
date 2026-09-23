use crate::block_printer::BlockPrinter;
use crate::config::DevelopmentConfig;
use crate::pb;
use crate::utils::{convert_sol_timestamp, create_account_block};
use lazy_static::lazy_static;
use pb::sf::solana::r#type::v1::Account;
use prost_types::Timestamp;
use rustc_hash::FxHashMap as HashMap;
use rustc_hash::FxHashSet;
use solana_rpc_client::rpc_client::RpcClient;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Mutex;

type BlockAccountChanges = HashMap<u64, AccountChanges>;
pub type AccountChanges = HashMap<[u8; 64], AccountWithWriteVersion>;
pub type AccountOwners = HashMap<[u8; 32], [u8; 32]>; // pubkey(32) -> owner(32)
                                                      // pubkey(32) -> composite value (slot << 25 + write_version)

pub type Transactions = HashMap<u64, Vec<ConfirmTransactionWithIndex>>;
type ProcessedSlot = HashMap<u64, bool>;

type BlockInfoMap = HashMap<u64, BlockInfo>;
type ConfirmedSlotsMap = HashMap<u64, bool>;

/// Number of `PendingAccountChanges` shards, picked by pubkey.
const PENDING_SHARDS: usize = 16;

/// Account changes of the slots not sent yet, split by pubkey into shards behind their own
/// locks, so account updates arriving on different validator threads rarely wait on each
/// other. Changes are only read and purged under the state write lock, when no update runs.
pub struct PendingAccountChanges {
    shards: Vec<Mutex<BlockAccountChanges>>,
    /// Slots with at least one change in any shard.
    slots: Mutex<FxHashSet<u64>>,
}

impl Default for PendingAccountChanges {
    fn default() -> Self {
        PendingAccountChanges {
            shards: (0..PENDING_SHARDS)
                .map(|_| Mutex::new(HashMap::default()))
                .collect(),
            slots: Mutex::new(FxHashSet::default()),
        }
    }
}

impl PendingAccountChanges {
    fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        mutex.lock().expect("pending account changes lock poisoned")
    }

    pub fn contains_slot(&self, slot: u64) -> bool {
        Self::lock(&self.slots).contains(&slot)
    }

    /// Keeps the change unless one with a higher write version is already pending for the
    /// same owner and account in that slot.
    pub fn insert(
        &self,
        slot: u64,
        owner_account_key: [u8; 64],
        change: AccountWithWriteVersion,
        trace: bool,
    ) {
        let mut shard =
            Self::lock(&self.shards[owner_account_key[32 + 16] as usize % PENDING_SHARDS]);
        let slot_entries = match shard.get_mut(&slot) {
            Some(slot_entries) => slot_entries,
            None => {
                if Self::lock(&self.slots).insert(slot) {
                    debug!("got some account data for slot {}", slot);
                }
                shard.entry(slot).or_default()
            }
        };

        if let Some(prev) = slot_entries.get(&owner_account_key) {
            if prev.write_version > change.write_version {
                if trace {
                    debug!(
                        "skipping slot because older version: {}, pub_key: {:?}, owner: {:?}, write_version: {}, prev_write_version: {}, deleted: {}, data_hash: {}",
                        slot, bs58::encode(change.account.address).into_string(), bs58::encode(change.account.owner).into_string(), change.write_version, prev.write_version, change.account.deleted, change.data_hash
                    );
                }
                return; // skipping older write_versions
            }
        }

        if trace {
            let data = &change.account.data;
            let data_as_hex = hex::encode(&data[..10.min(data.len())]);
            debug!("handle_account_change@{}: account {:?} owner: {:?} delete: {:?} version: {:?} Data Size: {} Data Hash: {} Data Preview: {}", slot, bs58::encode(change.account.address).into_string(), bs58::encode(change.account.owner).into_string(), change.account.deleted, change.write_version, data.len(), change.data_hash, data_as_hex);
        }

        slot_entries.insert(owner_account_key, change);
    }

    /// The pending changes of `slot`, across shards.
    fn slot_changes(&mut self, slot: u64) -> impl Iterator<Item = &AccountWithWriteVersion> {
        self.shards
            .iter_mut()
            .filter_map(move |shard| {
                shard
                    .get_mut()
                    .expect("pending account changes lock poisoned")
                    .get(&slot)
            })
            .flat_map(|changes| changes.values())
    }

    /// Moves out the pending changes of `slot`. The slot stays listed, so purging it still
    /// clears its block info.
    fn take_slot_changes(&mut self, slot: u64) -> Vec<AccountWithWriteVersion> {
        let mut changes = Vec::new();
        for shard in self.shards.iter_mut() {
            if let Some(slot_changes) = shard
                .get_mut()
                .expect("pending account changes lock poisoned")
                .get_mut(&slot)
            {
                changes.extend(slot_changes.drain().map(|(_, change)| change));
            }
        }
        changes
    }

    /// Removes the slots at or below `upto` and returns them.
    fn purge_up_to(&mut self, upto: u64) -> Vec<u64> {
        let slots = self
            .slots
            .get_mut()
            .expect("pending account changes lock poisoned");
        let purged: Vec<u64> = slots.iter().copied().filter(|&slot| slot <= upto).collect();
        if purged.is_empty() {
            return purged;
        }
        slots.retain(|&slot| slot > upto);
        for shard in self.shards.iter_mut() {
            shard
                .get_mut()
                .expect("pending account changes lock poisoned")
                .retain(|&slot, _| slot > upto);
        }
        purged
    }

    pub fn min_slot(&self) -> Option<u64> {
        Self::lock(&self.slots).iter().min().copied()
    }

    pub fn slot_count(&self) -> usize {
        Self::lock(&self.slots).len()
    }

    pub fn slots_between(&self, first: u64, last: u64) -> usize {
        Self::lock(&self.slots)
            .iter()
            .filter(|&&slot| slot >= first && slot <= last)
            .count()
    }

    /// Number of pending changes and bytes of account data they hold.
    pub fn totals(&self) -> (usize, usize) {
        let mut changes = 0usize;
        let mut data_bytes = 0usize;
        for shard in &self.shards {
            for slot_changes in Self::lock(shard).values() {
                changes += slot_changes.len();
                data_bytes += slot_changes
                    .values()
                    .map(|c| c.account.data.len())
                    .sum::<usize>();
            }
        }
        (changes, data_bytes)
    }
}

impl Clone for PendingAccountChanges {
    fn clone(&self) -> Self {
        PendingAccountChanges {
            shards: self
                .shards
                .iter()
                .map(|shard| Mutex::new(Self::lock(shard).clone()))
                .collect(),
            slots: Mutex::new(Self::lock(&self.slots).clone()),
        }
    }
}

/// One shard per value of the pubkey byte picked by `ShardedMap::shard`.
const SHARDS: usize = 256;

/// Map keyed by pubkey, split in shards that grow one at a time. Growing a hash map briefly
/// holds both its old and new tables, which for a single map of every account is tens of GB.
#[derive(Clone)]
pub struct ShardedMap<V> {
    shards: Vec<HashMap<[u8; 32], V>>,
}

impl<V> Default for ShardedMap<V> {
    fn default() -> Self {
        ShardedMap {
            shards: (0..SHARDS).map(|_| HashMap::default()).collect(),
        }
    }
}

impl<V> ShardedMap<V> {
    #[inline]
    fn shard(key: &[u8; 32]) -> usize {
        // A middle byte: vanity pubkeys share their first bytes (base58 prefix) and suffixes
        // like pump.fun's "...pump" fix the last few, but the middle bytes stay uniform.
        key[16] as usize
    }

    #[inline]
    pub fn get(&self, key: &[u8; 32]) -> Option<&V> {
        self.shards[Self::shard(key)].get(key)
    }

    #[inline]
    pub fn get_mut(&mut self, key: &[u8; 32]) -> Option<&mut V> {
        self.shards[Self::shard(key)].get_mut(key)
    }

    #[inline]
    pub fn insert(&mut self, key: [u8; 32], value: V) -> Option<V> {
        self.shards[Self::shard(&key)].insert(key, value)
    }

    #[inline]
    pub fn remove(&mut self, key: &[u8; 32]) -> Option<V> {
        self.shards[Self::shard(key)].remove(key)
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(HashMap::len).sum()
    }

    /// Converts one shard at a time, dropping each source shard once converted, so the
    /// conversion never holds more than one extra shard.
    fn convert<W>(self, convert: impl Fn(V) -> Option<W>) -> ShardedMap<W> {
        ShardedMap {
            shards: self
                .shards
                .into_iter()
                .map(|shard| {
                    let mut converted = HashMap::default();
                    converted.reserve(shard.len());
                    converted.extend(
                        shard
                            .into_iter()
                            .filter_map(|(key, value)| Some((key, convert(value)?))),
                    );
                    converted.shrink_to_fit();
                    converted
                })
                .collect(),
        }
    }
}

/// Packed to 12 bytes: the cache holds one per account on the chain.
#[derive(Clone, Copy)]
#[repr(C, packed)]
struct CachedAccount {
    owner_id: u32,
    data_hash: u64,
}

/// `StartupAccount::owner_id` of an account with a startup version but no cached data hash.
const NO_OWNER: u32 = u32::MAX;

/// Cache entry during startup, which also holds the newest snapshot version seen for the
/// account so the pubkey is stored once instead of in a second map.
#[derive(Clone, Copy)]
#[repr(C, packed)]
struct StartupAccount {
    owner_id: u32,
    data_hash: u64,
    version: u64,
    has_version: bool,
}

#[derive(Clone)]
enum Accounts {
    Startup {
        accounts: ShardedMap<StartupAccount>,
        cached: usize,
        versions: usize,
    },
    Running(ShardedMap<CachedAccount>),
}

/// Owner and data hash of the last sent version of every account, keyed by pubkey. An account
/// has one owner at a time, so a data hash is only returned when the owner asked for matches.
/// Owners are programs, few enough to be stored once and referenced by index.
///
/// Until `end_startup`, it also tracks the newest snapshot version received per account.
#[derive(Clone)]
pub struct AccountCache {
    accounts: Accounts,
    owners: Vec<[u8; 32]>,
    owner_ids: HashMap<[u8; 32], u32>,
}

impl Default for AccountCache {
    fn default() -> Self {
        AccountCache {
            accounts: Accounts::Startup {
                accounts: ShardedMap::default(),
                cached: 0,
                versions: 0,
            },
            owners: Vec::new(),
            owner_ids: HashMap::default(),
        }
    }
}

impl AccountCache {
    fn owner_id(&mut self, owner: [u8; 32]) -> u32 {
        if let Some(&id) = self.owner_ids.get(&owner) {
            return id;
        }
        let id = u32::try_from(self.owners.len())
            .ok()
            .filter(|&id| id != NO_OWNER)
            .expect("more than u32::MAX - 1 distinct owners");
        self.owners.push(owner);
        self.owner_ids.insert(owner, id);
        id
    }

    /// Owner id and data hash of the account, in one lookup.
    #[inline]
    fn get_ids(&self, address: &[u8; 32]) -> Option<(u32, u64)> {
        match &self.accounts {
            Accounts::Running(accounts) => {
                let cached = *accounts.get(address)?;
                Some((cached.owner_id, cached.data_hash))
            }
            Accounts::Startup { accounts, .. } => {
                let cached = *accounts.get(address)?;
                if cached.owner_id == NO_OWNER {
                    None
                } else {
                    Some((cached.owner_id, cached.data_hash))
                }
            }
        }
    }

    /// Owner and data hash of the account, in one lookup.
    #[inline]
    pub fn get(&self, address: &[u8; 32]) -> Option<(&[u8; 32], u64)> {
        let (owner_id, data_hash) = self.get_ids(address)?;
        Some((&self.owners[owner_id as usize], data_hash))
    }

    #[inline]
    pub fn owner(&self, address: &[u8; 32]) -> Option<&[u8; 32]> {
        self.get(address).map(|(owner, _)| owner)
    }

    #[inline]
    pub fn data_hash(&self, owner: &[u8; 32], address: &[u8; 32]) -> Option<u64> {
        match self.get(address)? {
            (cached_owner, data_hash) if cached_owner == owner => Some(data_hash),
            _ => None,
        }
    }

    pub fn insert(&mut self, owner: [u8; 32], address: [u8; 32], data_hash: u64) {
        let owner_id = self.owner_id(owner);
        match &mut self.accounts {
            Accounts::Running(accounts) => {
                accounts.insert(
                    address,
                    CachedAccount {
                        owner_id,
                        data_hash,
                    },
                );
            }
            Accounts::Startup {
                accounts, cached, ..
            } => match accounts.get_mut(&address) {
                Some(entry) => {
                    if entry.owner_id == NO_OWNER {
                        *cached += 1;
                    }
                    entry.owner_id = owner_id;
                    entry.data_hash = data_hash;
                }
                None => {
                    *cached += 1;
                    accounts.insert(
                        address,
                        StartupAccount {
                            owner_id,
                            data_hash,
                            version: 0,
                            has_version: false,
                        },
                    );
                }
            },
        }
    }

    /// Removes the account only if it is cached under `owner`: a deletion reported under a
    /// previous owner must not drop the entry of the current one.
    pub fn remove(&mut self, owner: &[u8; 32], address: &[u8; 32]) {
        if self.owner(address) == Some(owner) {
            self.remove_address(address);
        }
    }

    pub fn remove_address(&mut self, address: &[u8; 32]) {
        match &mut self.accounts {
            Accounts::Running(accounts) => {
                accounts.remove(address);
            }
            Accounts::Startup {
                accounts, cached, ..
            } => {
                let Some(entry) = accounts.get_mut(address) else {
                    return;
                };
                if entry.owner_id == NO_OWNER {
                    return;
                }
                *cached -= 1;
                if entry.has_version {
                    entry.owner_id = NO_OWNER;
                    entry.data_hash = 0;
                } else {
                    accounts.remove(address);
                }
            }
        }
    }

    /// Records a snapshot version of the account and caches it, unless a version at least as
    /// new was already received.
    pub fn set_on_startup(
        &mut self,
        owner: [u8; 32],
        address: [u8; 32],
        data_hash: u64,
        version: u64,
        deleted: bool,
    ) {
        if let Accounts::Running(_) = self.accounts {
            self.restart_startup();
        }
        let Accounts::Startup {
            accounts, versions, ..
        } = &mut self.accounts
        else {
            unreachable!("restart_startup switches to the startup layout");
        };

        match accounts.get_mut(&address) {
            Some(entry) if entry.has_version && entry.version >= version => return,
            Some(entry) => {
                if !entry.has_version {
                    *versions += 1;
                }
                entry.version = version;
                entry.has_version = true;
            }
            None => {
                *versions += 1;
                accounts.insert(
                    address,
                    StartupAccount {
                        owner_id: NO_OWNER,
                        data_hash: 0,
                        version,
                        has_version: true,
                    },
                );
            }
        }

        if deleted {
            self.remove_address(&address);
        } else {
            self.insert(owner, address, data_hash);
        }
    }

    /// Drops the startup versions and switches to the smaller running layout.
    pub fn end_startup(&mut self) {
        let accounts =
            std::mem::replace(&mut self.accounts, Accounts::Running(ShardedMap::default()));
        if let Accounts::Startup { accounts, .. } = accounts {
            self.accounts = Accounts::Running(accounts.convert(|entry| {
                if entry.owner_id == NO_OWNER {
                    None
                } else {
                    Some(CachedAccount {
                        owner_id: entry.owner_id,
                        data_hash: entry.data_hash,
                    })
                }
            }));
        } else {
            self.accounts = accounts;
        }
    }

    /// Snapshot notifications after `end_startup` start tracking versions again from none.
    fn restart_startup(&mut self) {
        let accounts =
            std::mem::replace(&mut self.accounts, Accounts::Running(ShardedMap::default()));
        if let Accounts::Running(accounts) = accounts {
            let cached = accounts.len();
            self.accounts = Accounts::Startup {
                accounts: accounts.convert(|entry| {
                    Some(StartupAccount {
                        owner_id: entry.owner_id,
                        data_hash: entry.data_hash,
                        version: 0,
                        has_version: false,
                    })
                }),
                cached,
                versions: 0,
            };
        } else {
            self.accounts = accounts;
        }
    }

    /// Number of accounts with a startup version, zero after `end_startup`.
    pub fn startup_versions(&self) -> usize {
        match &self.accounts {
            Accounts::Startup { versions, .. } => *versions,
            Accounts::Running(_) => 0,
        }
    }

    pub fn len(&self) -> usize {
        match &self.accounts {
            Accounts::Startup { cached, .. } => *cached,
            Accounts::Running(accounts) => accounts.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

use crate::pb::sf::solana::r#type::v1::{Block, BlockHeight, Reward, UnixTimestamp};
use crate::plugins::{to_block_rewards, ConfirmTransactionWithIndex};
use bs58;
use log::{debug, error, info, warn};
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_transaction_status::TransactionDetails;

#[derive(Debug, Clone, PartialEq)]
pub struct AccountFixed {
    pub address: [u8; 32],
    pub owner: [u8; 32],
    pub data: Vec<u8>,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AccountWithWriteVersion {
    pub account: AccountFixed,
    pub write_version: u64,
    pub data_hash: u64,
}

lazy_static! {
    pub static ref BLOCK_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());
    pub static ref ACC_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());
    pub static ref CURSOR_MUTEX: std::sync::Mutex<u64> = std::sync::Mutex::new(0);
}

#[derive(Default, Clone)]
pub struct BlockInfo {
    pub slot: u64,
    pub parent_slot: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub timestamp: Timestamp,
    pub height: Option<u64>,
    pub rewards: Vec<Reward>,
    pub transaction_count: u64,
}

const DEFAULT_RPC_BLOCK_CONFIG: RpcBlockConfig = RpcBlockConfig {
    encoding: None,
    transaction_details: Some(TransactionDetails::Signatures),
    rewards: Some(true),
    commitment: Some(CommitmentConfig::confirmed()),
    // Only enforced by getBlock when transaction_details asks for Full or Accounts.
    max_supported_transaction_version: Some(1),
};

pub struct State {
    pub initialized: bool, // passed the first received blockmeta

    pub first_received_blockmeta: Option<u64>,
    pub first_block_to_process: Option<u64>,

    pub last_sent_block: Option<u64>,

    pub cursor: Option<u64>,
    pub lib: Option<u64>,

    pub block_account_changes: PendingAccountChanges,

    pub account_cache: AccountCache, // only updated when we print the block

    pub block_infos: BlockInfoMap,
    pub confirmed_slots: ConfirmedSlotsMap,

    pub with_block: bool,
    //with_account: bool,
    /// Filled under the shared state lock by `try_set_transaction`.
    pub transactions: Mutex<Transactions>,
    pub processed_slots: ProcessedSlot,

    pub cursor_path: String,
    pub dev_config: DevelopmentConfig,

    /// Slot of the last `memory stats` log line.
    pub last_stats_slot: u64,
    /// Transactions received for a slot at or below `last_sent_block`, which are never sent.
    pub late_transactions: AtomicU64,
    /// Account updates received for a slot at or below `last_sent_block`.
    pub late_account_updates: AtomicU64,

    local_rpc_client: Option<RpcClient>,
    remote_rpc_client: Option<RpcClient>,
    block_printer: BlockPrinter,
}

const MEMORY_STATS_INTERVAL_SLOTS: u64 = 100;

impl State {
    pub fn new(
        local_rpc_client: RpcClient,
        remote_rpc_client: RpcClient,
        cursor: Option<u64>,
        cursor_path: String,
        block_printer: BlockPrinter,
        with_block: bool,
        dev_config: DevelopmentConfig,
    ) -> Self {
        State {
            cursor,
            first_block_to_process: if dev_config.force_send { Some(0) } else { None },
            first_received_blockmeta: if dev_config.force_send { Some(0) } else { None },
            lib: None,
            initialized: false,

            block_account_changes: PendingAccountChanges::default(),
            account_cache: AccountCache::default(),
            block_infos: HashMap::default(),
            confirmed_slots: HashMap::default(),
            last_sent_block: None,

            transactions: Mutex::new(HashMap::default()),
            processed_slots: HashMap::default(),

            last_stats_slot: 0,
            late_transactions: AtomicU64::new(0),
            late_account_updates: AtomicU64::new(0),

            local_rpc_client: Some(local_rpc_client),
            remote_rpc_client: Some(remote_rpc_client),
            cursor_path,
            block_printer,
            with_block,
            dev_config,
        }
    }

    /// Internal copy for testing purposes, cannot clone RpcClient nor
    /// BlockPrinter which are set respectively to None and a new instance.
    pub(crate) fn internal_copy(&self) -> State {
        State {
            initialized: self.initialized,
            first_received_blockmeta: self.first_received_blockmeta,
            first_block_to_process: self.first_block_to_process,
            last_sent_block: self.last_sent_block,
            cursor: self.cursor,
            lib: self.lib,
            block_account_changes: self.block_account_changes.clone(),
            account_cache: self.account_cache.clone(),
            block_infos: self.block_infos.clone(),
            confirmed_slots: self.confirmed_slots.clone(),
            with_block: self.with_block,
            transactions: Mutex::new(self.transactions.lock().unwrap().clone()),
            processed_slots: self.processed_slots.clone(),
            cursor_path: self.cursor_path.clone(),
            dev_config: self.dev_config.clone(),
            last_stats_slot: self.last_stats_slot,
            late_transactions: AtomicU64::new(self.late_transactions.load(AtomicOrdering::Relaxed)),
            late_account_updates: AtomicU64::new(
                self.late_account_updates.load(AtomicOrdering::Relaxed),
            ),

            // Cannot clone those
            local_rpc_client: None,
            remote_rpc_client: None,
            block_printer: BlockPrinter::new(None, None, true),
        }
    }

    fn set_last_finalized_block_from_rpc(&mut self) {
        let commitment_config = CommitmentConfig::finalized();
        match self
            .local_rpc_client
            .as_ref()
            .expect("local_rpc_client not set")
            .get_slot_with_commitment(commitment_config)
        {
            Ok(lib_num) => {
                info!("Block lib received from rpc client: {}", lib_num);
                self.lib = Some(lib_num);
                if let Some(cursor) = self.cursor {
                    if lib_num > cursor {
                        info!(
                            "ignoring cursor {} because LIB {} is greater",
                            cursor, lib_num
                        );
                        self.cursor = None;
                        self.first_block_to_process = None; // it would have been set by the cursor, we get rid of it too
                    }
                }
            }
            Err(e) => {
                info!("Error getting lib num from rpc client: {}", e);
            }
        }
    }

    pub fn set_lib(&mut self, slot: u64) {
        self.lib = Some(slot);
    }

    fn get_lib(&self) -> Option<u64> {
        self.lib
    }

    pub fn cache_block_from_rpc(&mut self, slot: u64, trace: bool) {
        let started = std::time::Instant::now();
        self.fetch_block_from_rpc(slot, trace);
        crate::stats::RPC_BLOCK_FETCH.record_since(started);
    }

    fn fetch_block_from_rpc(&mut self, slot: u64, trace: bool) {
        match self
            .local_rpc_client
            .as_ref()
            .expect("local_rpc_client not set")
            .get_block_with_config(slot, DEFAULT_RPC_BLOCK_CONFIG)
        {
            Ok(block) => {
                debug!("Block Info fetched locally for slot {}", slot);
                self.set_block_info(
                    BlockInfo {
                        timestamp: convert_sol_timestamp(block.block_time.unwrap_or_default()),
                        parent_slot: block.parent_slot.clone(),
                        slot,
                        block_hash: block.blockhash.clone(),
                        parent_hash: block.previous_blockhash.clone(),
                        height: block.block_height,
                        rewards: to_block_rewards(&block.rewards),
                        transaction_count: block.transactions.unwrap_or_default().len() as u64,
                    },
                    trace,
                )
            }
            Err(_err) => {
                match self
                    .remote_rpc_client
                    .as_ref()
                    .expect("remote_rpc_client not set")
                    .get_block_with_config(slot, DEFAULT_RPC_BLOCK_CONFIG)
                {
                    Ok(block) => {
                        debug!("Block Info fetched remotely for slot {}", slot);
                        self.set_block_info(
                            BlockInfo {
                                timestamp: convert_sol_timestamp(
                                    block.block_time.unwrap_or_default(),
                                ),
                                parent_slot: block.parent_slot.clone(),
                                slot,
                                block_hash: block.blockhash.clone(),
                                parent_hash: block.previous_blockhash.clone(),
                                height: block.block_height,
                                rewards: to_block_rewards(&block.rewards),
                                transaction_count: block.transactions.unwrap_or_default().len()
                                    as u64,
                            },
                            trace,
                        )
                    }
                    Err(_err) => return,
                }
            }
        }
    }

    pub fn ordered_confirmed_slots_upto(&self, slot: u64) -> Vec<u64> {
        // Collect all keys from confirmed_slots that are less than the given slot
        let mut slots: Vec<u64> = self
            .confirmed_slots
            .keys()
            .cloned()
            .filter(|&x| x <= slot)
            .collect();
        slots.sort();
        slots
    }

    fn add_missing_slots_to_confirmed_slots(
        &mut self,
        last_sent: u64,
        parent_slot: u64,
        trace: bool,
    ) -> bool {
        let mut i = parent_slot;
        while i > last_sent {
            match self.block_infos.get(&i) {
                Some(bi) => {
                    if self.confirmed_slots.insert(i, true).is_none() {
                        info!("added missing slot {} to confirmed_slots", i);
                    };
                    i = bi.parent_slot;
                }
                None => {
                    self.cache_block_from_rpc(i, trace);
                    match self.block_infos.get(&i) {
                        Some(bi) => {
                            if self.confirmed_slots.insert(i, true).is_none() {
                                info!("added missing slot {} to confirmed_slots", i);
                            };
                            i = bi.parent_slot;
                        }
                        None => {
                            warn!("Failed to get block info for slot {} while adding missing slots to confirmed_slots", i);
                            return false;
                        }
                    }
                }
            }
        }
        i == last_sent
    }

    // should_skip_slot skips when not initialized and below target block

    pub fn set_confirmed_slot(&mut self, slot: u64, trace: bool) {
        if let Some(cursor) = self.cursor {
            if self.first_block_to_process.is_none() {
                if slot >= cursor {
                    self.first_block_to_process = Some(slot);
                    info!("setting first_block_to_process: {}", slot - 1);
                }
            }
        }
        debug!("set_confirmed_slot: {}", slot);
        self.confirmed_slots.insert(slot, true);

        if self.is_ready(slot) {
            if let Err(e) = self.process_upto(trace, slot) {
                panic!(
                    "process_upto failed after set_confirmed_slot({}): {}",
                    slot, e
                )
            }
        }
    }

    pub fn has_block_info(&self, slot: u64) -> bool {
        self.block_infos.get(&slot).is_some()
    }

    pub fn is_ready(&self, slot: u64) -> bool {
        if self.confirmed_slots.get(&slot).is_none() {
            return false;
        }
        match self.block_infos.get(&slot) {
            None => false,
            Some(blk) => {
                if !self.with_block {
                    return true; // if we only track account changes, we don't need to count the transactions
                }
                if let Some(trxs) = self.transactions.lock().unwrap().get(&slot) {
                    if blk.transaction_count == trxs.len() as u64 {
                        true
                    } else {
                        debug!(
                            "slot {} has {} transactions, but {} were received, waiting for more",
                            slot,
                            blk.transaction_count,
                            trxs.len()
                        );
                        {
                            false
                        }
                    }
                } else {
                    if blk.transaction_count == 0 {
                        return true;
                    }
                    debug!(
                        "slot {} has no transactions, but is confirmed, waiting for transactions",
                        slot
                    );
                    false
                }
            }
        }
    }

    pub fn set_block_info(&mut self, block_info: BlockInfo, trace: bool) {
        let slot = block_info.slot;
        if slot >= self.last_stats_slot + MEMORY_STATS_INTERVAL_SLOTS {
            self.last_stats_slot = slot;
            self.log_memory_stats(slot);
        }
        if self.lib.is_none() {
            // this may set the cursor to none
            self.set_last_finalized_block_from_rpc();
        }
        if self.first_received_blockmeta.is_none() {
            self.first_received_blockmeta = Some(slot);
            info!(
                "first blockmeta received at slot {}, cursor={:?}, lib={:?}, account_hash_count={}",
                slot,
                self.cursor,
                self.lib,
                self.account_cache.len()
            );
            if self.cursor.is_none() {
                // usually because the lib has been set from rpc
                info!("setting first_block_to_process to: {} (no cursor)", slot);
                self.first_block_to_process = Some(slot);

                if slot != 0 {
                    // since we don't send these blocks, we apply their changes to the cache manually
                    self.apply_changes_upto(trace, slot - 1);

                    info!("purging blocks up to {} after first blockmeta", slot - 1);
                    self.purge_blocks_up_to(slot - 1);
                }
            }
        }
        debug!(
            "setting block info for slot {}, hash {}",
            slot, block_info.block_hash
        );
        self.block_infos.insert(slot, block_info);

        // if we get block_info for block 25, but we have 'confirmed blocks' 20 to 24, we'll fetch their block_info from RPC, which is a bit costly but prevents being stuck forever. This happens in rare cases, mostly upon startup
        for slot in self.ordered_confirmed_slots_upto(slot) {
            if !self.has_block_info(slot) {
                self.cache_block_from_rpc(slot, trace);
            }
        }

        if self.is_ready(slot) {
            if let Err(e) = self.process_upto(trace, slot) {
                panic!("process_upto failed after set_block_info({}): {}", slot, e)
            }
        }
    }

    // set_account_on_startup populates the account caches on startup
    pub fn set_account_on_startup(
        &mut self,
        pub_key: &[u8],
        owner: &[u8],
        data_hash: u64,
        slot: u64,
        write_version: u64,
        deleted: bool,
    ) {
        // Convert to fixed-length arrays (Solana pubkeys are always 32 bytes)
        let mut pub_key_fixed = [0u8; 32];
        let mut owner_fixed = [0u8; 32];

        // SAFETY: Both pub_key and owner are guaranteed to be 32 bytes (Solana pubkey size)
        // and pub_key_fixed/owner_fixed are also 32 bytes, so this is safe
        unsafe {
            std::ptr::copy_nonoverlapping(pub_key.as_ptr(), pub_key_fixed.as_mut_ptr(), 32);
            std::ptr::copy_nonoverlapping(owner.as_ptr(), owner_fixed.as_mut_ptr(), 32);
        }

        // Using left shift by 25 bits to pack slot and write_version into a u64:
        // 1) Assumed max write_version: 2^25 - 1 = 33,554,431 (~33.5M)
        //    This is well above the observed max of ~10K write_versions per slot
        // 2) Max slot considering u64: (2^64 - 1) >> 25 = 2^39 - 1 = 549,755,813,887 (~549B slots)
        //    At 400ms per slot, this supports ~6,900 years of blockchain history
        let composite_value = (slot << 25) | write_version;

        // The newest startup version wins whatever owner it has, including when it deletes
        self.account_cache.set_on_startup(
            owner_fixed,
            pub_key_fixed,
            data_hash,
            composite_value,
            deleted,
        );
    }

    pub fn delete_startup_info(&mut self) {
        self.account_cache.end_startup();
    }

    // set_account populates the caches for set_account
    /// Takes the data by value so callers can copy it before taking the state lock.
    pub fn set_account(
        &mut self,
        slot: u64,
        pub_key: &[u8],
        data: impl Into<Vec<u8>>,
        owner: &[u8],
        write_version: u64,
        deleted: bool,
        data_hash: u64,
        trace: bool,
    ) {
        // purge tail data on initialization
        if !self.block_account_changes.contains_slot(slot) {
            let initializing = self.cursor.is_none() && self.first_block_to_process.is_none();
            if initializing {
                debug!("initializing: deleting blocks up to: {}", slot - 32);
                self.purge_blocks_up_to(slot - 32);
            }
        }
        self.record_account(
            slot,
            pub_key,
            data.into(),
            owner,
            write_version,
            deleted,
            data_hash,
            trace,
        );
    }

    /// `set_account` under the shared state lock, so account updates from several threads run
    /// in parallel. Gives the data back when the update needs `set_account` instead, which is
    /// only while initializing, for the first change of a slot.
    pub fn try_set_account(
        &self,
        slot: u64,
        pub_key: &[u8],
        data: Vec<u8>,
        owner: &[u8],
        write_version: u64,
        deleted: bool,
        data_hash: u64,
        trace: bool,
    ) -> Result<(), Vec<u8>> {
        let initializing = self.cursor.is_none() && self.first_block_to_process.is_none();
        if initializing && !self.block_account_changes.contains_slot(slot) {
            return Err(data);
        }
        self.record_account(
            slot,
            pub_key,
            data,
            owner,
            write_version,
            deleted,
            data_hash,
            trace,
        );
        Ok(())
    }

    fn record_account(
        &self,
        slot: u64,
        pub_key: &[u8],
        data: Vec<u8>,
        owner: &[u8],
        write_version: u64,
        deleted: bool,
        data_hash: u64,
        trace: bool,
    ) {
        if let Some(last_sent) = self.last_sent_block {
            if last_sent >= slot {
                self.late_account_updates
                    .fetch_add(1, AtomicOrdering::Relaxed);
                error!("Received account data for slot {} which is older than the last sent block {} (owner: {:?}, account: {:?})", slot, last_sent,  bs58::encode(pub_key).into_string(), bs58::encode(owner).into_string());
            }
        }

        //create a unique key from owner and account addresses
        let mut owner_account_key = [0u8; 64];
        owner_account_key[..32].copy_from_slice(&owner[..32]);
        owner_account_key[32..].copy_from_slice(&pub_key[..32]);

        let mut address = [0u8; 32];
        let mut owner_array = [0u8; 32];
        address.copy_from_slice(&pub_key[..32]);
        owner_array.copy_from_slice(&owner[..32]);

        let awv = AccountWithWriteVersion {
            account: AccountFixed {
                address,
                owner: owner_array,
                data,
                deleted,
            },
            write_version,
            data_hash,
        };

        self.block_account_changes
            .insert(slot, owner_account_key, awv, trace);
    }

    pub fn set_transaction(
        &mut self,
        slot: u64,
        transaction: ConfirmTransactionWithIndex,
        trace: bool,
    ) {
        self.record_transaction(slot, transaction);

        if self.is_ready(slot) {
            if let Err(e) = self.process_upto(trace, slot) {
                panic!(
                    "process_upto failed after set_transaction(slot={}): {}",
                    slot, e
                )
            }
        }
    }

    /// `set_transaction` under the shared state lock, so transactions from several threads
    /// are recorded in parallel. Gives the transaction back when its slot is already
    /// confirmed: it may complete the block, which `set_transaction` then sends.
    pub fn try_set_transaction(
        &self,
        slot: u64,
        transaction: ConfirmTransactionWithIndex,
    ) -> Result<(), ConfirmTransactionWithIndex> {
        if self.confirmed_slots.contains_key(&slot) {
            return Err(transaction);
        }
        self.record_transaction(slot, transaction);
        Ok(())
    }

    fn record_transaction(&self, slot: u64, transaction: ConfirmTransactionWithIndex) {
        if self.processed_slots.get(&slot).is_some() {
            error!(
                "slot {} already processed should not receive transaction for it",
                slot
            );
        }
        if self
            .last_sent_block
            .is_some_and(|last_sent| slot <= last_sent)
        {
            self.late_transactions.fetch_add(1, AtomicOrdering::Relaxed);
        }

        let mut transactions = self.transactions.lock().unwrap();
        if let Some(txs) = transactions.get_mut(&slot) {
            txs.push(transaction);
        } else {
            debug!("inserting first transaction for slot {}", slot);
            transactions.insert(slot, vec![transaction]);
        }
    }

    fn purge_blocks_up_to(&mut self, upto: u64) {
        for block in self.block_account_changes.purge_up_to(upto) {
            self.block_infos.remove(&block);
        }

        let slots = self.confirmed_slots.keys().cloned().collect::<Vec<u64>>();
        for slot in slots {
            if slot <= upto {
                debug!("purging confirmed slot {}", slot);
                self.confirmed_slots.remove(&slot);
                if upto > 100 {
                    let processed_slot_remove = upto - 100;
                    self.processed_slots.remove(&processed_slot_remove);
                }
            }
        }

        // A slot at or below the LIB that is not confirmed by now never will be, and
        // `add_missing_slots_to_confirmed_slots` only walks back to the last sent block, so
        // the transactions of fork slots below both are never read.
        if let (Some(lib), Some(last_sent)) = (self.lib, self.last_sent_block) {
            let cutoff = lib.min(last_sent);
            self.transactions
                .get_mut()
                .unwrap()
                .retain(|&slot, _| slot > cutoff);
        }
    }

    fn apply_changes_upto(&mut self, trace: bool, slot: u64) {
        // Find the lowest slot value in block_account_changes keys
        let first_slot = self.block_account_changes.min_slot().unwrap_or(slot);

        if first_slot > slot {
            info!(
                "applying account cache changes: empty range (first_slot={} > upto={})",
                first_slot, slot
            );
            return;
        }

        let started = std::time::Instant::now();
        let pending_slots = self.block_account_changes.slots_between(first_slot, slot);
        info!(
            "applying account cache changes for slots from {} to: {} ({} slots with pending account changes, hash_count={})",
            first_slot,
            slot,
            pending_slots,
            self.account_cache.len()
        );

        let mut slots_applied = 0u64;
        let mut changes_applied = 0usize;
        // Loop through each slot from first_slot to slot (inclusive)
        for current_slot in first_slot..=slot {
            let slot_changes: Vec<&AccountWithWriteVersion> = self
                .block_account_changes
                .slot_changes(current_slot)
                .collect();
            let (_, changes) =
                filter_account_changes(&slot_changes, &self.account_cache, current_slot, trace);
            changes_applied += changes.len();
            self.apply_cache_changes(changes);
            slots_applied += 1;
        }

        info!(
            "finished applying account cache changes for slots {}..={} ({} slots, {} state changes) in {:?} (hash_count={})",
            first_slot,
            slot,
            slots_applied,
            changes_applied,
            started.elapsed(),
            self.account_cache.len()
        );
    }

    pub fn process_upto(
        &mut self,
        trace: bool,
        slot: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("processing upto slot {}", slot);
        let first_block_to_process = match self.first_block_to_process {
            Some(slot) => slot,
            None => {
                debug!(
                    "No 'first_block_to_process' yet, skipping processing for slot {}",
                    slot
                );
                return Ok(());
            }
        };

        if self.first_received_blockmeta.is_none() {
            debug!(
                "No 'first_received_blockmeta' yet, skipping processing for slot {}",
                slot
            );
            return Ok(());
        }

        let lib = match self.get_lib() {
            Some(lib) => lib,
            None => {
                debug!("No 'lib' yet, skipping processing for slot {}", slot);
                return Ok(());
            }
        };

        let is_first_send = self.last_sent_block.is_none();
        if is_first_send {
            info!(
                "process_upto first-send path: upto={} first_block_to_process={} lib={} first_blockmeta={:?} confirmed_slots={} account_hash_count={}",
                slot,
                first_block_to_process,
                lib,
                self.first_received_blockmeta,
                self.confirmed_slots.len(),
                self.account_cache.len()
            );
            if slot != 0 {
                self.apply_changes_upto(trace, slot - 1);
            }
            info!("process_upto: first-send init complete, marking initialized");
            self.initialized = true;
        }

        let confirmed_slots = self.ordered_confirmed_slots_upto(slot);
        if is_first_send {
            info!(
                "process_upto: considering {} confirmed slot(s) up to {}: {:?}",
                confirmed_slots.len(),
                slot,
                if confirmed_slots.len() <= 20 {
                    format!("{:?}", confirmed_slots)
                } else {
                    format!(
                        "[{}..{}] ({} total)",
                        confirmed_slots.first().copied().unwrap_or(0),
                        confirmed_slots.last().copied().unwrap_or(0),
                        confirmed_slots.len()
                    )
                }
            );
        }

        for slot in confirmed_slots {
            let must_send = slot >= first_block_to_process || self.dev_config.force_send;

            let block_info = match self.block_infos.get(&slot) {
                None => {
                    if must_send {
                        info!("No block info for slot {} in process_upto", slot);
                        return Ok(());
                    };
                    &BlockInfo::default()
                }
                Some(bi) => bi,
            };

            if let Some(last_sent_block) = self.last_sent_block {
                if last_sent_block < block_info.parent_slot {
                    warn!(
                            "last sent block {} is not the parent of slot {}. Expecting {}. (This is a very rare case that would create a hole). Manually adding missing slots to 'confirmed_slots', they will be sent on next loop",
                            last_sent_block,
                            slot,
                            block_info.parent_slot,
                        );

                    let success =
                        self.add_missing_slots_to_confirmed_slots(last_sent_block, slot, trace);
                    if !success {
                        warn!("Failed to add all missing slots to 'confirmed_slots' between {} and {}", last_sent_block, slot);
                    }
                    break; //
                } else if last_sent_block != slot - 1 {
                    warn!("dropped slots from {} to {}", last_sent_block + 1, slot - 1);
                }
            }

            // Purged right after this slot is processed, so its data can move into the block
            let slot_changes = self.block_account_changes.take_slot_changes(slot);
            let (effective_account_changes, cache_changes) = filter_account_changes(
                &slot_changes.iter().collect::<Vec<_>>(),
                &self.account_cache,
                slot,
                trace,
            );

            if must_send {
                let tx_count = self
                    .transactions
                    .get_mut()
                    .unwrap()
                    .get(&slot)
                    .map(|t| t.len())
                    .unwrap_or(0);
                info!(
                    "process_upto: preparing to send slot {} (account_changes={}, txs={}, parent={}, lib={})",
                    slot,
                    effective_account_changes.len(),
                    tx_count,
                    block_info.parent_slot,
                    lib
                );

                let acc_block = create_account_block(
                    filtered_accounts(&effective_account_changes, slot_changes),
                    &block_info,
                );

                let mut transactions_with_index = self
                    .transactions
                    .get_mut()
                    .unwrap()
                    .remove(&slot)
                    .unwrap_or_else(|| vec![]);

                transactions_with_index.sort_by_key(|ti| ti.index);

                let block = compose_and_purge_block(slot, &block_info, transactions_with_index);

                let printer = &mut self.block_printer;
                let result = printer.print(&block_info, lib, block, acc_block, &self.cursor_path);
                if let Err(e) = result {
                    error!("Error printing block at {}: {}", slot, e);
                    return Err(format!("Error printing block at {}: {}", slot, e).into());
                }
                info!("process_upto: print scheduled for slot {}", slot);
                self.last_sent_block = Some(block_info.slot);
            } else {
                info!(
                    "in process_upto, not actually sending slot {}, below first_block_to_process {}. applying to cache only",
                    slot, first_block_to_process
                );
            }
            self.apply_cache_changes(cache_changes);
            self.processed_slots.insert(slot, true);
            self.purge_blocks_up_to(slot);

            if BLOCK_MUTEX.is_poisoned() || ACC_MUTEX.is_poisoned() {
                error!(
                    "process_upto: output mutex poisoned after slot {} (BLOCK_MUTEX poisoned={}, ACC_MUTEX poisoned={})",
                    slot,
                    BLOCK_MUTEX.is_poisoned(),
                    ACC_MUTEX.is_poisoned()
                );
                return Err(format!(
                    "mutex poisoned after processing slot {} (block={}, acc={})",
                    slot,
                    BLOCK_MUTEX.is_poisoned(),
                    ACC_MUTEX.is_poisoned()
                )
                .into());
            }
        }
        if is_first_send {
            info!(
                "process_upto first-send path done: last_sent_block={:?}",
                self.last_sent_block
            );
        }
        Ok(())
    }

    /// Logs the size of every map the plugin keeps between notifications, plus the
    /// fifo writes still in flight, so memory growth can be attributed to one of them.
    pub fn log_memory_stats(&self, slot: u64) {
        use crate::stats::{
            NOTIFY_BLOCK_METADATA, NOTIFY_TRANSACTION, PENDING_ACCOUNT_BLOCK_WRITES,
            PENDING_BLOCK_WRITES, PENDING_WRITE_BYTES, RPC_BLOCK_FETCH, STATE_LOCK_WAIT,
            UPDATE_ACCOUNT, UPDATE_SLOT_STATUS,
        };
        use std::sync::atomic::Ordering;

        let (account_changes, account_data_bytes) = self.block_account_changes.totals();

        // Counts only: this runs under the state lock
        let pending_transactions = self.transactions.lock().unwrap();
        let transactions: usize = pending_transactions.values().map(Vec::len).sum();
        let stale_transaction_slots = self.last_sent_block.map_or(0, |last_sent| {
            pending_transactions
                .keys()
                .filter(|&&s| s <= last_sent)
                .count()
        });

        info!(
            "memory stats at slot {}: last_sent_block={:?} lib={:?} \
             block_account_changes(slots={} min_slot={:?} accounts={} data_bytes={}) \
             transactions(slots={} min_slot={:?} stale_slots={} count={}) \
             block_infos={} confirmed_slots={} processed_slots={} \
             account_cache={} startup_received_slot={} \
             late_transactions={} late_account_updates={} \
             pending_writes(blocks={} account_blocks={} base64_bytes={})",
            slot,
            self.last_sent_block,
            self.lib,
            self.block_account_changes.slot_count(),
            self.block_account_changes.min_slot(),
            account_changes,
            account_data_bytes,
            pending_transactions.len(),
            pending_transactions.keys().min(),
            stale_transaction_slots,
            transactions,
            self.block_infos.len(),
            self.confirmed_slots.len(),
            self.processed_slots.len(),
            self.account_cache.len(),
            self.account_cache.startup_versions(),
            self.late_transactions.load(Ordering::Relaxed),
            self.late_account_updates.load(Ordering::Relaxed),
            PENDING_BLOCK_WRITES.load(Ordering::Relaxed),
            PENDING_ACCOUNT_BLOCK_WRITES.load(Ordering::Relaxed),
            PENDING_WRITE_BYTES.load(Ordering::Relaxed),
        );
        info!(
            "callback timings since previous stats at slot {}: {} {} {} {} {} {}",
            slot,
            UPDATE_ACCOUNT.take(),
            NOTIFY_TRANSACTION.take(),
            NOTIFY_BLOCK_METADATA.take(),
            UPDATE_SLOT_STATUS.take(),
            STATE_LOCK_WAIT.take(),
            RPC_BLOCK_FETCH.take(),
        );
    }

    pub fn get_hash_count(&self) -> usize {
        self.account_cache.len()
    }

    fn apply_cache_changes(&mut self, changes: Vec<StateChange>) {
        for change in changes {
            let address_vec = change.address;
            let owner_vec = change.owner;
            let data_hash = change.data_hash;
            let deleted = change.deleted;

            // Convert to fixed-length arrays (assume exactly 32 bytes)
            let mut address_fixed = [0u8; 32];
            let mut owner_fixed = [0u8; 32];

            address_fixed.copy_from_slice(&address_vec);
            owner_fixed.copy_from_slice(&owner_vec);

            if deleted {
                self.account_cache.remove(&owner_fixed, &address_fixed);
            } else {
                self.account_cache
                    .insert(owner_fixed, address_fixed, data_hash); // last one wins
            }
        }
    }
}

struct StateChange {
    address: Vec<u8>,
    owner: Vec<u8>,
    data_hash: u64,
    deleted: bool,
}

/// An account change kept by `filter_account_changes`, borrowing the data from the slot's
/// pending changes so it is only copied when the block is sent.
/// An account change kept by `filter_account_changes`. Its data is that of `changes[source]`,
/// so the caller can move it into the block instead of copying it.
struct FilteredAccount {
    address: [u8; 32],
    owner: [u8; 32],
    source: usize,
    deleted: bool,
}

impl FilteredAccount {
    fn to_account(&self, data: Vec<u8>) -> Account {
        Account {
            address: self.address.to_vec(),
            owner: self.owner.to_vec(),
            data,
            deleted: self.deleted,
        }
    }
}

/// Builds the accounts of `filtered`, moving each data out of `changes` on its last use.
fn filtered_accounts(
    filtered: &[FilteredAccount],
    changes: Vec<AccountWithWriteVersion>,
) -> Vec<Account> {
    let mut uses = vec![0usize; changes.len()];
    for account in filtered {
        uses[account.source] += 1;
    }
    let mut data: Vec<Vec<u8>> = changes.into_iter().map(|c| c.account.data).collect();
    filtered
        .iter()
        .map(|account| {
            uses[account.source] -= 1;
            let data = if uses[account.source] == 0 {
                std::mem::take(&mut data[account.source])
            } else {
                data[account.source].clone()
            };
            account.to_account(data)
        })
        .collect()
}

fn filter_account_changes(
    changes: &[&AccountWithWriteVersion],
    account_cache: &AccountCache,
    slot: u64,
    trace: bool,
) -> (Vec<FilteredAccount>, Vec<StateChange>) {
    let mut filtered_changes: Vec<FilteredAccount> = Vec::new();
    let mut state_changes: Vec<StateChange> = Vec::new();

    let mut in_block_owners = AccountOwners::default();
    let mut ordered_changes: Vec<usize> = (0..changes.len()).collect();
    ordered_changes.sort_by(|&a, &b| {
        let (a, b) = (changes[a], changes[b]);
        a.account
            .address
            .cmp(&b.account.address)
            .then_with(|| a.write_version.cmp(&b.write_version))
    });

    for source in ordered_changes {
        let account_with_version = changes[source];
        let account = &account_with_version.account;
        let cached = account_cache.get(&account.address);

        let mut should_include = false;
        match cached {
            Some((cached_owner, cached_hash)) if cached_owner == &account.owner => {
                if cached_hash != account_with_version.data_hash {
                    should_include = true;
                }
                if account.deleted {
                    should_include = true;
                }
            }
            _ => should_include = true,
        }

        let cached_owner = in_block_owners
            .get(&account.address)
            .or(cached.map(|(owner, _)| owner))
            .copied();

        let different_previous_owner = match cached_owner {
            None => None,
            Some(owner) => {
                if owner != account.owner {
                    Some(owner)
                } else {
                    None
                }
            }
        };

        // Check for ownership change
        if let Some(cached_owner) = different_previous_owner {
            should_include = true;

            // Changes are sorted by address, so this account's earlier entries are the last ones
            let same_address_start = filtered_changes
                .iter()
                .rposition(|change| change.address != account.address)
                .map_or(0, |i| i + 1);

            let prev_already_pushed = filtered_changes[same_address_start..]
                .iter()
                .any(|change| change.owner == cached_owner);

            if !prev_already_pushed {
                // Push the account change with the previous owner
                // it will appear before the new owner's state change
                filtered_changes.push(FilteredAccount {
                    address: account.address,
                    owner: cached_owner,
                    source,
                    deleted: account.deleted,
                });
            }

            for change in filtered_changes[same_address_start..].iter_mut() {
                change.deleted = account.deleted;
                change.source = source;
            }
        }

        in_block_owners.insert(account.address, account.owner);

        if should_include {
            if trace {
                debug!("include_change@{}: account {:?} owner: {:?} delete: {:?} version: {:?} Data hash: {}", slot, bs58::encode(&account.address).into_string(), bs58::encode(&account.owner).into_string(), &account.deleted, account_with_version.write_version, account_with_version.data_hash);
            }
            filtered_changes.push(FilteredAccount {
                address: account.address,
                owner: account.owner,
                source,
                deleted: account.deleted,
            });

            if let Some(cached_owner) = different_previous_owner {
                state_changes.push(StateChange {
                    address: account.address.to_vec(),
                    owner: cached_owner.to_vec(),
                    data_hash: 0,
                    deleted: true, // we delete the version with the old owner from the cache
                });
            }

            state_changes.push(StateChange {
                address: account.address.to_vec(),
                owner: account.owner.to_vec(),
                data_hash: account_with_version.data_hash,
                deleted: account.deleted,
            });

            // save owner in case it gets changed in same slot
            in_block_owners.insert(account.address, account.owner);
        } else {
            if trace {
                debug!("exclude_change@{}: account {:?} owner: {:?} delete: {:?} version: {:?} Data hash: {}", slot, bs58::encode(&account.address).into_string(), bs58::encode(&account.owner).into_string(), &account.deleted, account_with_version.write_version, account_with_version.data_hash);
            }
        }
    }

    (filtered_changes, state_changes)
}

#[cfg(test)]
mod tests {
    use super::*;

    use gxhash::gxhash64;
    use pretty_assertions::assert_eq;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // Helper function to convert Vec<u8> to fixed arrays - expects exactly 32 bytes
    fn vec_to_fixed_32(v: &[u8]) -> [u8; 32] {
        assert_eq!(
            v.len(),
            32,
            "Expected exactly 32 bytes for Solana address/owner"
        );
        let mut arr = [0u8; 32];
        arr.copy_from_slice(v);
        arr
    }

    // Helper function to create composite keys matching the logic in apply_cache_changes
    fn create_composite_key(owner: &[u8], address: &[u8]) -> [u8; 64] {
        let mut owner_fixed = [0u8; 32];
        let mut address_fixed = [0u8; 32];

        // Assume owner and address are exactly 32 bytes
        owner_fixed.copy_from_slice(owner);
        address_fixed.copy_from_slice(address);

        let mut composite_key = [0u8; 64];
        composite_key[..32].copy_from_slice(&owner_fixed);
        composite_key[32..].copy_from_slice(&address_fixed);
        composite_key
    }

    fn create_test_account(
        address: Vec<u8>,
        owner: Vec<u8>,
        data: Vec<u8>,
        deleted: bool,
    ) -> AccountFixed {
        let mut address_fixed = [0u8; 32];
        let mut owner_fixed = [0u8; 32];

        // Assume address and owner are exactly 32 bytes
        address_fixed.copy_from_slice(&address);
        owner_fixed.copy_from_slice(&owner);

        AccountFixed {
            address: address_fixed,
            owner: owner_fixed,
            data,
            deleted,
        }
    }

    fn create_test_account_with_version(
        account: AccountFixed,
        write_version: u64,
        data_hash: u64,
    ) -> AccountWithWriteVersion {
        AccountWithWriteVersion {
            account,
            write_version,
            data_hash,
        }
    }

    #[test]
    fn test_filter_account_changes_empty_changes() {
        let account_cache = AccountCache::default();

        let (filtered_changes, state_changes) =
            filter_account_changes_owned(None, &account_cache, 0, false);

        assert!(filtered_changes.is_empty());
        assert!(state_changes.is_empty());
    }

    #[test]
    fn test_filter_account_changes_new_account() {
        let mut changes = HashMap::default();
        let account_cache = AccountCache::default();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data = vec![7, 8, 9];
        let account = create_test_account(address.clone(), owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        let key = create_composite_key(&owner, &address);
        changes.insert(key, account_with_version);

        let (filtered_changes, state_changes) =
            filter_account_changes_owned(Some(&changes), &account_cache, 0, false);

        assert_eq!(filtered_changes.len(), 1);
        assert_eq!(state_changes.len(), 1);

        let filtered_account = &filtered_changes[0];
        assert_eq!(filtered_account.address, address);
        assert_eq!(filtered_account.owner, owner);
        assert_eq!(filtered_account.data, data);
        assert_eq!(filtered_account.deleted, false);

        let state_change = &state_changes[0];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, owner);
        assert_eq!(state_change.data_hash, 123);
        assert_eq!(state_change.deleted, false);
    }

    #[test]
    fn test_filter_account_changes_same_data_hash_not_deleted() {
        let mut changes = HashMap::default();
        let mut account_cache = AccountCache::default();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data = vec![7, 8, 9];
        let account = create_test_account(address.clone(), owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        let owner_account_key = create_composite_key(&owner, &address);
        changes.insert(owner_account_key, account_with_version);
        let owner_account_key_fixed = owner_account_key;
        account_cache.insert_key(owner_account_key_fixed, 123); // Same hash

        let (filtered_changes, state_changes) =
            filter_account_changes_owned(Some(&changes), &account_cache, 0, false);

        // Should be filtered out because hash is the same and not deleted
        assert!(filtered_changes.is_empty());
        assert!(state_changes.is_empty());
    }

    #[test]
    fn test_filter_account_changes_different_data_hash() {
        let mut changes = HashMap::default();
        let mut account_cache = AccountCache::default();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data = vec![7, 8, 9];
        let account = create_test_account(address.clone(), owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        let owner_account_key = create_composite_key(&owner, &address);
        changes.insert(owner_account_key, account_with_version);
        let owner_account_key_fixed = owner_account_key;
        account_cache.insert_key(owner_account_key_fixed, 456); // Different hash

        let (filtered_changes, state_changes) =
            filter_account_changes_owned(Some(&changes), &account_cache, 0, false);

        // Should be included because hash is different
        assert_eq!(filtered_changes.len(), 1);
        assert_eq!(filtered_changes[0].address, address);

        assert_eq!(state_changes.len(), 1);
        assert_eq!(state_changes[0].data_hash, 123);
    }

    #[test]
    fn test_filter_account_changes_deleted_account() {
        let mut changes = HashMap::default();
        let mut account_cache = AccountCache::default();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data = vec![7, 8, 9];
        let account = create_test_account(address.clone(), owner.clone(), data.clone(), true);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        let owner_account_key = create_composite_key(&owner, &address);
        changes.insert(owner_account_key, account_with_version);
        let owner_account_key_fixed = owner_account_key;
        account_cache.insert_key(owner_account_key_fixed, 123); // Same hash but account is deleted

        let (filtered_changes, state_changes) =
            filter_account_changes_owned(Some(&changes), &account_cache, 0, false);

        // Should be included because account is deleted even with same hash
        assert_eq!(filtered_changes.len(), 1);
        assert_eq!(filtered_changes[0].address, address);
        assert!(filtered_changes[0].deleted);

        assert_eq!(state_changes.len(), 1);
        assert!(state_changes[0].deleted);
    }

    #[test]
    fn test_filter_account_changes_ownership_change() {
        let mut changes = HashMap::default();
        let mut account_cache = AccountCache::default();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let old_owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let new_owner = vec![
            7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
        ];
        let data = vec![10, 11, 12];
        let account = create_test_account(address.clone(), new_owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);

        changes.insert(
            create_composite_key(&new_owner, &address),
            account_with_version,
        );
        account_cache.insert(vec_to_fixed_32(&old_owner), vec_to_fixed_32(&address), 0); // Different owner

        let (filtered_changes, state_changes) =
            filter_account_changes_owned(Some(&changes), &account_cache, 0, false);

        // Should have 2 accounts: one with old owner, one with new owner
        assert_eq!(filtered_changes.len(), 2);
        assert_eq!(state_changes.len(), 2);

        // First account should have the old owner
        let first_account = &filtered_changes[0];
        assert_eq!(first_account.address, address);
        assert_eq!(first_account.owner, old_owner);
        assert_eq!(first_account.data, data);

        // Second account should have the new owner
        let second_account = &filtered_changes[1];
        assert_eq!(second_account.address, address);
        assert_eq!(second_account.owner, new_owner);
        assert_eq!(second_account.data, data);

        // State change should delete the old owner
        let state_change = &state_changes[0];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, old_owner);
        assert_eq!(state_change.deleted, true);

        // State change should reflect the new owner
        let state_change = &state_changes[1];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, new_owner);
    }

    #[test]
    fn test_filter_account_changes_create_changeowner_delete() {
        let mut changes = HashMap::default();
        let account_cache = AccountCache::default();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let old_owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let new_owner = vec![
            7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
        ];
        let data = vec![10, 11, 12];

        let account = create_test_account(address.clone(), old_owner.clone(), data.clone(), false);
        let account_with_version = create_test_account_with_version(account, 1, 123);
        let key1 = create_composite_key(&old_owner, &address);
        changes.insert(key1, account_with_version);

        let account2 = create_test_account(address.clone(), new_owner.clone(), vec![], true);
        let account2_with_version = create_test_account_with_version(account2, 2, 0);
        let key2 = create_composite_key(&new_owner, &address);
        changes.insert(key2, account2_with_version);

        let (mut filtered_changes, state_changes) =
            filter_account_changes_owned(Some(&changes), &account_cache, 0, false);

        // Should have 2 accounts: one with old owner, one with new owner
        assert_eq!(filtered_changes.len(), 2);
        // state_changes are not deduped when we add a deletion after owner change, so we get 3
        assert_eq!(state_changes.len(), 3);

        // we sort because them hashes are unordered and we want deterministic tests
        filtered_changes.sort_by(|a, b| a.owner.cmp(&b.owner));

        // First account should have the new owner
        let first_account = &filtered_changes[0];
        assert_eq!(first_account.address, address);
        assert_eq!(first_account.owner, old_owner);
        assert_eq!(first_account.data.len(), 0);
        assert_eq!(first_account.deleted, true);

        // Second account should have the old owner
        let second_account = &filtered_changes[1];
        assert_eq!(second_account.address, address);
        assert_eq!(second_account.owner, new_owner);
        assert_eq!(second_account.deleted, true);
        assert_eq!(second_account.data.len(), 0);

        // State change should reflect the creation+deletion, then new owner
        let state_change = &state_changes[0];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, old_owner);

        let state_change = &state_changes[1];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, old_owner);
        assert_eq!(state_change.deleted, true);

        let state_change = &state_changes[2];
        assert_eq!(state_change.address, address);
        assert_eq!(state_change.owner, new_owner);
    }

    #[test]
    fn test_filter_account_changes_multiple_accounts_sorted() {
        let mut changes = HashMap::default();
        let account_cache = AccountCache::default();

        // Create accounts with addresses that will test sorting
        let address1 = vec![
            3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ]; // Higher value
        let address2 = vec![
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ]; // Lower value
        let address3 = vec![
            2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ]; // Middle value

        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data = vec![7, 8, 9];

        let account1 = create_test_account(address1.clone(), owner.clone(), data.clone(), false);
        let account2 = create_test_account(address2.clone(), owner.clone(), data.clone(), false);
        let account3 = create_test_account(address3.clone(), owner.clone(), data.clone(), false);

        let account_with_version1 = create_test_account_with_version(account1, 1, 123);
        let account_with_version2 = create_test_account_with_version(account2, 2, 456);
        let account_with_version3 = create_test_account_with_version(account3, 3, 789);

        changes.insert(
            create_composite_key(&owner, &address1),
            account_with_version1,
        );
        changes.insert(
            create_composite_key(&owner, &address2),
            account_with_version2,
        );
        changes.insert(
            create_composite_key(&owner, &address3),
            account_with_version3,
        );

        let (filtered_changes, state_changes) =
            filter_account_changes_owned(Some(&changes), &account_cache, 0, false);

        assert_eq!(filtered_changes.len(), 3);
        assert_eq!(state_changes.len(), 3);

        // Results should be sorted by address
        assert_eq!(filtered_changes[0].address, address2); // [1, 0, 0]
        assert_eq!(filtered_changes[1].address, address3); // [2, 0, 0]
        assert_eq!(filtered_changes[2].address, address1); // [3, 0, 0]
    }

    #[test]
    fn test_filter_account_changes_complex_scenario() {
        let mut changes = HashMap::default();
        let mut account_cache = AccountCache::default();

        // Account 1: New account (should be included)
        let address1 = vec![
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner1 = vec![
            10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data1 = vec![100, 0, 0];
        let account1 = create_test_account(address1.clone(), owner1.clone(), data1.clone(), false);
        let account_with_version1 = create_test_account_with_version(account1, 1, 111);
        let owner_account_key1 = create_composite_key(&owner1, &address1);
        changes.insert(owner_account_key1, account_with_version1);

        // Account 2: Same data hash, not deleted (should be filtered out)
        let address2 = vec![
            2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner2 = vec![
            20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data2 = vec![200, 0, 0];
        let account2 = create_test_account(address2.clone(), owner2.clone(), data2.clone(), false);
        let account_with_version2 = create_test_account_with_version(account2, 2, 222);
        let owner_account_key2 = create_composite_key(&owner2, &address2);
        changes.insert(owner_account_key2, account_with_version2);
        account_cache.insert_key(owner_account_key2, 222); // Same hash

        // Account 3: Ownership change (should include both old and new owner versions)
        let address3 = vec![
            3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let old_owner3 = vec![
            30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let new_owner3 = vec![
            31, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data3 = vec![255, 0, 0];
        let account3 =
            create_test_account(address3.clone(), new_owner3.clone(), data3.clone(), false);
        let account_with_version3 = create_test_account_with_version(account3, 3, 333);
        let owner_account_key3 = create_composite_key(&new_owner3, &address3);
        changes.insert(owner_account_key3, account_with_version3);
        account_cache.insert(vec_to_fixed_32(&old_owner3), vec_to_fixed_32(&address3), 0);

        // Account 4: Deleted account with same hash (should be included)
        let address4 = vec![
            4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner4 = vec![
            40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data4 = vec![200, 0, 0];
        let account4 = create_test_account(address4.clone(), owner4.clone(), data4.clone(), true);
        let account_with_version4 = create_test_account_with_version(account4, 4, 444);
        let owner_account_key4 = create_composite_key(&owner4, &address4);
        changes.insert(owner_account_key4, account_with_version4);
        account_cache.insert_key(owner_account_key4, 444); // Same hash but deleted

        let (filtered_changes, state_changes) =
            filter_account_changes_owned(Some(&changes), &account_cache, 0, false);

        // Should have: Account1, Account3 (old owner), Account3 (new owner), Account4
        assert_eq!(filtered_changes.len(), 4);
        // Should have state changes for: Account1, Account3, (account3: delete), Account4
        assert_eq!(state_changes.len(), 4);

        // Verify the accounts are sorted by address
        assert_eq!(filtered_changes[0].address, address1); // Account 1
        assert_eq!(filtered_changes[1].address, address3); // Account 3 (old owner)
        assert_eq!(filtered_changes[1].owner, old_owner3);
        assert_eq!(filtered_changes[2].address, address3); // Account 3 (new owner)
        assert_eq!(filtered_changes[2].owner, new_owner3);
        assert_eq!(filtered_changes[3].address, address4); // Account 4 (deleted)
        assert_eq!(filtered_changes[3].deleted, true);
    }

    #[test]
    fn test_apply_cache_changes_empty_changes() {
        let mut state = create_test_state();
        let changes = Vec::new();

        state.apply_cache_changes(changes);

        assert!(state.account_cache.is_empty());
    }

    #[test]
    fn test_apply_cache_changes_single_non_deleted_account() {
        let mut state = create_test_state();
        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data_hash = 123u64;

        let change = StateChange {
            address: address.clone(),
            owner: owner.clone(),
            data_hash,
            deleted: false,
        };

        state.apply_cache_changes(vec![change]);

        // Check that the cached owner is updated
        let address_fixed = vec_to_fixed_32(&address);
        let owner_fixed = vec_to_fixed_32(&owner);
        assert_eq!(
            state.account_cache.owner(&address_fixed),
            Some(&owner_fixed)
        );

        // Check that the data hash is cached under the owner
        let expected_key_fixed = create_composite_key(&owner, &address);
        assert_eq!(
            state.account_cache.get_by_key(&expected_key_fixed),
            Some(data_hash)
        );
    }

    #[test]
    fn test_apply_cache_changes_single_deleted_account() {
        let mut state = create_test_state();
        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let data_hash = 123u64;

        // First add an account
        let address_fixed = vec_to_fixed_32(&address);
        let owner_account_key_fixed = create_composite_key(&owner, &address);
        state
            .account_cache
            .insert_key(owner_account_key_fixed, data_hash);

        // Verify it's there
        assert!(state.account_cache.owner(&address_fixed).is_some());
        assert!(state.account_cache.contains_key(&owner_account_key_fixed));

        // Now delete it
        let change = StateChange {
            address: address.clone(),
            owner: owner.clone(),
            data_hash,
            deleted: true,
        };

        state.apply_cache_changes(vec![change]);

        // Check that both are removed
        assert!(!state.account_cache.owner(&address_fixed).is_some());
        assert!(!state.account_cache.contains_key(&owner_account_key_fixed));
    }

    #[test]
    fn test_apply_cache_changes_multiple_accounts() {
        let mut state = create_test_state();

        let address1 = vec![
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner1 = vec![
            10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data_hash1 = 111u64;

        let address2 = vec![
            2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner2 = vec![
            20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data_hash2 = 222u64;

        let address3 = vec![
            3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner3 = vec![
            30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data_hash3 = 333u64;

        let changes = vec![
            StateChange {
                address: address1.clone(),
                owner: owner1.clone(),
                data_hash: data_hash1,
                deleted: false,
            },
            StateChange {
                address: address2.clone(),
                owner: owner2.clone(),
                data_hash: data_hash2,
                deleted: false,
            },
            StateChange {
                address: address3.clone(),
                owner: owner3.clone(),
                data_hash: data_hash3,
                deleted: false,
            },
        ];

        state.apply_cache_changes(changes);

        // Check all accounts are added
        let address1_fixed = vec_to_fixed_32(&address1);
        let owner1_fixed = vec_to_fixed_32(&owner1);
        let address2_fixed = vec_to_fixed_32(&address2);
        let owner2_fixed = vec_to_fixed_32(&owner2);
        let address3_fixed = vec_to_fixed_32(&address3);
        let owner3_fixed = vec_to_fixed_32(&owner3);

        assert_eq!(
            state.account_cache.owner(&address1_fixed),
            Some(&owner1_fixed)
        );
        assert_eq!(
            state.account_cache.owner(&address2_fixed),
            Some(&owner2_fixed)
        );
        assert_eq!(
            state.account_cache.owner(&address3_fixed),
            Some(&owner3_fixed)
        );

        // Check all data hashes are added
        let key1_fixed = create_composite_key(&owner1, &address1);
        let key2_fixed = create_composite_key(&owner2, &address2);
        let key3_fixed = create_composite_key(&owner3, &address3);

        assert_eq!(
            state.account_cache.get_by_key(&key1_fixed),
            Some(data_hash1)
        );
        assert_eq!(
            state.account_cache.get_by_key(&key2_fixed),
            Some(data_hash2)
        );
        assert_eq!(
            state.account_cache.get_by_key(&key3_fixed),
            Some(data_hash3)
        );
    }

    #[test]
    fn test_apply_cache_changes_mixed_operations() {
        let mut state = create_test_state();

        // Pre-populate with some data
        let address1 = vec![
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner1 = vec![
            10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data_hash1 = 111u64;

        let address2 = vec![
            2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner2 = vec![
            20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data_hash2 = 222u64;

        let address1_fixed = vec_to_fixed_32(&address1);
        let address2_fixed = vec_to_fixed_32(&address2);
        let owner2_fixed = vec_to_fixed_32(&owner2);
        let key1_fixed = create_composite_key(&owner1, &address1);
        let key2_fixed = create_composite_key(&owner2, &address2);
        state.account_cache.insert_key(key1_fixed, data_hash1);
        state.account_cache.insert_key(key2_fixed, data_hash2);

        // Now apply mixed changes: delete one, add one, update one
        let address3 = vec![
            3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let owner3 = vec![
            30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let data_hash3 = 333u64;

        let changes = vec![
            // Delete address1
            StateChange {
                address: address1.clone(),
                owner: owner1.clone(),
                data_hash: data_hash1,
                deleted: true,
            },
            // Add new address3
            StateChange {
                address: address3.clone(),
                owner: owner3.clone(),
                data_hash: data_hash3,
                deleted: false,
            },
            // Update address2 with new data hash
            StateChange {
                address: address2.clone(),
                owner: owner2.clone(),
                data_hash: 999u64,
                deleted: false,
            },
        ];

        state.apply_cache_changes(changes);

        // Check address1 is deleted
        assert!(!state.account_cache.owner(&address1_fixed).is_some());
        assert!(!state.account_cache.contains_key(&key1_fixed));

        // Check address2 is updated
        assert_eq!(
            state.account_cache.owner(&address2_fixed),
            Some(&owner2_fixed)
        );
        assert_eq!(state.account_cache.get_by_key(&key2_fixed), Some(999u64));

        // Check address3 is added
        let address3_fixed = vec_to_fixed_32(&address3);
        let owner3_fixed = vec_to_fixed_32(&owner3);
        assert_eq!(
            state.account_cache.owner(&address3_fixed),
            Some(&owner3_fixed)
        );
        let key3_fixed = create_composite_key(&owner3, &address3);
        assert_eq!(
            state.account_cache.get_by_key(&key3_fixed),
            Some(data_hash3)
        );
    }

    #[test]
    fn test_apply_cache_changes_ownership_change() {
        let mut state = create_test_state();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let old_owner = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let new_owner = vec![
            7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
        ];
        let data_hash = 123u64;

        // Set up initial state with old owner
        let address_fixed = vec_to_fixed_32(&address);
        let old_key_fixed = create_composite_key(&old_owner, &address);
        state.account_cache.insert_key(old_key_fixed, data_hash);

        // Apply ownership change
        let change = StateChange {
            address: address.clone(),
            owner: new_owner.clone(),
            data_hash,
            deleted: false,
        };

        state.apply_cache_changes(vec![change]);

        // Check that the cached owner is updated to the new owner
        let new_owner_fixed = vec_to_fixed_32(&new_owner);
        assert_eq!(
            state.account_cache.owner(&address_fixed),
            Some(&new_owner_fixed)
        );

        // Check that new key is added
        let new_key_fixed = create_composite_key(&new_owner, &address);
        assert_eq!(
            state.account_cache.get_by_key(&new_key_fixed),
            Some(data_hash)
        );

        // Note: the apply_cache should be called with a 'deleted: true' on the old state. we're testing an incomplete scenario
    }

    #[test]
    fn test_apply_cache_changes_same_address_different_owners() {
        let mut state = create_test_state();

        let address = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let owner1 = vec![
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35,
        ];
        let owner2 = vec![
            7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
        ];
        let data_hash1 = 111u64;
        let data_hash2 = 222u64;

        // Apply changes for the same address but different owners
        let changes = vec![
            StateChange {
                address: address.clone(),
                owner: owner1.clone(),
                data_hash: data_hash1,
                deleted: true, // data for old owner should always be deleted there
            },
            StateChange {
                address: address.clone(),
                owner: owner2.clone(),
                data_hash: data_hash2,
                deleted: false,
            },
        ];

        state.apply_cache_changes(changes);

        // Verify new account is added and old account with old owner is deleted
        let address_fixed = vec_to_fixed_32(&address);
        let owner2_fixed = vec_to_fixed_32(&owner2);
        assert_eq!(
            state.account_cache.owner(&address_fixed),
            Some(&owner2_fixed)
        );

        let new_key_fixed = create_composite_key(&owner2, &address);
        let old_key_fixed = create_composite_key(&owner1, &address);

        assert_eq!(
            state.account_cache.get_by_key(&new_key_fixed),
            Some(data_hash2)
        );
        assert_eq!(state.account_cache.get_by_key(&old_key_fixed), None);
    }

    // Helper function to create a test State instance
    fn create_test_state() -> State {
        use crate::block_printer::BlockPrinter;

        // Create temporary RPC clients (these won't be used in cache tests)
        let local_client = RpcClient::new("http://localhost:8899".to_string());
        let remote_client = RpcClient::new("http://localhost:8899".to_string());

        // Create a noop BlockPrinter
        let block_printer = BlockPrinter::new(None, None, true);

        State::new(
            local_client,
            remote_client,
            None,
            "/tmp/test_cursor".to_string(),
            block_printer,
            false,
            DevelopmentConfig::default(),
        )
    }

    /// Create test state with customizable parameters
    fn new_test_state(
        cursor: Option<u64>,
        block_printer: crate::block_printer::BlockPrinter,
    ) -> State {
        // Create temporary RPC clients (these won't be used in cache tests)
        let local_client = RpcClient::new("http://localhost:8899".to_string());
        let remote_client = RpcClient::new("http://localhost:8899".to_string());

        State::new(
            local_client,
            remote_client,
            cursor,
            "/tmp/test_cursor".to_string(),
            block_printer,
            false,
            DevelopmentConfig::default(),
        )
    }

    /// Assert that logs contain expected messages in order
    fn assert_logs_contain_ordered(expected_logs: Vec<String>) {
        testing_logger::validate(|captured_logs| {
            let log_bodies: Vec<String> =
                captured_logs.iter().map(|log| log.body.clone()).collect();

            for expected_log in &expected_logs {
                let found = log_bodies.iter().any(|log| log.contains(expected_log));
                assert!(
                    found,
                    "Expected log message '{}' not found in captured logs.\nCaptured logs: {:?}",
                    expected_log, log_bodies
                );
            }
        });
    }

    /// Setup noop block printer with logging for tests
    fn setup_noop_block_printer_with_logging(
        with_block: bool,
        with_account: bool,
    ) -> crate::block_printer::BlockPrinter {
        use crate::block_printer::BlockPrinter;
        use std::fs::File;
        use tempfile::NamedTempFile;
        use testing_logger;

        // Setup log capture
        testing_logger::setup();

        let block_file = if with_block {
            let temp_file = NamedTempFile::new().unwrap();
            Some(File::open(temp_file.path()).unwrap())
        } else {
            None
        };

        let account_file = if with_account {
            let temp_file = NamedTempFile::new().unwrap();
            Some(File::open(temp_file.path()).unwrap())
        } else {
            None
        };

        BlockPrinter::new(block_file, account_file, true)
    }

    /// Create simple BlockInfo with just slot number, parent_slot defaults to slot-1
    fn simple_block_info(slot: u64) -> BlockInfo {
        BlockInfo {
            slot,
            parent_slot: slot.saturating_sub(1),
            block_hash: format!("block_hash_{}", slot),
            parent_hash: format!("parent_hash_{}", slot.saturating_sub(1)),
            timestamp: Timestamp {
                seconds: 1000 + slot as i64,
                nanos: 0,
            },
            height: Some(slot),
            rewards: vec![],
            transaction_count: 0,
        }
    }

    fn test_block_info(slot: u64, parent_slot: u64) -> BlockInfo {
        BlockInfo {
            timestamp: Timestamp {
                seconds: 1234,
                nanos: 0,
            },
            parent_slot,
            slot,
            block_hash: "hash1".to_string(),
            parent_hash: "parent1".to_string(),
            height: Some(100),
            rewards: vec![],
            transaction_count: 0,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_set_block_info() {
        let mock_server = MockServer::start().await;
        let test_url = mock_server.uri();

        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "result": 100,
                "id": 1
            })))
            .mount(&mock_server)
            .await;

        // Initialize state with no lib and no first_received_blockmeta

        let mut state = State::new(
            RpcClient::new(test_url.clone()),
            RpcClient::new(test_url.clone()),
            None,
            "test_cursor_file".to_string(),
            BlockPrinter::new(None, None, false),
            true,
            DevelopmentConfig::default(),
        );

        // Test case 1: No lib set yet
        let block_info = test_block_info(100, 99);

        state.set_block_info(block_info.clone(), false);
        assert_eq!(state.lib, Some(100)); // From mock response
        assert_eq!(state.first_received_blockmeta, Some(100));
        assert_eq!(state.first_block_to_process, Some(100));

        // Test case 2: With cursor set, lib is before cursor
        let mut state_with_cursor = State::new(
            RpcClient::new(test_url.clone()),
            RpcClient::new(test_url.clone()),
            Some(110),
            "test_cursor_file".to_string(),
            BlockPrinter::new(None, None, false),
            true,
            DevelopmentConfig::default(),
        );

        state_with_cursor.set_block_info(block_info.clone(), false);
        assert_eq!(state_with_cursor.first_received_blockmeta, Some(100));
        assert_eq!(state_with_cursor.first_block_to_process, None); // Should not be set since cursor exists
        assert_eq!(state_with_cursor.cursor, Some(110));

        // Test case 3: With cursor set, lib is greater than cursor which will get cancelled
        let mut state_with_cursor = State::new(
            RpcClient::new(test_url.clone()),
            RpcClient::new(test_url.clone()),
            Some(90),
            "test_cursor_file".to_string(),
            BlockPrinter::new(None, None, false),
            true,
            DevelopmentConfig::default(),
        );

        state_with_cursor.set_block_info(block_info.clone(), false);
        assert_eq!(state_with_cursor.first_received_blockmeta, Some(100));
        assert_eq!(state_with_cursor.first_block_to_process, Some(100)); // gets set since cursor must be ignored
        assert_eq!(state_with_cursor.cursor, None);

        // Test case 4: Already initialized state

        state.first_received_blockmeta = Some(50);
        state.set_block_info(test_block_info(100, 99), false);

        // Check block was added without modifying first_received_blockmeta
        assert_eq!(state.first_received_blockmeta, Some(50));
        assert!(state.block_infos.contains_key(&100));
    }

    #[test]
    fn test_add_missing_slots_to_confirmed_slots() {
        let mut state = State::new(
            RpcClient::new("http://test.local"),
            RpcClient::new("http://test.remote"),
            None,
            "test_cursor.txt".to_string(),
            BlockPrinter::new(None, None, false),
            true,
            DevelopmentConfig::default(),
        );

        // Setup initial state
        state.initialized = true;
        state.last_sent_block = Some(1);

        state.block_infos.insert(1, test_block_info(1, 0));
        state.block_infos.insert(2, test_block_info(2, 1));
        state.block_infos.insert(4, test_block_info(4, 2));
        state.block_infos.insert(6, test_block_info(6, 4));

        // assume we receive confirmed_slot 7 with parent_slot 6
        let result =
            state.add_missing_slots_to_confirmed_slots(state.last_sent_block.unwrap(), 6, false);
        assert!(result);

        assert!(state.confirmed_slots.get(&1).is_none()); // was already sent

        assert!(state.confirmed_slots.get(&2).is_some());
        assert!(state.confirmed_slots.get(&4).is_some());
        assert!(state.confirmed_slots.get(&6).is_some());
    }

    const PUB_KEY_1: &[u8] = &[
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1,
    ];
    const PUB_KEY_2: &[u8] = &[
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        2, 2,
    ];
    const PUB_KEY_3: &[u8] = &[
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        3, 3,
    ];
    const OWNER_KEY_1: &[u8] = &[
        10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
        10, 10, 10, 10, 10, 10, 10, 10, 10,
    ];
    const DATA_1: &[u8] = b"data.1";
    const DATA_2: &[u8] = b"data.2";
    const DATA_3: &[u8] = b"data.2";
    const OWNER_KEY_11111111111111111111111111111111: &[u8] = [
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ]
    .as_slice();

    #[test]
    fn test_startup_value_repeated_is_still_saved() {
        let mut state = test_state_no_rpc(None);
        let slot: u64 = 1;
        let data_hash = gxhash64(b"data.1", 76);

        state.first_block_to_process = Some(slot);
        // First call with is_startup=true
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash, slot, 0, false);

        // Second call with incremented slot
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_1,
            0,
            false,
            data_hash,
            false,
        );

        // Even if it is the same data, we still save it here, because it could be changed by another slot
        assert_eq!(state.block_account_changes.get(&slot).unwrap().len(), 1);
    }

    #[test]
    fn test_set_account_on_startup_keeps_highest_slot() {
        let mut state = test_state_no_rpc(None);

        let data_hash_1 = gxhash64(b"data.1", 76);
        let data_hash_2 = gxhash64(b"data.1", 76);
        let data_hash_3 = gxhash64(b"data.1", 76);
        let data_hash_4 = gxhash64(b"data.4", 76);

        let owner_account_key_fixed = create_composite_key(OWNER_KEY_1, PUB_KEY_1);
        let pub_key_1_fixed = vec_to_fixed_32(PUB_KEY_1);

        state.first_block_to_process = Some(10);

        // set startup value with slot=8 -> must be set
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_1, 8, 1, false);

        assert_eq!(
            state
                .account_cache
                .get_by_key(&owner_account_key_fixed)
                .unwrap(),
            data_hash_1
        );

        // set startup value with slot=7 -> unchanged
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_2, 7, 1, false);
        assert_eq!(
            state
                .account_cache
                .get_by_key(&owner_account_key_fixed)
                .unwrap(),
            data_hash_1
        );

        // set startup value with slot=8, higher write_version -> changed
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_2, 8, 4, false);
        assert_eq!(
            state
                .account_cache
                .get_by_key(&owner_account_key_fixed)
                .unwrap(),
            data_hash_2
        );

        // set startup value with slot=9, lower write_version -> changed
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_3, 9, 0, false);
        assert_eq!(
            state
                .account_cache
                .get_by_key(&owner_account_key_fixed)
                .unwrap(),
            data_hash_3
        );

        // set startup value with slot=10, higher write_version, deleted=true -> all traces removed
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_4, 10, 1, true);

        // verify all traces to the account are removed from hashes
        assert!(state
            .account_cache
            .get_by_key(&owner_account_key_fixed)
            .is_none());
        assert!(state.account_cache.owner(&pub_key_1_fixed).is_none());

        // test with different owner: set up account with OWNER_KEY_1 again
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_1, 11, 0, false);
        assert_eq!(
            state
                .account_cache
                .get_by_key(&owner_account_key_fixed)
                .unwrap(),
            data_hash_1
        );
        assert_eq!(
            state.account_cache.owner(&pub_key_1_fixed).unwrap(),
            &vec_to_fixed_32(OWNER_KEY_1)
        );

        // set with different owner (OWNER_KEY_11111111111111111111111111111111) and deleted=true
        let different_owner = OWNER_KEY_11111111111111111111111111111111;
        state.set_account_on_startup(PUB_KEY_1, different_owner, data_hash_4, 12, 0, true);

        // Verify previous values with the other owner are also gone
        assert!(state
            .account_cache
            .get_by_key(&owner_account_key_fixed)
            .is_none());
        assert!(state.account_cache.owner(&pub_key_1_fixed).is_none());

        // set a value 'before' the block where it got deleted. it should remain deleted
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_1, 11, 0, false);
        assert!(state
            .account_cache
            .get_by_key(&owner_account_key_fixed)
            .is_none());

        // set a value 'after' the block where it got deleted. it should remain deleted
        state.set_account_on_startup(PUB_KEY_1, OWNER_KEY_1, data_hash_1, 12, 1, false);
        assert_eq!(
            state
                .account_cache
                .get_by_key(&owner_account_key_fixed)
                .unwrap(),
            data_hash_1
        );
    }

    #[test]
    fn test_set_account_on_startup_owner_change_cleanup() {
        let mut state = test_state_no_rpc(None);
        let slot1: u64 = 10;
        let slot2: u64 = 20;

        let owner1 = &[
            11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
            11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
        ];
        let owner2 = &[
            22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22,
            22, 22, 22, 22, 22, 22, 22, 22, 22, 22,
        ];
        let data_hash1 = gxhash64(b"data.1", 76);
        let data_hash2 = gxhash64(b"data.2", 76);

        state.first_block_to_process = Some(slot1);

        // First call with owner1
        state.set_account_on_startup(PUB_KEY_1, owner1, data_hash1, slot1, 0, false);

        // Verify owner1+pubkey entry exists in the account cache
        let owner1_account_key_fixed = create_composite_key(owner1, PUB_KEY_1);
        let pub_key_1_fixed = vec_to_fixed_32(PUB_KEY_1);
        let owner1_fixed = vec_to_fixed_32(owner1);
        assert_eq!(
            state
                .account_cache
                .get_by_key(&owner1_account_key_fixed)
                .unwrap(),
            data_hash1
        );
        assert_eq!(
            state.account_cache.owner(&pub_key_1_fixed).unwrap(),
            &owner1_fixed
        );

        // Second call with owner2 and higher slot number
        state.set_account_on_startup(PUB_KEY_1, owner2, data_hash2, slot2, 0, false);

        // Verify that owner1+pubkey entry is deleted from the account cache
        assert!(state
            .account_cache
            .get_by_key(&owner1_account_key_fixed)
            .is_none());

        // Verify that owner2+pubkey entry exists in the account cache
        let owner2_account_key_fixed = create_composite_key(owner2, PUB_KEY_1);
        let owner2_fixed = vec_to_fixed_32(owner2);
        assert_eq!(
            state
                .account_cache
                .get_by_key(&owner2_account_key_fixed)
                .unwrap(),
            data_hash2
        );
        assert_eq!(
            state.account_cache.owner(&pub_key_1_fixed).unwrap(),
            &owner2_fixed
        );
    }

    #[test]
    fn test_set_account_delete_two_owners_repeat() {
        let mut state = test_state_no_rpc(None);
        let slot: u64 = 1;
        let data_hash = 0;

        state.first_block_to_process = Some(slot);
        // create account there
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_1,
            0,
            false, // deleted
            data_hash,
            false,
        );

        // delete account here
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_11111111111111111111111111111111,
            0,
            true, // deleted
            data_hash,
            false,
        );

        // Assert that self.block_account_entries(slot_number) is empty for the slot+1
        let slot_changes = state.block_account_changes.get(&slot).unwrap();
        assert!(!slot_changes.is_empty());
        assert!(slot_changes.len() == 2);

        let acc_owner_1 = create_composite_key(OWNER_KEY_1, PUB_KEY_1);
        let acc_owner_11111 =
            create_composite_key(OWNER_KEY_11111111111111111111111111111111, PUB_KEY_1);

        assert!(slot_changes.contains_key(&acc_owner_1));
        assert!(slot_changes.contains_key(&acc_owner_11111));
    }

    #[test]
    fn test_set_account_delete_before_ownerchange_repeat() {
        let mut state = test_state_no_rpc(None);
        let slot: u64 = 1;
        let data_hash = 0;

        state.first_block_to_process = Some(slot);
        // create account there
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_1,
            0,
            false, // deleted
            data_hash,
            false,
        );

        // delete account here
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_1,
            0,
            true, // deleted
            data_hash,
            false,
        );

        // delete account here
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_11111111111111111111111111111111,
            0,
            true, // deleted
            data_hash,
            false,
        );

        // Assert that self.block_account_entries(slot_number) is empty for the slot+1
        let slot_changes = state.block_account_changes.get(&slot).unwrap();
        assert!(!slot_changes.is_empty());
        assert!(slot_changes.len() == 2);

        let acc_owner_1 = create_composite_key(OWNER_KEY_1, PUB_KEY_1);
        let acc_owner_11111 =
            create_composite_key(OWNER_KEY_11111111111111111111111111111111, PUB_KEY_1);

        assert!(slot_changes.contains_key(&acc_owner_1));
        assert!(slot_changes.contains_key(&acc_owner_11111));
    }

    #[test]
    fn test_set_account_delete_after_ownerchange_repeat() {
        let mut state = test_state_no_rpc(None);
        let slot: u64 = 1;
        let data_hash = 0;

        state.first_block_to_process = Some(slot);
        // create account there
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_1,
            0,
            false,
            data_hash,
            false,
        );

        // delete account here
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_11111111111111111111111111111111,
            0,
            false,
            data_hash,
            false,
        );

        // delete account here
        state.set_account(
            slot,
            PUB_KEY_1,
            DATA_1,
            OWNER_KEY_11111111111111111111111111111111,
            0,
            true, // deleted
            data_hash,
            false,
        );

        // Assert that self.block_account_entries(slot_number) is empty for the slot+1
        let slot_changes = state.block_account_changes.get(&slot).unwrap();
        assert!(!slot_changes.is_empty());
        assert!(slot_changes.len() == 2);

        let acc_owner_1 = create_composite_key(OWNER_KEY_1, PUB_KEY_1);
        let acc_owner_11111 =
            create_composite_key(OWNER_KEY_11111111111111111111111111111111, PUB_KEY_1);

        assert!(slot_changes.contains_key(&acc_owner_1));
        assert!(slot_changes.contains_key(&acc_owner_11111));
    }

    fn test_state_no_rpc(cursor: Option<u64>) -> State {
        test_state(
            "http://localhost:8899".to_string(),
            "http://localhost:8899".to_string(),
            cursor,
        )
    }

    fn test_state(local_rpc: String, remote_rpc: String, cursor: Option<u64>) -> State {
        State::new(
            RpcClient::new(local_rpc),
            RpcClient::new(remote_rpc),
            cursor,
            "test_cursor.txt".to_string(),
            BlockPrinter::new(None, None, false),
            true,
            DevelopmentConfig::default(),
        )
    }

    #[test]
    fn test_integration_simple() {
        let mut state = new_test_state(Some(99), setup_noop_block_printer_with_logging(true, true));

        state.set_lib(99);

        state.set_account(100, PUB_KEY_1, DATA_1, OWNER_KEY_1, 1, false, 12345, true);
        state.set_account(101, PUB_KEY_2, DATA_2, OWNER_KEY_1, 1, false, 23456, true);
        state.set_account(102, PUB_KEY_3, DATA_3, OWNER_KEY_1, 1, false, 34567, true);

        state.set_block_info(simple_block_info(100), true);
        state.set_confirmed_slot(100, true);
        state.set_block_info(simple_block_info(101), true);
        state.set_confirmed_slot(101, true);
        state.set_block_info(simple_block_info(102), true);
        state.set_confirmed_slot(102, true);

        // Build expected log messages
        let mut expected_logs = Vec::new();
        for slot in [100, 101, 102] {
            expected_logs.push(format!("printing block {} (noop mode)", slot));
            expected_logs.push(format!("printing account_block {} (noop mode)", slot));
        }

        // Validate captured logs
        assert_logs_contain_ordered(expected_logs);
    }

    fn concat_keys(owner: &[u8], account: &[u8]) -> [u8; 64] {
        let mut result = [0u8; 64];
        result[..32].copy_from_slice(owner);
        result[32..].copy_from_slice(account);
        result
    }

    #[test]
    fn test_integration_cursor_after_start() {
        // Create state with noop BlockPrinter
        let mut state =
            new_test_state(Some(101), setup_noop_block_printer_with_logging(true, true));

        state.set_lib(99);

        state.set_account(100, PUB_KEY_1, DATA_1, OWNER_KEY_1, 1, false, 12345, true);
        state.set_account(101, PUB_KEY_2, DATA_2, OWNER_KEY_1, 1, false, 23456, true);
        state.set_account(102, PUB_KEY_3, DATA_3, OWNER_KEY_1, 1, false, 34567, true);

        state.set_block_info(simple_block_info(100), true);
        state.set_confirmed_slot(100, true);
        state.set_block_info(simple_block_info(101), true);
        state.set_confirmed_slot(101, true);
        state.set_block_info(simple_block_info(102), true);
        state.set_confirmed_slot(102, true);

        // Build expected log messages
        let mut expected_logs = Vec::new();
        for slot in [101, 102] {
            // slot 100 will not be printed
            expected_logs.push(format!("printing block {} (noop mode)", slot));
            expected_logs.push(format!("printing account_block {} (noop mode)", slot));
        }

        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_1))
                .unwrap(),
            12345
        );
        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_2))
                .unwrap(),
            23456
        );
        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_3))
                .unwrap(),
            34567
        );

        // Validate captured logs
        assert_logs_contain_ordered(expected_logs);
    }

    #[test]
    fn test_integration_lib_after_cursor() {
        // Create state with noop BlockPrinter
        let mut state = new_test_state(Some(50), setup_noop_block_printer_with_logging(true, true));

        state.set_lib(100);

        state.set_account(100, PUB_KEY_1, DATA_1, OWNER_KEY_1, 1, false, 12345, true);
        state.set_account(101, PUB_KEY_2, DATA_2, OWNER_KEY_1, 1, false, 23456, true);
        state.set_account(102, PUB_KEY_3, DATA_3, OWNER_KEY_1, 1, false, 34567, true);

        state.set_block_info(simple_block_info(100), true);
        state.set_confirmed_slot(100, true);
        state.set_block_info(simple_block_info(101), true);
        state.set_confirmed_slot(101, true);
        state.set_block_info(simple_block_info(102), true);
        state.set_confirmed_slot(102, true);

        // Build expected log messages
        let mut expected_logs = Vec::new();
        for slot in [100, 101, 102] {
            expected_logs.push(format!("printing block {} (noop mode)", slot));
            expected_logs.push(format!("printing account_block {} (noop mode)", slot));
        }

        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_1))
                .unwrap(),
            12345
        );
        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_2))
                .unwrap(),
            23456
        );
        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_3))
                .unwrap(),
            34567
        );

        // Validate captured logs
        assert_logs_contain_ordered(expected_logs);
    }

    #[test]
    fn test_integration_no_cursor_set_lib_delayed() {
        // Create state with noop BlockPrinter
        let mut state = new_test_state(None, setup_noop_block_printer_with_logging(true, true));

        state.set_account(100, PUB_KEY_1, DATA_1, OWNER_KEY_1, 1, false, 12345, true);
        state.set_account(101, PUB_KEY_2, DATA_2, OWNER_KEY_1, 1, false, 23456, true);
        state.set_account(102, PUB_KEY_3, DATA_3, OWNER_KEY_1, 1, false, 34567, true);
        state.set_block_info(simple_block_info(100), true);
        state.set_confirmed_slot(100, true);
        state.set_block_info(simple_block_info(101), true);
        state.set_confirmed_slot(101, true);

        state.set_lib(50);

        state.set_block_info(simple_block_info(102), true);
        state.set_confirmed_slot(102, true);

        // Build expected log messages
        let mut expected_logs = Vec::new();
        for slot in [100, 101, 102] {
            expected_logs.push(format!("printing block {} (noop mode)", slot));
            expected_logs.push(format!("printing account_block {} (noop mode)", slot));
        }

        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_1))
                .unwrap(),
            12345
        );
        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_2))
                .unwrap(),
            23456
        );
        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_3))
                .unwrap(),
            34567
        );

        // Validate captured logs
        assert_logs_contain_ordered(expected_logs);
    }

    #[test]
    fn test_integration_no_cursor_missing_first_blockinfo() {
        // Create state with noop BlockPrinter
        let mut state = new_test_state(None, setup_noop_block_printer_with_logging(true, true));

        state.set_account(100, PUB_KEY_1, DATA_1, OWNER_KEY_1, 1, false, 12345, true);
        state.set_account(101, PUB_KEY_2, DATA_2, OWNER_KEY_1, 1, false, 23456, true);
        state.set_account(102, PUB_KEY_3, DATA_3, OWNER_KEY_1, 1, false, 34567, true);

        state.set_lib(100);

        // here we DON'T send block_info for slot 100
        state.set_confirmed_slot(100, true);
        state.set_block_info(simple_block_info(101), true);
        state.set_confirmed_slot(101, true);
        state.set_block_info(simple_block_info(102), true);
        state.set_confirmed_slot(102, true);

        // Build expected log messages
        let mut expected_logs = Vec::new();
        for slot in [101, 102] {
            // block 100 is not sent because we didn't get blockinfo for it and had no cursor
            expected_logs.push(format!("printing block {} (noop mode)", slot));
            expected_logs.push(format!("printing account_block {} (noop mode)", slot));
        }

        // this should be inserted even if we don't actually SEND the block
        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_1))
                .unwrap(),
            12345
        );
        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_2))
                .unwrap(),
            23456
        );
        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_3))
                .unwrap(),
            34567
        );

        // Validate captured logs
        assert_logs_contain_ordered(expected_logs);
    }

    #[test]
    fn test_integration_cursor_and_lib_after_missing_first_blockinfo_and_confirmed() {
        // Create state with noop BlockPrinter
        let mut state =
            new_test_state(Some(102), setup_noop_block_printer_with_logging(true, true));

        state.set_account(100, PUB_KEY_1, DATA_1, OWNER_KEY_1, 1, false, 12345, true);
        state.set_account(101, PUB_KEY_2, DATA_2, OWNER_KEY_1, 1, false, 23456, true);
        state.set_account(102, PUB_KEY_3, DATA_3, OWNER_KEY_1, 1, false, 34567, true);

        // here we DON'T send block_info for slot 100
        // we DON'T send confirmed_slot either
        state.set_block_info(simple_block_info(101), true);
        state.set_confirmed_slot(101, true);

        state.set_lib(101);
        state.set_block_info(simple_block_info(102), true);
        state.set_confirmed_slot(102, true);

        // Build expected log messages
        let mut expected_logs = Vec::new();
        // 100 and 101 not sent because cursor is at slot 102
        for slot in [102] {
            // block 100 is not sent because we didn't get blockinfo for it and had no cursor
            expected_logs.push(format!("printing block {} (noop mode)", slot));
            expected_logs.push(format!("printing account_block {} (noop mode)", slot));
        }

        // this should be inserted even if we don't actually SEND the block
        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_1))
                .unwrap(),
            12345
        );
        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_2))
                .unwrap(),
            23456
        );
        assert_eq!(
            state
                .account_cache
                .get_by_key(&concat_keys(OWNER_KEY_1, PUB_KEY_3))
                .unwrap(),
            34567
        );

        // Validate captured logs
        assert_logs_contain_ordered(expected_logs);
    }

    fn filter_account_changes_owned(
        changes: Option<&HashMap<[u8; 64], AccountWithWriteVersion>>,
        account_cache: &AccountCache,
        slot: u64,
        trace: bool,
    ) -> (Vec<Account>, Vec<StateChange>) {
        let changes: Vec<&AccountWithWriteVersion> = changes
            .into_iter()
            .flat_map(|changes| changes.values())
            .collect();
        let (accounts, state_changes) =
            filter_account_changes(&changes, account_cache, slot, trace);
        let accounts = accounts
            .iter()
            .map(|account| account.to_account(changes[account.source].account.data.clone()))
            .collect();
        (accounts, state_changes)
    }

    #[test]
    fn test_purge_drops_transactions_below_lib_and_last_sent() {
        let mut state = create_test_state();
        let transaction = || ConfirmTransactionWithIndex {
            index: 0,
            transaction: crate::pb::sf::solana::r#type::v1::ConfirmedTransaction::default(),
        };
        for slot in [50, 90, 95, 101, 150] {
            state
                .transactions
                .get_mut()
                .unwrap()
                .insert(slot, vec![transaction()]);
        }

        // Without a sent block, missing parents may still be confirmed from any slot
        state.lib = Some(90);
        state.purge_blocks_up_to(100);
        assert_eq!(state.transactions.get_mut().unwrap().len(), 5);

        state.last_sent_block = Some(100);
        state.purge_blocks_up_to(100);
        let mut kept: Vec<u64> = state
            .transactions
            .get_mut()
            .unwrap()
            .keys()
            .copied()
            .collect();
        kept.sort();
        assert_eq!(kept, vec![95, 101, 150]);
    }

    /// The pending changes of a slot, merged across shards.
    impl PendingAccountChanges {
        fn get(&self, slot: &u64) -> Option<AccountChanges> {
            if !self.contains_slot(*slot) {
                return None;
            }
            let mut merged = AccountChanges::default();
            for shard in &self.shards {
                if let Some(changes) = Self::lock(shard).get(slot) {
                    merged.extend(changes.iter().map(|(key, change)| (*key, change.clone())));
                }
            }
            Some(merged)
        }
    }

    fn split_key(key: &[u8; 64]) -> ([u8; 32], [u8; 32]) {
        let mut owner = [0u8; 32];
        let mut address = [0u8; 32];
        owner.copy_from_slice(&key[..32]);
        address.copy_from_slice(&key[32..]);
        (owner, address)
    }

    /// Lookups and inserts by owner(32) + pubkey(32) key, the layout the tests are written in.
    impl AccountCache {
        fn get_by_key(&self, key: &[u8; 64]) -> Option<u64> {
            let (owner, address) = split_key(key);
            self.data_hash(&owner, &address)
        }

        fn contains_key(&self, key: &[u8; 64]) -> bool {
            self.get_by_key(key).is_some()
        }

        fn insert_key(&mut self, key: [u8; 64], data_hash: u64) {
            let (owner, address) = split_key(&key);
            self.insert(owner, address, data_hash);
        }
    }

    /// The account cache kept as two maps, owner + pubkey -> data hash and pubkey -> owner,
    /// plus a third for the newest snapshot version of each pubkey, with the same update
    /// rules as `AccountCache`.
    #[derive(Default)]
    struct TwoMapCache {
        data_hash: HashMap<[u8; 64], u64>,
        owners: HashMap<[u8; 32], [u8; 32]>,
        startup_versions: HashMap<[u8; 32], u64>,
    }

    impl TwoMapCache {
        fn set_on_startup(
            &mut self,
            owner: [u8; 32],
            address: [u8; 32],
            data_hash: u64,
            version: u64,
            deleted: bool,
        ) {
            if let Some(&existing) = self.startup_versions.get(&address) {
                if existing >= version {
                    return;
                }
            }
            self.startup_versions.insert(address, version);

            if let Some(previous_owner) = self.owners.get(&address) {
                if previous_owner != &owner {
                    self.data_hash
                        .remove(&concat_keys(previous_owner, &address));
                }
            }
            let key = concat_keys(&owner, &address);
            if deleted {
                self.data_hash.remove(&key);
                self.owners.remove(&address);
            } else {
                self.data_hash.insert(key, data_hash);
                self.owners.insert(address, owner);
            }
        }

        fn apply(&mut self, changes: &[StateChange]) {
            for change in changes {
                let owner = vec_to_fixed_32(&change.owner);
                let address = vec_to_fixed_32(&change.address);
                let key = concat_keys(&owner, &address);
                if change.deleted {
                    self.data_hash.remove(&key);
                    if self.owners.get(&address) == Some(&owner) {
                        self.owners.remove(&address);
                    }
                } else {
                    self.data_hash.insert(key, change.data_hash);
                    self.owners.insert(address, owner);
                }
            }
        }
    }

    #[test]
    fn test_account_cache_matches_two_map_cache() {
        const ADDRESSES: u8 = 4;
        const OWNERS: u8 = 3;

        // xorshift64, so failures replay identically
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = |bound: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % bound
        };

        let mut state = create_test_state();
        let mut expected = TwoMapCache::default();
        let mut write_version = 0u64;

        for slot in 1..20_000u64 {
            if next(500) == 0 {
                expected.startup_versions.clear();
                state.delete_startup_info();
            } else if next(4) == 0 {
                let owner = [1 + next(OWNERS as u64) as u8; 32];
                let address = [10 + next(ADDRESSES as u64) as u8; 32];
                let data_hash = next(3);
                let deleted = next(5) == 0;
                // Versions out of order, so some are older than one already received
                let startup_slot = next(8);
                let startup_write_version = next(4);
                expected.set_on_startup(
                    owner,
                    address,
                    data_hash,
                    (startup_slot << 25) | startup_write_version,
                    deleted,
                );
                state.set_account_on_startup(
                    &address,
                    &owner,
                    data_hash,
                    startup_slot,
                    startup_write_version,
                    deleted,
                );
            } else {
                let mut changes: AccountChanges = HashMap::default();
                for _ in 0..1 + next(6) {
                    write_version += 1;
                    let owner = [1 + next(OWNERS as u64) as u8; 32];
                    let address = [10 + next(ADDRESSES as u64) as u8; 32];
                    let data_hash = next(3);
                    let deleted = next(5) == 0;
                    let account =
                        create_test_account(address.to_vec(), owner.to_vec(), vec![], deleted);
                    changes.insert(
                        concat_keys(&owner, &address),
                        create_test_account_with_version(account, write_version, data_hash),
                    );
                }
                let (_, state_changes) = filter_account_changes(
                    &changes.values().collect::<Vec<_>>(),
                    &state.account_cache,
                    slot,
                    false,
                );
                expected.apply(&state_changes);
                state.apply_cache_changes(state_changes);
            }

            assert_eq!(state.account_cache.len(), expected.owners.len());
            assert_eq!(
                state.account_cache.startup_versions(),
                expected.startup_versions.len()
            );
            for a in 0..ADDRESSES {
                let address = [10 + a; 32];
                assert_eq!(
                    state.account_cache.owner(&address),
                    expected.owners.get(&address),
                    "owner of address {} after slot {}",
                    a,
                    slot
                );
                for o in 0..OWNERS {
                    let owner = [1 + o; 32];
                    assert_eq!(
                        state.account_cache.data_hash(&owner, &address),
                        expected
                            .data_hash
                            .get(&concat_keys(&owner, &address))
                            .copied(),
                        "data hash of address {} under owner {} after slot {}",
                        a,
                        o,
                        slot
                    );
                }
            }
        }
    }
}

fn compose_and_purge_block(
    slot: u64,
    block_info: &BlockInfo,
    transactions_with_index: Vec<ConfirmTransactionWithIndex>,
) -> Block {
    Block {
        previous_blockhash: block_info.parent_hash.clone(),
        blockhash: block_info.block_hash.clone(),
        slot,
        transactions: transactions_with_index
            .into_iter()
            .map(|ti| ti.transaction)
            .collect(),
        rewards: block_info.rewards.clone(),
        block_time: Some(UnixTimestamp {
            timestamp: block_info.timestamp.seconds,
        }),
        parent_slot: block_info.parent_slot,
        block_height: match block_info.height {
            Some(height) => Some(BlockHeight {
                block_height: height,
            }),
            None => None,
        },
    }
}
