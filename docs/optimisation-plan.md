# Memory and CPU optimisation plan

A mainnet validator running the plugin was OOM-killed at 507 GB RSS. The plugin's `memory stats` log showed its buffers flat (pending account changes, transactions, pending fifo writes), but its two account caches held 1.18 billion entries each. This document records what was changed, how it was measured and verified, and what is left.

Every change keeps the plugin's output identical. Nothing here changes what is written to the block and account block streams.

## Results

### Mainnet scale, 1.18 billion accounts (computed from entry sizes and hashbrown's power-of-two tables)

| | Before | After |
|---|---|---|
| Account cache, steady state | ~297 GB | ~97 GB |
| Startup peak (cache and startup versions) | ~385 GB, plus a whole-map growth spike | ~116 GB, growth one shard at a time |

### Throughput benchmark, Linux, 20 million accounts, 8 threads

`tests/throughput_test.rs`, run in an arm64 Linux container with `RUSTFLAGS="-C target-feature=+aes,+neon"` and `THROUGHPUT_SLOTS=3000`. Snapshot accounts arrive from 8 threads, like Agave's index generation. Live traffic is 8 threads sending interleaved account updates (2,400 per slot) and transactions (1,000 per slot), like replay threads.

| | Original (`be95410`) | Now |
|---|---|---|
| Startup, per snapshot account | 564 ns | 292 ns |
| RSS at startup | 8.6 to 10.4 GB | 2.4 to 2.5 GB |
| Account update or transaction callback | 533 ns | 312 ns |
| Block processing, exclusive lock held | 3.0 ms/slot | 1.03 ms/slot (some runs 1.44 ms, see below) |
| Process CPU over 3,000 live slots | 34.2 s | 30.8 to 31.2 s |

## What was done

1. **One cache entry per account.** The two maps, owner + pubkey to data hash and pubkey to owner, became one map keyed by pubkey holding owner and data hash (`aecf28f`). A data hash is only returned when the owner asked for matches.
2. **Compact entries.** Owners are stored once and referenced by a `u32` index, and entries are packed to 44 bytes (`734a7a0`).
3. **Sharded cache.** The cache is split in 256 shards picked by pubkey byte 16, which stays uniform for vanity prefixes and pump.fun's `...pump` suffix. Shards grow one at a time, so the old and new tables coexist for one shard, not the whole map (`734a7a0`).
4. **Startup versions inside the cache.** The newest snapshot version of each account lives in its cache entry during startup instead of in a second map keyed by pubkey. The cache converts to the smaller layout at end of startup (`751f97c`).
5. **Fork transactions dropped.** Transactions of slots at or below both the LIB and the last sent block are dropped. Before, they stayed until restart: 188 slots and about 200k transactions on the node that crashed (`6e69810`).
6. **Hashing and copying outside the lock.** Account data is hashed and copied before taking the state lock (`0d0ef1c`).
7. **No rayon for base64.** `rbase64` spread every payload over 128 KiB on rayon's global pool, which cost about 24% more CPU. Blocks are now encoded with `base64` on their printer thread. `rbase64` also installed mimalloc as the plugin's global allocator, which the plugin now declares itself (`7629c4c`).
8. **Parallel account updates and transactions.** They take the state lock in shared mode. Account updates write to 16 pending-changes shards and transactions to a map, each behind its own lock. Only block processing, and a transaction arriving after its slot is confirmed, take the lock exclusively. Moving transactions was needed: with only account updates on the shared lock, each transaction taking it exclusively made the updates drain, which was slower than one lock (`11b0f14`, `3e0c934`).
9. **Data moved into the block.** Pending account data moves into the block instead of being copied three times, which cut block processing by a third (`22be702`).
10. **Parallel startup.** Snapshot accounts are recorded under the shared lock, with an account's version check and update under its shard's lock. The end-of-startup conversion runs on 8 threads. The locks written by every callback sit on their own cache lines: without that, callbacks after startup were 8% slower (`7c53b60`).

## How it was verified

- `tests/output_regression_test.rs` runs a seeded workload through the Geyser callbacks: snapshot load, owner changes, deletes, forks with transactions, skipped slots, late updates. It checks a digest of both output streams. The digest was recorded on the code before these changes and matches after each one. Deliberate changes to the filter and to the data moves change it.
- The same workload with account updates and transactions spread over 4 threads gives the same digest.
- `test_account_cache_matches_two_map_cache` checks every cache lookup against the original two-map logic over 20,000 random rounds, including out-of-order snapshot versions and ending startup mid-run.
- The solana-battlefield suite passes against Agave `v4.3.0-fh3.0`: all 4 account tests and 2 block tests.

## What is left

### Not measured yet

- **Production CPU.** Benchmarks ran on arm64 (Apple M-series and a Linux VM on it). Production is AMD Zen 4 on Linux. The lock behavior measured on Linux applies. Hashing and memcpy speeds differ.
- **The 1.25 s stalls in production.** Every callback had `max_ms` around 1.25 s in the same window, so something held the state lock exclusively that long. The `callback timings` line was cut off by the log collector (`up>`), which hid the `state_lock_wait` and `rpc_block_fetch` columns. Candidates: the blocking `getBlock` RPC fallback, which runs under the exclusive lock, or memory-pressure stalls before the OOM. Splitting that log line in two would show it.
- **Block processing sometimes at 1.44 ms instead of 1.0 ms.** In the Linux VM, about one run in three stays at the higher value for the whole run. It only appeared after the parallel startup change (item 10); runs with the startup serialized never showed it. The cause is unknown. If it shows up in production `callback timings`, try serializing startup again.

### Possible next steps

| Idea | Expected gain | Cost or risk |
|---|---|---|
| Large OS pages for mimalloc (`MIMALLOC_ALLOW_LARGE_OS_PAGES=1` in the validator's environment) | Fewer TLB misses on the ~97 GB cache | Configuration only. Needs a production check of RSS and latency |
| Split the `callback timings` log line so the collector keeps all columns | Shows what holds the lock for 1.25 s | None |
| Move the `getBlock` RPC fallback out of the exclusive lock | Removes seconds-long stalls when it runs | Changes when block info lands relative to other callbacks; needs care |
| Lock-free cache reads during block processing (the exclusive lock already excludes writers) | 3 to 5% of block processing | More code paths into the cache |
| 65,536 shards and 30-byte keys (2 key bytes implied by the shard) | ~4 GB | Moderate code change |
| Key the cache by 16 bytes of the pubkey | ~35 GB (97 GB to ~62 GB) | Two accounts sharing 16 bytes would be treated as one. Very unlikely, but not impossible, so it changes behavior |
| Sorted static table from the snapshot plus a map of changes | ~40 GB | Slower lookups, and changes pile up in the map over time |
| gxhash `hybrid` (VAES + AVX2 on Zen 4) | Faster hashing of large accounts | Requires nightly Rust |
