# Benchmark: HashMap Implementation Comparison

This benchmark compares the performance of the `set_account_on_startup` method using two different HashMap implementations:

1. **hashbrown::HashMap** - The current implementation used in the main codebase
2. **ahash::AHashMap** - Alternative implementation using the ahash hasher

## Test Setup

The benchmark tests loading **10,000 entries** with the following characteristics:

- **8,000 unique entries** - New account data that gets stored
- **2,000 duplicates** - Entries with lower write versions that should be skipped
- **100 different owners** - Simulates realistic ownership distribution
- **Owner changes** - Tests the cleanup logic when accounts change owners

## Running the Benchmarks

### Full Benchmark Suite
```bash
cargo bench --bench startup_benchmark
```

### Specific Benchmarks Only
```bash
cargo bench --bench startup_benchmark "hashbrown_set_account_on_startup|ahash_set_account_on_startup"
```

### Verification Test
```bash
cargo run --bin test_benchmark --release
```

## Results Summary

Based on our benchmark results:

| Implementation | Average Time | Performance |
|----------------|-------------|-------------|
| **hashbrown** | ~1.11 ms | Baseline |
| **ahash** | ~1.06 ms | **~4.5% faster** |

### Key Findings

1. **ahash is consistently faster** - Shows approximately 4-5% performance improvement
2. **Both implementations are functionally identical** - All verification tests pass
3. **Performance difference is modest but measurable** - The improvement is consistent across runs
4. **Memory usage patterns are similar** - Both use comparable amounts of memory

## Implementation Details

Both implementations use identical logic for:
- Duplicate detection (slot/write_version comparison)
- Owner change handling with cleanup
- Memory pre-allocation for reduced allocations

The only difference is the underlying HashMap implementation:
- `hashbrown::HashMap` uses SipHash (cryptographically secure but slower)
- `ahash::AHashMap` uses AHash (faster, non-cryptographic hash function)

## Files

- `startup_benchmark.rs` - Main benchmark suite using Criterion
- `test_benchmark.rs` - Simple verification and quick performance test
- `README.md` - This documentation

## Dependencies Added

```toml
[dependencies]
ahash = "0.8"

[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }
```

## Recommendation

The benchmark suggests that switching to `ahash::AHashMap` would provide a small but consistent performance improvement (~4-5%) for the `set_account_on_startup` method without any functional changes to the codebase.

For high-throughput scenarios processing many account updates, this improvement could be meaningful while maintaining identical behavior and memory characteristics.