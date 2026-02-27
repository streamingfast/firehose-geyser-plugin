# Upgrade Notes

## Upgrading to Agave v4.0

This document tracks the analysis and migration steps required to upgrade this plugin
to support [agave v4.0](https://github.com/anza-xyz/agave/tree/v4.0) (the Solana validator
client maintained by Anza, formerly known as the `solana-labs/solana` repo).

---

### GeyserPlugin interface: no breaking changes

After comparing `geyser-plugin-interface/src/geyser_plugin_interface.rs` between
`v3.1.8` (currently used) and the `v4.0` branch, **the plugin interface is identical**.
All versioned structs and the `GeyserPlugin` trait are unchanged:

- `ReplicaAccountInfoVersions` — V0_0_1, V0_0_2, V0_0_3 (unchanged)
- `ReplicaTransactionInfoVersions` — V0_0_1, V0_0_2, V0_0_3 (unchanged)
- `ReplicaBlockInfoVersions` — V0_0_1 through V0_0_4 (unchanged)
- `ReplicaEntryInfoVersions` — V0_0_1, V0_0_2 (unchanged)
- `SlotStatus` — all variants (unchanged)
- `GeyserPlugin` trait methods — all signatures (unchanged)

This means all callback logic in `src/plugins.rs` (`update_account`,
`notify_transaction`, `notify_block_metadata`, etc.) will continue to compile and
work without modification once dependency versions are updated.

---

### Availability on crates.io

As of the time of this writing, **no `4.0.0-alpha.0` crates have been published to
crates.io**. The latest stable release is `3.1.9`. The v4.0 branch is still in alpha.

To test against v4.0 before it is published you would need to use git dependencies
pointing at the `v4.0` branch of `https://github.com/anza-xyz/agave`.

---

### Dependency changes required

The v4.0 workspace bumps its own version to `4.0.0-alpha.0` and updates several
external dependencies to new semver-major versions. The table below maps each
dependency declared in `Cargo.toml` to what v4.0 expects.

| `Cargo.toml` dependency | Current version | v4.0 version |
|---|---|---|
| `agave-geyser-plugin-interface` | `=3.1.8` | `=4.0.0-alpha.0` |
| `solana-rpc-client` | `=3.1.8` | `=4.0.0-alpha.0` |
| `solana-rpc-client-api` | `=3.1.8` | `=4.0.0-alpha.0` |
| `solana-transaction-status` | `=3.1.8` | `=4.0.0-alpha.0` |
| `solana-transaction-context` | `=3.1.8` | `=4.0.0-alpha.0` |
| `solana-sdk` | `3.0.0` | **removed** (dissolved into fine-grained crates) |
| `solana-commitment-config` | `3.0` | `3.1.1` |
| `solana-message` | `3.0` | `3.1.0` |
| `solana-program` | `3.0` | `3.0.0` (unchanged) |
| `solana-transaction` | `3.0` | `3.1.0` |
| `env_logger` | `0.9.3` | `0.11.9` |
| `thiserror` (transitive) | `1.x` | `2.0.18` |

---

### Breaking changes in detail

#### 1. `solana-sdk` is being removed

The monolithic `solana-sdk` crate is being dissolved into many fine-grained crates.
This is the most impactful change for this plugin. The following imports in
`src/plugins.rs` will need to be rewritten:

```rust
// Before (solana-sdk monolith)
use solana_sdk::hash::Hash;
use solana_sdk::message::v0::LoadedAddresses;
use solana_sdk::message::AccountKeys;
use solana_sdk::{bs58, pubkey::Pubkey};
```

These map to the following individual crates (all already published at `3.x`):

| Old import | New crate |
|---|---|
| `solana_sdk::hash::Hash` | `solana-hash` → `solana_hash::Hash` |
| `solana_sdk::message::v0::LoadedAddresses` | `solana-message` → `solana_message::v0::LoadedAddresses` |
| `solana_sdk::message::AccountKeys` | `solana-message` → `solana_message::AccountKeys` |
| `solana_sdk::pubkey::Pubkey` | `solana-pubkey` → `solana_pubkey::Pubkey` |
| `solana_sdk::bs58` | external `bs58` crate directly |

#### 2. `solana-transaction-context` — potential API changes

`solana-transaction-context` moves from an external crate at `3.x` to a path crate
at `4.0.0-alpha.0` inside the agave workspace (with the `agave-unstable-api` feature
flag). The plugin uses `solana_transaction_context::TransactionReturnData`. Verify
this type is still accessible after the bump.

#### 3. `thiserror` 2.x

The agave workspace pins `thiserror = "2.0.18"`. This is a semver-major bump from
`1.x` and includes derive-macro changes. Any custom error types that derive
`thiserror::Error` should be tested for compatibility.

#### 4. `env_logger` 0.11

The plugin directly depends on `env_logger = "0.9.3"`. v4.0 uses `0.11.9`. The
public API is largely compatible but the builder API changed slightly between 0.9
and 0.11 — review any custom `Builder` usage.

#### 5. `rand` 0.9 (transitive)

The agave workspace pins `rand = "0.9.2"`, a major bump from `0.8.x`. This is
transitive but can surface as duplicate-version linker or type-mismatch errors if
any direct dependency also pins an older version.

---

### What you can do right now (before v4.0 ships)

1. **Bump to `3.1.9`** — the latest stable release. Update all `=3.1.8` pins to
   `=3.1.9` in `Cargo.toml`. The interface is identical; this is a safe drop-in.

2. **Replace `solana-sdk` imports proactively.** The fine-grained crates (`solana-pubkey`,
   `solana-hash`, `solana-message`) are already published at `3.x` and are what
   v4.0 uses internally. Migrating now reduces the diff when v4.0 lands.

3. **Update `env_logger`** from `0.9.3` to `0.11` — this is independent of the
   agave version bump and is safe to do today.

---

### Migration checklist for when v4.0 is published

- [ ] Bump `agave-geyser-plugin-interface` to `=4.0.0`
- [ ] Bump `solana-rpc-client` to `=4.0.0`
- [ ] Bump `solana-rpc-client-api` to `=4.0.0`
- [ ] Bump `solana-transaction-status` to `=4.0.0`
- [ ] Bump `solana-transaction-context` to `=4.0.0`
- [ ] Remove `solana-sdk` dependency
- [ ] Add `solana-pubkey`, `solana-hash` as direct dependencies
- [ ] Update `solana_sdk::*` imports in `src/plugins.rs` (see table above)
- [ ] Bump `solana-commitment-config` to `3.1`
- [ ] Bump `solana-message` to `3.1`
- [ ] Bump `solana-transaction` to `3.1`
- [ ] Update `env_logger` to `0.11`
- [ ] Verify `thiserror 2.x` compatibility
- [ ] Run full test suite: `cargo test`
- [ ] Build the cdylib and smoke-test against a v4.0 validator node
- [ ] Update `rust-toolchain.toml` to match the toolchain pinned by agave v4.0