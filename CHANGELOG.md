# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v4.3.0-rc.0-fh3.0

* Bumped to [Agave 4.3.0-rc.0](https://github.com/anza-xyz/agave/releases/tag/v4.3.0-rc.0) (`agave-geyser-plugin-interface`, `solana-rpc-client`, `solana-rpc-client-api`, `solana-transaction-status`, `solana-transaction-context`), which is 16 upstream commits past `v4.3.0-beta.2` and covers the `v4.3.0-beta.3` and `v4.3.0-rc.0` releases. All five crates are published and unyanked.
* Aligned the Docker image's Solana validator to `v4.3.0-rc.0-fh3.0` (was `v4.3.0-beta.2-fh3.0`).
* No plugin code change was required. The Geyser plugin interface is untouched in this range, as are `transaction-status`, `transaction-context` and the `rpc-client` crates — no trait signature changes, no field changes on `ReplicaAccountInfo*`, `ReplicaTransactionInfo*`, `ReplicaBlockInfo*` or `TransactionStatusMeta`.
* `rust-toolchain.toml` stays at `1.97.1` and the standalone `solana-*` pins are unchanged: Agave 4.3.0-rc.0 resolves the same `solana-hash` 4.6.0, `solana-pubkey` 4.3.0, `solana-signature` 3.5.2, `solana-clock` 3.2.0, `solana-commitment-config` 3.1.1, `solana-message` 4.5.0 and `solana-transaction` 4.2.0. The `Cargo.lock` change is limited to the 35 Agave-versioned crates; no transitive dependency moved.
* Upstream fixed the same class of bug the transaction v1 work addressed, on a path the plugin does not use: `storage-proto` was reading every stored V1 message back as a V0, dropping its inline compute budget, because it branched on `versioned`, which is true for V0 and V1 alike ([#14874](https://github.com/anza-xyz/agave/pull/14874)). That is the Bigtable decode path. The plugin builds its protobuf straight from `VersionedMessage` and reads the V1 config directly, so it was never affected.
* Validated against the `solana-battlefield` test suite (run locally against an Agave 4.3.0-rc.0 `solana-test-validator` built from the fork), on top of the workspace unit tests.

## v4.3.0-beta.2-fh3.0-1

* Emit the `version` and `transaction_config` fields added to `sf.solana.type.v1.Message` in [firehose-solana v1.4.0](https://github.com/streamingfast/firehose-solana/releases/tag/v1.4.0), for Solana transaction v1 (SIMD-0296, SIMD-0385). Transaction v1 activated on devnet at slot 492480000 and reaches mainnet around September 9.
* `version` carries the wire version, so 0 for a v0 message and 1 for a v1 message, and stays unset for a legacy message. The plugin already decoded a v1 message but reported it through the `versioned` boolean, which cannot distinguish v0 from v1. `versioned` is unchanged.
* `transaction_config` carries the compute budget a v1 message holds inline, which the plugin previously dropped. Legacy and v0 messages request the same settings through ComputeBudget program instructions and leave the field unset, as does a v1 message that requests nothing. Both the `ReplicaTransactionInfoV3` and `ReplicaTransactionInfoV2` paths fill in the two fields.
* Added a type check over `solana_message::v1::TransactionConfig` so that a field added upstream fails the build rather than being silently dropped.
* Repinned the `buf.build/streamingfast/firehose-solana` dependency to the commit carrying the new fields.

## v4.3.0-beta.2-fh3.0

* Bumped to [Agave 4.3.0-beta.2](https://github.com/anza-xyz/agave/releases/tag/v4.3.0-beta.2) (`agave-geyser-plugin-interface`, `solana-rpc-client`, `solana-rpc-client-api`, `solana-transaction-status`, `solana-transaction-context`). This moves the plugin from the `4.2` release line to `4.3`. `v4.3.0-beta.2` is the most recent upstream tag with published, unyanked crates and it is live on devnet and testnet (`v4.3.0-beta.1` crates were yanked after a `cargo audit` failure on RUSTSEC-2026-0258).
* Aligned the Docker image's Solana validator to `v4.3.0-beta.2-fh3.0` (was `v4.2.1-fh3.0`).
* Bumped `rust-toolchain.toml` to `1.97.1` to match Agave 4.3 (was `1.96.1`).
* No plugin code change was required. Agave 4.3 is an Alpenglow release and reworks the Geyser plugin interface, but backwards-compatibly: `update_account`, `notify_transaction`, `notify_entry` and `notify_block_metadata` are deprecated in favour of `update_account_from_snapshot`, `update_account_for_bank`, `notify_transaction_for_bank`, `notify_entry_for_bank` and `notify_block_metadata_for_bank`, which carry a `BankId`. Every new callback has a default implementation delegating to the old one, so the plugin's existing implementations keep receiving all events.
* `update_slot_status` is now only called directly for statuses with no bank (`FirstShredReceived`, `Completed`, `Dead`); bank-scoped statuses (`Confirmed`, `Processed`, `Rooted`, `CreatedBank`) go to the new `update_bank_status`, whose default delegates to `update_slot_status`. The plugin does not override it, so slot handling is unchanged. The `SlotStatus` enum itself is unchanged and the plugin's match remains exhaustive.
* New Alpenglow callbacks left unimplemented (defaults are no-ops): `notify_block_footer` (gated on `block_footer_notifications_enabled`, which stays `false`), `notify_entry_update_parent` and `notify_deshred_update_parent`.
* The `transaction-status` changes in this range are confined to the human-readable token extension parsers (`confidential_transfer`, `confidential_mint_burn`, `permissioned_burn`), which the plugin does not use — it consumes the raw `TransactionStatusMeta`, `InnerInstructions` and `Rewards` types.
* Standalone `solana-*` crates realigned to what Agave 4.3 resolves: `solana-hash` 4.4.0 → 4.6.0, `solana-pubkey` 4.2.1 → 4.3.0, `solana-signature` 3.4.1 → 3.5.2, `solana-clock` 3.1.1 → 3.2.0, `solana-message` 4.2.3 → 4.5.0, `solana-transaction` 4.1.4 → 4.2.0. `solana-commitment-config` stays at 3.1.1.
* Validated against the `solana-battlefield` test suite (run locally against an Agave 4.3.0-beta.2 `solana-test-validator` built from the fork), on top of the workspace unit tests.

## v4.2.1-fh3.0

* Bumped to [Agave 4.2.1](https://github.com/anza-xyz/agave/releases/tag/v4.2.1)


## v4.2.0-fh3.0

* Bumped to [Agave 4.2.0](https://github.com/anza-xyz/agave/releases/tag/v4.2.0) (`agave-geyser-plugin-interface`, `solana-rpc-client`, `solana-rpc-client-api`, `solana-transaction-status`, `solana-transaction-context`).
* Aligned the Docker image's Solana validator to [`v4.2.0-fh3.0`](https://github.com/streamingfast/solana/pkgs/container/solana) (was `v4.2.0-rc.1-fh3.0`).
* No plugin code change was required: the upstream range between `4.2.0-rc.1` and `4.2.0` is 10 commits touching `account-decoder`/`transaction-status` (SPL Token-2022 instruction parsing), `poh`, `gossip`, `runtime`, `rpc` and `snapshots`. The Geyser plugin interface is untouched — no trait signature changes, no field changes on `ReplicaAccountInfo*`, `ReplicaTransactionInfo*`, `ReplicaBlockInfo*` or `TransactionStatusMeta`. The `transaction-status` changes are confined to the human-readable instruction parsers (`parse_token`, new `permissioned_burn` extension), which the plugin does not use — it consumes the raw `TransactionStatusMeta`, `InnerInstructions` and `Rewards` types.
* Transitive dependency moves pulled in by Agave 4.2.0: `spl-token-2022-interface` 2.1.0 → 3.1.1, `spl-token-metadata-interface` 0.8.0 → 1.0.1, `spl-token-confidential-transfer-proof-extraction` 0.5.1 → 0.6.1, `solana-curve25519` 3.1.2 → 4.0.1, and `solana-zk-sdk` replaced by `solana-zk-sdk-pod`.
* The standalone `solana-*` crates (`solana-hash` 4.4.0, `solana-pubkey` 4.2.0, `solana-signature` 3.4.1, `solana-clock` 3.1.1, `solana-commitment-config` 3.1.1, `solana-message` 4.2.3, `solana-transaction` 4.1.4) are unchanged: Agave 4.2.0 resolves the same versions the plugin already pins.
* Validated against the `solana-battlefield` test suite (run locally against an Agave 4.2.0 `solana-test-validator`), on top of the workspace unit tests.

## v4.2.0-rc.1-fh3.0

* Bumped to [Agave 4.2.0-rc.1](https://github.com/anza-xyz/agave/releases/tag/v4.2.0-rc.1) (`agave-geyser-plugin-interface`, `solana-rpc-client`, `solana-rpc-client-api`, `solana-transaction-status`, `solana-transaction-context`).
* Aligned the Docker image's Solana validator to [`v4.2.0-rc.1-fh3.0`](https://github.com/streamingfast/solana/pkgs/container/solana) (was `v4.2.0-rc.0-fh3.0`).
* No plugin code change was required: the upstream range between `4.2.0-rc.0` and `4.2.0-rc.1` is 5 commits touching only `core` (vote listener, slot supporters, replay stage) and `ledger` (blockstore processor). The Geyser plugin interface is untouched — no trait signature changes, no field changes on `ReplicaAccountInfo*`, `ReplicaTransactionInfo*`, `ReplicaBlockInfo*` or `TransactionStatusMeta`.
* The standalone `solana-*` crates (`solana-hash` 4.4.0, `solana-pubkey` 4.2.0, `solana-signature` 3.4.1, `solana-clock` 3.1.1, `solana-commitment-config` 3.1.1, `solana-message` 4.2.3, `solana-transaction` 4.1.4) are unchanged: Agave 4.2.0-rc.1 resolves the same versions the plugin already pins.
* Validated against the `solana-battlefield` test suite (run locally), on top of the workspace unit tests.
* CI: build and push Docker images on pushes to `release/**` (not only on tags).
* CI: extract release notes for the exact tag section in CHANGELOG (avoids substring matches like `v4.2.0-rc.0` → `v4.2.0-rc.0-fh3.0-1`).
* CI: clarify GitHub release Docker blurb — image is for solana-devnet only; production should use a native build for host-optimal instructions.
* CHANGELOG: backfilled missing sections for `v4.2.0-rc.0-fh3.0-1`, `v4.2.0-rc.0-fh3.0`, `v4.2.0-beta.1-3`, and `v4.2.0-beta.1-1`.

## v4.2.0-rc.0-fh3.0-1

* Fixed silent validator death on solana-devnet (`trap invalid opcode` / SIGILL in `libfirehose_geyser_plugin.so` on the `solBankNotif` thread). Docker/CI built the plugin with `RUSTFLAGS=-C target-cpu=native`, so the `.so` could contain instructions from the builder CPU (e.g. AVX-512) that production hosts do not implement. Release builds now use portable `RUSTFLAGS=-C target-feature=+aes,+sse2` (the minimum gxhash requires) instead of `native`.

## v4.2.0-rc.0-fh3.0

* Added post-startup diagnostic breadcrumbs for silent exits on solana-devnet. Surfaces stage, duration, and real error context around account-cache apply, first-send `process_upto`, and block FIFO writes so the next unexpected stop leaves a clear trail instead of a silent EOF.

## v4.2.0-rc.0

* Bumped to [Agave 4.2.0-rc.0](https://github.com/anza-xyz/agave/releases/tag/v4.2.0-rc.0).
* Aligned the Docker image's Solana validator to [`v4.2.0-rc.0-fh3.0`](https://github.com/streamingfast/solana/pkgs/container/solana) (was `v4.2.0-beta.1-novote`). Note the tag suffix changed from `-novote` to `-fh3.0`: our Solana fork now uses the Firehose protocol suffix, matching the convention of the other StreamingFast chain forks. The novote behaviour itself is unchanged.
* No plugin code change was required: the Geyser plugin interface is identical between Agave `4.2.0-beta.1` and `4.2.0-rc.0` (no trait signature changes, no field changes on `ReplicaAccountInfo*`, `ReplicaTransactionInfo*`, `ReplicaBlockInfo*` or `TransactionStatusMeta`, no notification-semantics changes). The upstream range is 9 commits, all `accounts-db`, `runtime`, `status_cache` and XDP performance backports.
* The Rust toolchain stays at 1.96.1, matching the Agave 4.2 validator (a Geyser plugin must be built with the same toolchain as the validator it loads into).
* Pinned `solana-hash` (4.4.0), `solana-message` (4.2.3) and `solana-transaction` (4.1.4) to the exact versions the Agave 4.2.0-rc.0 validator itself compiles. They previously floated one minor/patch ahead. Since the plugin is a `cdylib` loaded into the validator process, these crates supply types that cross the plugin boundary, and two independently resolved copies of a struct is the one place where semver compatibility is not a layout guarantee.
* Removed the `solana-program` dependency. It was pinned at `=4.0.0` with a stale `not upgraded to 4.1.x yet` note (no 4.1.x was ever published; 4.0.0 is the latest), and was used for a single import, `clock::UnixTimestamp`. That now comes from `solana-clock`, which the Geyser plugin interface already pulls in.
* Refreshed the declared minimum versions of `solana-hash`, `solana-pubkey`, `solana-signature`, `solana-message` and `solana-transaction` so they state what actually resolves instead of trailing it.

## v4.2.0-beta.1-3

* Documentation-only: explained fewer `deleted: true` account events versus 4.1 (upstream Agave [#13079](https://github.com/anza-xyz/agave/pull/13079)); see `v4.2.0-beta.1`. No plugin code change.

## v4.2.0-beta.1-2

* Stamped the `cap_net_admin,cap_net_raw+ep` file capabilities onto `/app/agave-validator` in the Docker image. Starting with agave-validator v4.2.0 (alpenglow client), the validator requires `CAP_NET_ADMIN` and `CAP_NET_RAW` to manage its UDP sockets, otherwise it aborts at startup. The image runs non-root, so `cap_add` alone only fills the bounding set; file capabilities grant them effective at exec. Requires the deployer (sf-operator) to add `NET_ADMIN` and `NET_RAW` to `cap_add`.
* Added `libcap2-bin` to the final image stage to provide `setcap`.

## v4.2.0-beta.1-1

* First release through the GitHub Actions Release workflow: on a `v*` tag, builds and pushes the Docker image to ghcr.io and publishes a GitHub release with CHANGELOG notes and a `docker pull` link (no `.so` asset attached).
* Fixed the CHANGELOG section header for the 4.2.0-beta.1 line (was incorrectly labeled `v4.2.0-rc.1`).

## v4.2.0-beta.1

* Bumped to [Agave 4.2.0-beta.1](https://github.com/anza-xyz/agave/releases/tag/v4.2.0-beta.1).
* Aligned the Rust toolchain to 1.96.1 to match the Agave 4.2 validator (a Geyser plugin must be built with the same toolchain as the validator it loads into).
* Aligned the Docker image's Solana validator to [`v4.2.0-beta.1-novote`](https://github.com/streamingfast/solana/pkgs/container/solana) (was `v4.1.0-novote`), matching the Agave 4.2 plugin.
* Behavior change (upstream, no plugin code change): account exhibits emit fewer `deleted: true` entries than 4.1. Agave [#13079](https://github.com/anza-xyz/agave/pull/13079) ("perf: skip writing untouched accounts") makes `collect_accounts_to_store` skip write-locked accounts a transaction left unmodified (`touched_flags`), so they no longer trigger a Geyser `notify_account_update`. Previously every writable account of a transaction was notified even when unchanged, producing spurious `deleted: true` events for already-dead (0-lamport) accounts. The plugin's notification path is unchanged; it simply receives fewer updates. Expected/benign diff versus 4.1 golden exhibits — same class as the 4.1.0 bump diff.

## v3.1.8-1

* Added missing `cost_units` field to transactionStatusMeta
* Added type checks for other missing fields (will break at compile-time if agave libraries add more)

## v3.1.8

* Bumped to [Agave 3.1.8](https://github.com/anza-xyz/agave/releases/tag/v3.0.8).

## v3.0.11

* Bumped to [Agave 3.0.11](https://github.com/anza-xyz/agave/releases/tag/v3.0.11).

## v3.0.9

* Bumped to [Agave 3.0.9](https://github.com/anza-xyz/agave/releases/tag/v3.0.9) (plugin dependencies were kept to [Agave 3.0.8](https://github.com/anza-xyz/agave/releases/tag/v3.0.8) as no version `3.0.9` has been published).

## v3.0.7

* Bumped to [Agave 3.0.7](https://github.com/anza-xyz/agave/releases/tag/v3.0.7).

## v3.0.6-1

* Fixed parsing of Firehose geyser plugin config when no `"dev"` section is present.

## v3.0.6

* Bumped to [Agave 3.0.6](https://github.com/anza-xyz/agave/releases/tag/v3.0.6).

## v3.0.5

* Bumped to [Agave 3.0.5](https://github.com/anza-xyz/agave/releases/tag/v3.0.5).

* First release through `sfreleaser`.
