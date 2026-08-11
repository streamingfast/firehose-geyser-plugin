# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
