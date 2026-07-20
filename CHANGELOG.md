# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v4.2.0-beta.1-2

* Stamped the `cap_net_admin,cap_net_raw+ep` file capabilities onto `/app/agave-validator` in the Docker image. Starting with agave-validator v4.2.0 (alpenglow client), the validator requires `CAP_NET_ADMIN` and `CAP_NET_RAW` to manage its UDP sockets, otherwise it aborts at startup. The image runs non-root, so `cap_add` alone only fills the bounding set; file capabilities grant them effective at exec. Requires the deployer (sf-operator) to add `NET_ADMIN` and `NET_RAW` to `cap_add`.
* Added `libcap2-bin` to the final image stage to provide `setcap`.

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
