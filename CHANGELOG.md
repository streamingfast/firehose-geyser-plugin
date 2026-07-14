# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v4.2.0-rc.1

* Bumped to [Agave 4.2.0-beta.1](https://github.com/anza-xyz/agave/releases/tag/v4.2.0-beta.1).
* Aligned the Rust toolchain to 1.96.1 to match the Agave 4.2 validator (a Geyser plugin must be built with the same toolchain as the validator it loads into).

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
