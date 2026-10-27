## Chain Update

When doing a chain update, it's important to also modify those location when bumping to right version to ensure proper alignment of Solana validator (agave) version and the Geyser Plugin.

- [Dockerfile](./Dockerfile#L2)

  The `ARG AGAVE_VERSION=...` line must be updated to the upstream Agave tag (for example `v4.4.0-rc.1`). The image builds `agave-validator` and `solana-test-validator` from [anza-xyz/agave](https://github.com/anza-xyz/agave) at that tag, and the build fails if Agave's `rust-toolchain.toml` channel differs from this repository's [rust-toolchain.toml](./rust-toolchain.toml), which must then be updated to match.

- [Cargo.toml](./Cargo.toml) and [logger/Cargo.toml](./logger/Cargo.toml)

  Every `agave-*` and `solana-*` dependency must move together with `AGAVE_VERSION`. Dependabot is configured to ignore those crates (see [.github/dependabot.yml](./.github/dependabot.yml)), so a chain update is the only place they ever change.
