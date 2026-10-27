## Chain Update

When doing a chain update, it's important to also modify those location when bumping to right version to ensure proper alignment of Solana validator (agave) version and the Geyser Plugin.

- [Dockerfile](./Dockerfile#L2)

  The `ARG SOLANA_TAG=...` line must be updated to use the correct image tag value for our Solana fork at `ghcr.io/streamingfast/solana:${SOLANA_TAG}`.

- [Cargo.toml](./Cargo.toml) and [logger/Cargo.toml](./logger/Cargo.toml)

  Every `agave-*` and `solana-*` dependency must move together with the validator image. Dependabot is configured to ignore those crates (see [.github/dependabot.yml](./.github/dependabot.yml)), so a chain update is the only place they ever change.
