## Chain Update

When doing a chain update, it's important to also modify those location when bumping to right version to ensure proper alignment of Solana validator (agave) version and the Geyser Plugin.

- [Dockerfile](./Dockerfile#L2)

  The line `ARG SOLANA_TAG=v4.1.0-novote` must be updated to use the correct image tag value for our Solana fork at `ghcr.io/streamingfast/solana:${SOLANA_TAG}`.