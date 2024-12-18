# firehose-geyser-plugin

This plugin prints "blocks" and "account-blocks" into two different linux named pipes, which can be read by [firecore](https://github.com/streamingfast/firehose-core) readers.

* The account-block corresponds to this "AccountBlock" protobuf definition: https://github.com/streamingfast/firehose-solana/blob/develop/proto/sf/solana/type/v1/account.proto
* The block corresponds to this "Block" protobuf definition: https://github.com/streamingfast/firehose-solana/blob/develop/proto/sf/solana/type/v1/type.proto

## Build the plugin

* From the linux machine that will run agave-validator:

```
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

## Create the `fifo` named pipes and launch the readers

* `mkfifo /path/to/blocks.fifo`
* Launch the block reader in a loop (it should be started BEFORE the agave validator).
```
firecore start reader-node-stdin \
    --config-file= \
    --log-to-file=false \
    --data-dir=/path/to/datablock \
    --common-one-block-store-url=/path/to/oneblock
    --reader-node-grpc-listen-addr=:9000 \
    --reader-node-readiness-max-latency=1h \
    --reader-node-line-buffer-size=838860800 < /path/to/blocks.fifo
```


* `mkfifo /path/to/accounts.fifo`
* Launch the 'account block' reader in a loop (it should be started BEFORE the agave validator).
```
firecore start reader-node-stdin \
    --config-file= \
    --log-to-file=false \
    --data-dir=/path/to/dataaccount \
    --common-one-block-store-url=/path/to/oneblock-accounts
    --reader-node-grpc-listen-addr=:10000 \
    --reader-node-readiness-max-latency=1h \
    --reader-node-line-buffer-size=838860800 < /path/to/accounts.fifo
```

> [!IMPORTANT]  
> The readers must be reading from the fifo files **before** the agave-validator is launched.


## Enable the plugin

* You need a `firehose-geyser-plugin.json` file like this:

```
{
    "libpath": "/path/to/libfirehose_geyser_plugin.so",
    "local_rpc_client": {
        "endpoint": "http://localhost:9000"
    },
    "remote_rpc_client": {
        "endpoint": "https://api.mainnet-beta.solana.com"
    },
    "send_processed": false,
    "account_block_destination_file": "/path/to/accounts.fifo",
    "block_destination_file": "/path/to/blocks.fifo",
    "cursor_file": "/path/to/cursor.fh",
    "noop": false,
    "log": {
        "level": "INFO"
    }
}
```

Flags:
  * `libpath`: points to the `.so` file (under `target/release` when you build it yourself)
  * `local_rpc_client.endpoint`: must point to this node's RPC endpoint to resolve slots and block info
  * `remote_rpc_client.endpoint`: is a failover endpoint, it must point to a valid RPC endpoint for the same chain. It is used only on startup for segments of chain that the local node won't serve.
  * `send_processed`: experimental flag to send blocks before they are confirmed. DO NOT USE, IT CAUSES BLOCK HASHES MISMATCHES ON REORGS !
  * `account_block_destination_file`: path to a linux named pipe where the account blocks will be written. Must be writable and created with `mkfifo /path/to/file`
  * `block_destination_file`: path to a linux named pipe where the normal blocks will be written. Must be writable and created with `mkfifo /path/to/file`
  * `cursor_file`: path where the cursor will be written. This is used for optimizations when restarting the server.
  * `noop`: for debugging - when set to true, blocks are not printed to the FIFO destination files, but a log indicates which block would be written.
  * `log.level`: one of [TRACE, DEBUG, INFO] to get anything interesting.

* agave-validator must be run with the following flag: `--geyser-plugin-config /path/to/libfirehose-geyser-plugin.json`
