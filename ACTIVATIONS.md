# Feature activations

Every feature Agave defines, with its status on the clusters we serve and its effect on the Firehose block. Statuses are the activation slot, `not activated`, or `pending` (activation requested, takes effect at the next epoch).

* Agave version: `v4.4.0-beta.0`
* Statuses checked: 2026-10-08, on `api.mainnet-beta.solana.com` and `api.testnet.solana.com`

Effects:

* **shape**: changes which fields are set, adds or removes a kind of transaction or reward, or changes when the plugin is notified. Needs a plugin or block model decision.
* **values**: changes values the plugin copies unchanged (compute units, fees, balances, logs, errors, account data, which transactions succeed).
* **none**: does not reach anything the plugin receives (networking, shreds, snapshots, CLI).
* **gate removed**: Agave no longer checks the feature, so its behavior does not depend on activation.

On each bump, add the features that are new in the target version, review the ones whose status changed, and refresh the statuses. `solana -um feature status --display-all` (and `-ut`), with a CLI built from the target version, lists them all.

## Features with a shape effect

* `alpenglow` (mainnet: not activated, testnet: 444620256): Block time comes from the Alpenglow footer instead of PoH clock (runtime/src/bank.rs:2136-2143); the block footer is only delivered through notify_block_footer, with no model field; votes move to BLS certificates; vote rewards and VAT burn become account writes (bank.rs:2822).
* `alpenglow_fast_leader_handover` (mainnet: not activated, testnet: not activated): UpdateParent markers let a slot's parent change after replay started and replay restart from a later FEC set (ledger/src/blockstore_processor.rs:1409,2051; core/src/replay_stage/update_parent.rs:152); transactions from the cleared bank reach the plugin, which keys by slot and ignores bank_id. Feature ID has no private key yet.
* `block_revenue_sharing` (mainnet: not activated, testnet: not activated): Block fees split into up to two RewardType::Fee rewards, to the commission collector and the vote account, with commission_bps None (runtime/src/bank/fee_distribution.rs:183-240); a Fee reward can go to an address other than the leader. The model can carry it, consumers assuming one Fee reward to the leader cannot.
* `commission_rate_in_basis_points` (mainnet: 433296000, testnet: 416540256): geyser-plugin-manager/src/block_metadata_notifier.rs:105-114 sends Reward.commission = None and the value in commission_bps; sf.solana.type.v1.Reward has only commission, so it is empty for every Voting and Staking reward.
* `custom_commission_collector` (mainnet: 445392000, testnet: 435116256): Voting and Staking rewards carry neither commission nor commission_bps ((!custom_commission_collector).then_some(..) in runtime/src/bank/partitioned_epoch_rewards/calculation.rs:715,785,796), so the commission rate is not in the block even with a commission_bps field; commission is paid to the vote account's collector (calculation.rs:787, fee_distribution.rs:157), which may not be the vote account.
* `relax_fee_payer_constraint` (mainnet: not activated, testnet: not activated): Replay commits transactions whose fee payer is invalid or unfunded (svm/src/transaction_processor.rs:808-830, runtime/src/bank.rs:4739): status Err, fee 0, no logs, no inner instructions, and the fee payer account may not exist. Such transactions used to make the block invalid; they now appear in blocks. They are counted in executed_transaction_count (runtime/src/bank.rs:4327, was_processed) and sent to Geyser like other committed transactions (rpc/src/transaction_status_service.rs:168), so the plugin's transaction count check still completes the block.

## Features Agave still checks

| Feature | ID | Mainnet | Testnet | Effect | Note |
|---|---|---|---|---|---|
| `alpenglow` | `A1pengvuM6JEcyNuTnMqepBKhwHE3N6PmUrdATGawhJS` | not activated | 444620256 | shape | Block time comes from the Alpenglow footer instead of PoH clock (runtime/src/bank.rs:2136-2143); the block footer is only delivered through notify_block_footer, with no model field; votes move to BLS certificates; vote rewards and VAT burn become account writes (bank.rs:2822). |
| `alpenglow_fast_leader_handover` | `FastLeaderHandover11111111111111111111111111` | not activated | not activated | shape | UpdateParent markers let a slot's parent change after replay started and replay restart from a later FEC set (ledger/src/blockstore_processor.rs:1409,2051; core/src/replay_stage/update_parent.rs:152); transactions from the cleared bank reach the plugin, which keys by slot and ignores bank_id. Feature ID has no private key yet. |
| `block_revenue_sharing` | `7MYx95UBiJufqnumyN7HfskJ9vKdcGMmhreVguqrE97K` | not activated | not activated | shape | Block fees split into up to two RewardType::Fee rewards, to the commission collector and the vote account, with commission_bps None (runtime/src/bank/fee_distribution.rs:183-240); a Fee reward can go to an address other than the leader. The model can carry it, consumers assuming one Fee reward to the leader cannot. |
| `commission_rate_in_basis_points` | `Eg7tXEwMZzS98xaZ1YHUbdRHsaYZiCsSaR6sKgxreoaj` | 433296000 | 416540256 | shape | geyser-plugin-manager/src/block_metadata_notifier.rs:105-114 sends Reward.commission = None and the value in commission_bps; sf.solana.type.v1.Reward has only commission, so it is empty for every Voting and Staking reward. |
| `custom_commission_collector` | `3HcSrCTGXTUnrTueHi4DAwNuMxZSsm5xui2Ax3mgxHqf` | 445392000 | 435116256 | shape | Voting and Staking rewards carry neither commission nor commission_bps ((!custom_commission_collector).then_some(..) in runtime/src/bank/partitioned_epoch_rewards/calculation.rs:715,785,796), so the commission rate is not in the block even with a commission_bps field; commission is paid to the vote account's collector (calculation.rs:787, fee_distribution.rs:157), which may not be the vote account. |
| `relax_fee_payer_constraint` | `FEEXbxUuKobtrt1qNK5pjtzbPQhsppBTrNNG74xu4mai` | not activated | not activated | shape | Replay commits transactions whose fee payer is invalid or unfunded (svm/src/transaction_processor.rs:808-830, runtime/src/bank.rs:4739): status Err, fee 0, no logs, no inner instructions, and the fee payer account may not exist. Such transactions used to make the block invalid; they now appear in blocks. They are counted in executed_transaction_count (runtime/src/bank.rs:4327, was_processed) and sent to Geyser like other committed transactions (rpc/src/transaction_status_service.rs:168), so the plugin's transaction count check still completes the block. |
| `abort_on_invalid_curve` | `FuS3FPfJDKSNot99ECLXtp3rueq36hMNStJkPJwWodLh` | 311904000 | 300764256 | values | Changes syscall error behavior in syscalls/src/lib.rs, so tx success/failure and logs. |
| `account_data_direct_mapping` | `CR3dVN2Yoo95Y96kLSTaziWDAQT2MNEpiWh5cqVq2pNE` | not activated | 408332256 | values | Changes how account data is mapped into the VM (program-runtime/src/cpi.rs, serialization.rs); account data, transaction success and compute units. |
| `allow_commission_decrease_at_any_time` | `decoMktMcnmiq6t3u7g5BfgcQu91nKZr6RvMYf9z1Jb` | 304128000 | 299036260 | values | Vote program commission rule; transaction success. |
| `alt_bn128_little_endian` | `bn2oPgpkzQPT3tohMaAsMVGjhDmmDa4jCaVPqCFmtxM` | 425088000 | 406604256 | values | Syscall variants only. |
| `bank_transaction_count_fix` | `Vo5siZ442SaZBKPXNocthiXysNviW4UYPwRFggmbgAp` | 171072012 | 168572256 | values | No remaining non-test reader; executed_transaction_count passed to block metadata (replay_stage.rs:4283) counts failed transactions too. |
| `bls_pubkey_management_in_vote_account` | `AnAP9zPV4KL7czAPQbFhpDKV2tx7g4UGNbK9wvXwjaRo` | 431568000 | 416540256 | values | BLS pubkey in the vote account, vote instruction costs (programs/vote/src/vote_processor.rs:65). |
| `commission_updates_only_allowed_in_first_half_of_epoch` | `noRuG2kzACwgaY7TVmLRnUNPLKNVQE1fb7X55YWBehp` | 210384016 | 197948256 | values | Vote program rule on when commission may change; transaction success. |
| `create_account_allow_prefund` | `6sPDzwyARRExKH52LECxcGoqziH8G7SZofwuxi8Ja331` | 422928004 | 406604256 | values | New system instruction CreateAccountAllowPrefund (programs/system/src/system_processor.rs:537); instruction data is copied unchanged. |
| `curve25519_syscall_enabled` | `7rcw5UtqgDTBBv2EcynNfYckgdAaH1MAsCjKgXMkN7Ri` | 275184000 | 268364256 | values | Syscall availability; transaction success and compute units. |
| `define_ltds_fee_only_semantics` | `LTDSzjZKFJMKHYpNycG1FrWwGGTaFFwqEFjB5GGLNVD` | 435456000 | 417836256 | values | Loaded data size recorded for fee-only transactions (svm/src/account_loader.rs:460). |
| `delay_commission_updates` | `76dHtohc2s5dR3ahJyBxs7eJJVipFkaPdih9CLgTTb4B` | 428112000 | 406604256 | values | Voting rewards use the previous epoch's commission (partitioned_epoch_rewards/calculation.rs:747); values only. |
| `deplete_cu_meter_on_vm_failure` | `B7H2caeia4ZFcpE3QcgMqbiWiBtWrdBRBSJ1DY6Ktxbq` | 327888000 | 319340257 | values | program-runtime/src/vm.rs:371 depletes compute meter on VM errors, changing compute_units_consumed. |
| `deprecate_legacy_vote_ixs` | `depVvnQ2UysGrhwdiwU42tCadZL8GcBb1i2GYhMopQv` | not activated | not activated | values | Banking stage drops legacy vote packets (core/src/banking_stage/latest_validator_vote_packet.rs:45) and the vote program rejects them; vote transactions use TowerSync only. |
| `deprecate_rent_exemption_threshold` | `rent6iVy6PDoViPBeJ6k5EJQrkj62h7DPyLbWGHwjrC` | 407376000 | 386300256 | values | runtime/src/bank.rs:6347,6424 changes rent exemption threshold to 1.0, altering minimum balances and tx outcomes. |
| `direct_account_pointers_in_program_input` | `ptr9umikaeAS7ZBBp2fsfRhie16F1V2jCKA2y6gXNAK` | not activated | 418268256 | values | Changes program input serialization in program-runtime/src/serialization.rs; tx outcomes/CUs only. |
| `disable_fees_sysvar` | `JAN1trEUEtZjgXYzNBYHU9DYd7GnThhXfFP7SzPXkPsG` | 208656004 | 213500260 | values | syscalls/src/lib.rs:327,444 only gates registering the sol_get_fees_sysvar syscall; affects which txs succeed. |
| `disable_sbpf_v0_execution` | `TestFeature11111111111111111111111111111111` | not activated | not activated | values | SBPFv0 programs fail to load (syscalls/src/lib.rs:335); transaction errors and logs. |
| `disable_sbpf_v0_v1_v2_deployment` | `B8JJXCy5amZyWG9r7EnUYLwzXSXTxG7GZ1qZ1qggo83g` | not activated | not activated | values | Deploying old SBPF versions fails (program-runtime/src/deploy.rs:30). |
| `disable_zk_elgamal_proof_program` | `zkdoVwnSFnSLtGJG7irJPEYUpmb4i7sGMGcnN6T9rnC` | 347760000 | 340508256 | values | The zk-elgamal proof program returns an error (programs/zk-elgamal-proof/src/lib.rs:178). |
| `double_disinflation_rate` | `55oikhjJ2LUi1xdgJ17ueRyHFURZEw32asT3iAKfh7gg` | not activated | not activated | values | runtime/src/bank.rs:6357,6374,6483 changes the inflation taper, altering reward amounts only. |
| `enable_alt_bn128_compression_syscall` | `EJJewYSddEEtSZHiqugnvhQHiWyZKjkFDQASd7oKSagn` | 276912000 | 279164260 | values | Enables syscalls; transaction success, logs and compute units. |
| `enable_alt_bn128_g2_syscalls` | `bn1hKNURMGQaQoEVxahcEAcqiX3NwRs6hgKKNSLeKxH` | 425520000 | 406604256 | values | Enables syscalls. |
| `enable_alt_bn128_syscall` | `A16q37opZdQMCbe5qJ6xpBB9usykfv8jZaMkxvZQi4GJ` | 275616000 | 247628260 | values | Syscall registration gate in syscalls/src/lib.rs; changes which txs succeed and CUs. |
| `enable_big_mod_exp_syscall` | `expH2ppKPW2ANEdEmAjfhSEcnBQJfmoX4FjuNpe9ttg` | not activated | not activated | values | Syscall registration gate in syscalls/src/lib.rs; not activated anywhere. |
| `enable_bls12_381_syscall` | `b1sgUiJ3qu7hYm3tNDyyqZNQd6gLGJmJppnLNa93PCQ` | 425952004 | 406604256 | values | Enables syscalls. |
| `enable_bpf_loader_set_authority_checked_ix` | `5x3825XS7M2A3Ekbn5VGGkvFoAg5qrRWkTrY4bARP1GL` | 251424000 | 247628260 | values | Loader-v3 SetAuthorityChecked instruction (programs/bpf_loader/src/lib.rs:640). |
| `enable_get_epoch_stake_syscall` | `FKe75t4LXxGaQnVHdUKM6DSFifVVraGZ8LyNo7oPwy1Z` | 330912000 | 322796256 | values | Enables a syscall (syscalls/src/lib.rs:333). |
| `enable_poseidon_syscall` | `FL9RsQA6TVUoh5xJQ9d936RHSebA1NLQqe3Zv9sXZRpr` | 278208000 | 280892257 | values | Enables a syscall; transaction success, logs and compute units. |
| `enable_sha512_syscall` | `s512oDwgx8hjMnaQjXfqqrZroVj4HvC6TkN3iSSWXCh` | not activated | 416540256 | values | Syscall registration gate in syscalls/src/lib.rs. |
| `fix_alt_bn128_multiplication_input_length` | `bn2puAyxUx6JUabAxYdKdJ5QHbNNmKw8dCGuGCyRrFN` | 361152000 | 346988256 | values | Syscall input-length handling only. |
| `fix_alt_bn128_pairing_length_check` | `bnYzodLwmybj7e1HAe98yZrdJTd7we69eMMLgCXqKZm` | 406944000 | 385868256 | values | Syscall input length check. |
| `formalize_loaded_transaction_data_size` | `DeS7sR48ZcFTUmt5FFEVDr1v1bh73aAbZiZq3SYr8Eh8` | 381888000 | 369884256 | values | Loaded account data size accounting; compute units, fees and load errors. |
| `full_inflation::devnet_and_testnet` | `DT4n6ABDqs6w4bnfwrXT9rsprcPf6cdDga1egctaPkLC` | not activated | not activated | values | Changes the inflation rate (runtime/src/bank.rs:3111) and so reward amounts. |
| `full_inflation::mainnet::certusone::enable` | `7XRJcS5Ud5vxGB54JbK9N2vBZVwnwdBNeJW1ibRgD9gx` | 64800004 | not activated | values | Only feeds full_inflation_features_enabled() in runtime/src/bank.rs:3118/6477, changing inflation amounts (reward lamports) but not reward kinds. |
| `get_sysvar_syscall_enabled` | `CLCoTADvV64PSrnR6QXty6Fwrt9Xc6EdxSJE4wLRePjq` | 321840000 | 316748256 | values | Enables a sysvar-read syscall; execution results only. |
| `increase_cpi_account_info_limit` | `H6iVbVaDZgDphcPbcZwc5LoznMPWQfnJ1AM7L1xzqvt5` | 403056000 | 385868256 | values | Raises a CPI account info limit. |
| `increase_tx_account_lock_limit` | `9LZdXeKGeBV6hRLdxS1rHbHoEUsKqesCC2ZAPTPKJAbK` | pending | not activated | values | Raises the per-transaction account lock cap from 64 to 128 (runtime/src/bank.rs:3849); account key lists are copied unchanged. |
| `last_restart_slot_sysvar` | `HooKD5NC9QNxk25QuzCssB8ecrEzGt6eXEPBUxWp1LaR` | 282096004 | 283916256 | values | Enables a sysvar syscall (syscalls/src/lib.rs:328). |
| `loader_v3_minimum_extend_program_size` | `YbbRLkvenrocjGPGyoQE4wjnvYzTgfsk38NFmcYK7a5` | 432864000 | 416540256 | values | Loader-v3 ExtendProgram minimum size (programs/bpf_loader/src/lib.rs:893). |
| `loader_v3_set_program_data_to_elf_length` | `EhisBfVtGvEA8bVCVN5VMaYEaX6iTfoUrmcDi8LY7Kxy` | not activated | not activated | values | Programdata account size on deploy (programs/bpf_loader/src/lib.rs:380). |
| `move_precompile_verification_to_svm` | `9ypxGLzkMxi89eDerRKXWDXe44UY2z4hBig4mDhNq5Dp` | 328320000 | 320636257 | values | Moves precompile signature verification into SVM; changes where precompile failures are detected and errors reported, but the tx is still carried with status/error. |
| `move_stake_and_move_lamports_ixs` | `7bTK6Jis8Xpfrs8ZoUfiMDPazTcdPcTWheZFJTA5Z6X4` | 314064000 | 302060257 | values | New stake program instructions; transaction success and account data. |
| `pico_inflation` | `4RWNif6C2WCNiKVW7otP4G7dkmkHGyKQWRpuZ1pxKU5m` | 57456000 | 49772256 | values | Only changes the inflation rate used for Voting/Staking reward amounts (runtime/src/bank.rs:3125). |
| `poseidon_enforce_padding` | `poUdAqRXXsNmfqAZ6UqpjbeYgwBygbfQLEvWSqVhSnb` | 406080000 | 385868256 | values | Syscall input validation only. |
| `raise_block_limits_to_100m` | `P1BCUMpAC7V2GRBRiJCNUgpMyWZhoqt3LKo712ePqsz` | 435888000 | 419132256 | values | Raises cost-tracker block limits (runtime/src/bank.rs:5120); more compute per block. |
| `raise_cpi_nesting_limit_to_8` | `6TkHkRmP7JZy1fdM6fg5uXn76wChQBWGokHBJzrLB3mj` | not activated | not activated | values | CPI depth 4 to 8 (program-runtime/src/invoke_context.rs:879): inner instruction stack_height can reach 9; same field type. |
| `reduce_slot_time_to_200ms` | `iBRLjhJnkmDZgNoZRDMW11d8ZV7HvsL3vAyRjZB5npW` | 454464000 | 429068256 | values | Same SlotParams selection (runtime/src/slot_params.rs:198); more slots per second. |
| `reduce_slot_time_to_250ms` | `iBRLMc81UjRa8fn8A6eE8bJTnRbgQoPTynM51akENCV` | 447552000 | 428204256 | values | Selects SLOT_PARAMS_250MS (runtime/src/slot_params.rs:194): shorter slots, cost limits, reward partition size; same callback shapes. |
| `reduce_slot_time_to_300ms` | `iBRLL3k18HST852F1Mf3Lv83waTNQmmqvKDxvYGwQFL` | 441936000 | 427340256 | values | Selects a SlotParams table (runtime/src/slot_params.rs:190); more slots per second. |
| `reduce_slot_time_to_350ms` | `iBRL5RuWhw4yqaAZu96RUULHckHTZAoe2b77qaV38JZ` | 440208000 | 426476256 | values | Selects a SlotParams table (runtime/src/slot_params.rs:186); block_time still comes from the bank clock (replay_stage.rs:4276); more slots per second. |
| `reduce_stake_warmup_cooldown` | `GwtDQBghCTBgmX2cpEGNPxTEBUTQRaDMGTr5qychdGMj` | 244080000 | 247628260 | values | No runtime gate beyond feature-set and CLI; affects stake warmup rates, hence reward lamports and stake account data. |
| `reenable_sbpf_v0_execution` | `TestFeature21111111111111111111111111111111` | not activated | not activated | values | Test-feature id gating SBPFv0 execution in syscalls/runtime; only changes which txs succeed. |
| `reenable_zk_elgamal_proof_program` | `zkexuyPRdyTVbZqEAREueqL2xvvoBhRgth9xGSc1tMN` | 424224000 | 406604256 | values | programs/zk-elgamal-proof/src/lib.rs:181 gates whether the program executes; affects tx success only. |
| `relax_post_exec_min_balance_check` | `BY4JhHLahVzS9ynfDz4exzGPbVXhFmJvEyMWsXbDBqME` | 443664000 | 434252256 | values | Relaxes min-balance checks after execution (svm/src/account_loader.rs, fee_distribution.rs:266). |
| `relax_programdata_account_check_migration` | `rexav5eNTUSNT1K2N7cfRjnthwhcP5BC25v2tA4rW4h` | 412992000 | 395372256 | values | Relaxes core-BPF migration check in runtime/src/bank.rs:6499-6567; only affects which migrations succeed. |
| `remaining_compute_units_syscall_enabled` | `5TuppMutoyzhUSfuYdhgzD47F92GL1g89KpCZQKqedxP` | not activated | not activated | values | Syscall registration gate in syscalls/src/lib.rs; not activated. |
| `remove_bpf_loader_incorrect_program_id` | `2HmTkCj9tXuPE4ueHzdD7jPeMf9JGCoZh5AsyoATiWEe` | 237168000 | 224300256 | values | Changes which BPF loader instructions fail. |
| `remove_inactive_stakes` | `RMsTKfD6hZnBhhNvgGBeKNrqCNkeoP3DYYxNtcuWtRg` | not activated | not activated | values | Inactive delegations are skipped in reward calculation (partitioned_epoch_rewards/calculation.rs:858); fewer Staking rewards. |
| `replace_spl_token_with_p_token` | `ptokFjwyJtrwCa9Kgo9xoDS59V4QccBGEaRFnRPnSdP` | 419472000 | 396236256 | values | runtime/src/bank.rs:6493 swaps the SPL Token program account data via core-BPF migration (account update for program/programdata). |
| `set_lamports_per_byte_to_1322` | `rntD7invRBswCAdKtRsh1G4psKjrPdS3BKqtnA78C7N` | not activated | not activated | values | Rent lamports per byte (runtime/src/bank.rs:6447). |
| `set_lamports_per_byte_to_2575` | `rntCigrTppP5JdZz7K8TyN9sMzLdAcXp8SejYpVpX6D` | not activated | not activated | values | Rewrites the Rent sysvar (runtime/src/bank.rs:6443) and the rent-exempt minimum. |
| `set_lamports_per_byte_to_5080` | `61BtM7BkDEE8Yq5fskEVAQT9mYA8qCejJWoLe5apqg81` | 446256000 | 437708256 | values | runtime/src/bank.rs:6439 changes rent lamports-per-byte, changing minimum balances. |
| `set_lamports_per_byte_to_6333` | `4a6f7o7iTcA8hRDCrPLkSatnt5Ykxiu36wo5p1Tt12wC` | 444096000 | 434684256 | values | Rent lamports per byte (runtime/src/bank.rs:6435). |
| `set_lamports_per_byte_to_696` | `rntTjNZ9boq8owDxjGVFHPfWNQPDaKiM5JcjxmDGg47` | not activated | not activated | values | Same rent lamports-per-byte switch in runtime/src/bank.rs. |
| `set_lamports_per_byte_to_6960` | `rnt8ZQpz2HYhX3DkYBDGjJS1a36mYq69oXka7JrhEdi` | not activated | not activated | values | Same rent lamports-per-byte switch in runtime/src/bank.rs. |
| `simplify_alt_bn128_syscall_error_codes` | `JDn5q3GBeqzvUa7z67BbmVHVdE3EbUAjvFep3weR3jxX` | 274320000 | 278300256 | values | Syscall return codes only; affects transaction logs and results. |
| `syscall_parameter_address_restrictions` | `EDGMC5kxFxGk4ixsNkGt8bW7QL5hDMXnbwaZvYMwNfzF` | 429840000 | 407468256 | values | Stricter pointer checks in syscalls; transaction status, logs and compute units. |
| `timely_vote_credits` | `tvcF6b1TRz353zKuhBjinZkKzjmihXmBAHJdjNYw1sQ` | 303696000 | 299036260 | values | Vote credit calculation; vote account data and reward amounts. |
| `upgrade_bpf_stake_program_to_v5` | `STk5Xj8hdAx3sTzmtJ3QysKkq6X2A3yj73JtxttiRyk` | 427248000 | 407036256 | values | Replaces the Core BPF Stake program account at activation (runtime/src/bank.rs:6509). |
| `virtual_address_space_adjustments` | `7VgiehxNxu53KdxgLspGQY8myE6f7UokaWa4jsGcaSz` | pending | 407900256 | values | Changes program input serialization/memory mapping in program-runtime (serialization.rs, vm.rs) so tx results/CUs/account data may differ, but no field or kind change. |
| `vote_account_initialize_v2` | `VoteAccount1nitia1izeV211111111111111111111` | not activated | not activated | values | New vote program InitializeAccountV2 instruction (programs/vote/src/vote_processor.rs:69). |
| `vote_authorize_with_seed` | `6tRxEYKuy2L5nnv5bgn7iT28MxUbYxp5h7F3Ncf1exrT` | 148608004 | 144380257 | values | Vote program instruction; transaction success and vote account data. |
| `vote_state_v4` | `Gx4XFcrVMt4HUvPzTpTSVkdDVgcDSjKhDN1RqRS6KDuZ` | 409968000 | 387596256 | values | New vote account data layout (programs/vote/src/vote_state/mod.rs:977); bytes copied unchanged. |
| `zk_elgamal_proof_program_enabled` | `zkhiy5oLowR7HY4zogXjCjeMXyruLqBwSWH21qcFtnv` | 315792000 | 302924256 | values | Enables the builtin ZkElGamal program (builtins/src/lib.rs:105), adding an account update for the program account; plain data. |
| `accounts_lt_hash` | `LTHasHQX6661DaDD4S6A2TFi6QBuiwXKv66fB1obfHq` | 347328000 | 335324256 | none | Bank hash and snapshot format only (runtime/src/bank/accounts_lt_hash.rs). |
| `blake3_syscall_enabled` | `HTW2pSyErTj4BV6KBM9NZ9VBUJVxt7sacNWcf76wtzb3` | not activated | not activated | none | Only registers a BPF syscall (syscalls/src/lib.rs:323). |
| `credits_auto_rewind` | `BUS12ciZ5gCoFafUHWW8qaFMMtwFQGVxjsDheWLdqBE2` | 200448008 | 195356264 | none | No runtime reader remains; only referenced by ledger-tool/src/main.rs:2825. |
| `deprecate_rewards_sysvar` | `GaBtBJvmS4Arjj5W1NmFcyvPjsHN38UGYDq2MDwbs9Qu` | 55728001 | 39404256 | none | Only used as a feature id in ledger-tool/src/main.rs:2843; no runtime reader. |
| `enforce_correct_proof_size` | `turbzzBJLGMJJikLvgCCJu9e1hTmfxwarrbLndYAsK5` | not activated | not activated | none | Shred Merkle proof size enforcement in ledger/gossip only. |
| `full_inflation::mainnet::certusone::vote` | `BzBBveUDymEYoYzcMWNQCx3cd4jQs7puaVFHLtsbB6fm` | 64800004 | not activated | none | Governance vote feature consumed only by the full_inflation activation logic. |
| `set_exempt_rent_epoch_max` | `5wAGiy15X1Jb2hkHnPDCM8oB9V42VNA9ftNVFK84dEgv` | 246240000 | 247628260 | none | Only defined in feature-set and a conformance comment (conformance/src/txn.rs:460); no runtime reader changes account rent_epoch. |
| `switch_to_chacha8_turbine` | `CHaChatUnR3s6cPyPMMGNJa3VdQQ8PNH2JqdD4LpCKnB` | 408672000 | 387164256 | none | Turbine shuffle RNG only (turbine/src/cluster_nodes.rs). |
| `verify_retransmitter_signature` | `51VCKU5eV6mcTc9q9ArfWELU2CqDoi13hdAjr6fHMdtv` | not activated | not activated | none | Turbine shred signature verification only (turbine/src/sigverify_shreds.rs). |
| `zk_token_sdk_enabled` | `zk1snxsc6Fh3wsGNbbHAJNHiJoYgF29mMnTSusGx5EJ` | not activated | not activated | none | Only gates registration of the deprecated zk token proof builtin (builtins/src/lib.rs:95). |

## Features whose gate is removed

| Feature | ID | Mainnet | Testnet | Description |
|---|---|---|---|---|
| `account_hash_ignore_slot` | `SVn36yVApPLYsa8koK3qUcy14zXDnqkNYWyUh1f4oK1` | 225504004 | 217388260 | ignore slot when calculating an account hash #28420 |
| `add_compute_budget_program` | `4d5AKtxoh93Dwm1vHXUU3iRATuMndx1c431KgT2td52r` | 117072004 | 105068256 | Add compute_budget_program |
| `add_get_minimum_delegation_instruction_to_stake_program` | `St8k9dVXP97xT6faW24YmRSYConLbhsMJA4TJTBLmMT` | 199584000 | 195356264 | add GetMinimumDelegation instruction to stake program |
| `add_get_processed_sibling_instruction_syscall` | `CFK1hRCNy8JJuAAY8Pb2GjLFNdCThS2qwZNe3izzBMgn` | 134352008 | 124508260 | add add_get_processed_sibling_instruction_syscall |
| `add_new_reserved_account_keys` | `8U4skmMVnF6k2kMvrWbQuRUT3qQSiTYpSjqmhmgfthZu` | 320976000 | 316316256 | SIMD-0105: Maintain Dynamic Set of Reserved Account Keys |
| `add_set_compute_unit_price_ix` | `98std1NSHqXi9WYvFShfVepRdCoq1qvsp8fsR2XZtG8g` | 142128000 | 137900256 | add compute budget ix for setting a compute unit price |
| `add_set_tx_loaded_accounts_data_size_instruction` | `G6vbf1UBok8MWb8m25ex86aoQHeKTzDKzuZADHkShqm6` | 231984004 | 217388260 | add compute budget instruction for setting account data size per transaction #30366 |
| `add_shred_type_to_shred_seed` | `Ds87KVeqhbv7Jw8W6avsS1mqz3Mw5J3pRTpPoDQ2QdiJ` | 137376016 | 136172256 | add shred-type to shred seed #25556 |
| `allow_votes_to_directly_update_vote_state` | `Ff8b1fBeB86q8cjq47ZhsQLgv5EkHu3G1C99zjUfAzrq` | 212112000 | 217388260 | enable direct vote state update |
| `apply_cost_tracker_during_replay` | `2ry7ygxiYURULZCrypHhveanvP5tzZ4toRwVp89oCNSj` | 329616000 | 322364256 | apply cost tracker to blocks during replay #29595 |
| `better_error_codes_for_tx_lamport_check` | `Ffswd3egL3tccB6Rv3XY6oqfdzn913vUcjCSnpvCKpfx` | 222480020 | 224732260 | better error codes for tx lamport check #33353 |
| `cap_accounts_data_allocations_per_transaction` | `9gxu85LYRAcZL38We8MYJ4A9AwgBBPtVBAqebMcT1241` | 224640020 | 217388260 | cap accounts data allocations per transaction #27375 |
| `cap_bpf_program_instruction_accounts` | `9k5ijzTbYPtjzu8wj2ErH9v45xecHzQ1x4PMYMMxFgdM` | 205200004 | 195356264 | enforce max number of accounts per bpf program instruction #26628 |
| `cap_transaction_accounts_data_size` | `DdLwVYuvDz26JohmgSbA7mjpJFgX5zP2dkp8qsF2C33V` | 230256020 | 217388260 | cap transaction accounts data size up to a limit #27839 |
| `chained_merkle_conflict_duplicate_proofs` | `chaie9S2zVfuxJKNRGkyTDokLwWxx6kD2ZLsqQHaDD8` | not activated | not activated | generate duplicate proofs for chained merkle root conflicts |
| `check_init_vote_data` | `3ccR6QpxGYsAbWyfevEtBNGfWV4xBffxRj2tD6A9i39F` | 68688000 | 67052260 | check initialized Vote data |
| `check_physical_overlapping` | `nWBqjr3gpETbiaVj3CBJ3HFC5TMdnJDGt21hnvSTvVZ` | 142560008 | 139196256 | check physical overlapping regions |
| `check_slice_translation_size` | `GmC19j9qLn2RFk5NduX6QXaDhVpGncVVBzyM8e9WMz2F` | 207792008 | 195356264 | check size when translating slices |
| `check_syscall_outputs_do_not_overlap` | `3uRVPBpyEJRo1emLCrq38eLRFGcu6uKSpUXqGvU8T7SZ` | 174096000 | 165980260 | check syscall outputs do_not overlap #28600 |
| `checked_arithmetic_in_fee_validation` | `5Pecy6ie6XGm22pc9d4P9W5c31BugcFBuy6hsP2zkETv` | 234576004 | 217388260 | checked arithmetic in fee validation #31273 |
| `clean_up_delegation_errors` | `Bj2jmUsM2iRhfdLLDSTkhM5UQRQvQHm57HSmPibPtEyu` | 211680000 | 201404260 | Return InsufficientDelegation instead of InsufficientFunds or InsufficientStake where applicable #31206 |
| `compact_vote_state_updates` | `86HpNqzutEZwLcPxS6EHDcMNYWk6ikhteg9un7Y2PBKE` | 212112000 | 217388260 | Compact vote state updates to lower block size |
| `consume_blockstore_duplicate_proofs` | `6YsBCejwK96GZCkJ6mkZ4b68oP63z2PLoQmWjC7ggTqZ` | 260064000 | 259724256 | consume duplicate proofs from blockstore in consensus #34372 |
| `cost_model_requested_write_lock_cost` | `wLckV1a64ngtcKPRGU4S4grVTestXjmNjxBjaKZrAcn` | 312336000 | 301196256 | cost model uses number of requested write locks #34819 |
| `curve25519_restrict_msm_length` | `eca6zf6JJRjQsYYPkBHF3N32MTzur4n2WL4QiiacPCL` | 262224008 | 264044260 | restrict curve25519 multiscalar multiplication vector lengths #34763 |
| `dedupe_config_program_signers` | `8kEuAshXLsgkUEdcFVLqrjCGGHVWFW99ZZpxvAzzMtBp` | 110592000 | 86060263 | dedupe config program signers |
| `default_units_per_instruction` | `J2QdYx8crLbTVK8nur1jeLsmc3krDbfjoxoea2V1Uy5Q` | 141264004 | 135308256 | Default max tx-wide compute units calculated per instruction |
| `delay_visibility_of_program_deployment` | `GmuBvtFb2aHfSfMXpuFeWZGHyDeCLPS79s48fmCWCfM5` | 228960012 | 217388260 | delay visibility of program upgrades #30085 |
| `demote_program_write_locks` | `3E3jV7v9VcdJL8iYZUMax9DiDno8j7EWUVbhm9RtShj2` | 100656000 | 96860257 | demote program write locks to readonly, except when upgradeable loader present #19593 #20265 |
| `deprecate_unused_legacy_vote_plumbing` | `6Uf8S75PVh91MYgPQSHnjRAPQq6an5BDv9vomrCwDqLe` | 283392000 | 284348256 | Deprecate unused legacy vote tx plumbing |
| `disable_account_loader_special_case` | `EQUMpNFr7Nacb1sva56xn1aLfBxppEoSBH8RRVdkcD1x` | 314496000 | 302492256 | Disable account loader special case #3513 |
| `disable_bpf_deprecated_load_instructions` | `3XgNukcZWf9o3HdA3fpJbm94XFc4qpvTXc8h1wxYwiPi` | 139104000 | 124508260 | disable ldabs* and ldind* SBF instructions |
| `disable_bpf_loader_instructions` | `7WeS1vfPRgeeoXArLh7879YcB9mgE9ktjPDtajXeWfXn` | 259632004 | 257132256 | disable bpf loader management instructions #34194 |
| `disable_bpf_unresolved_symbols_at_runtime` | `4yuaYAj2jGMGTh1sSmi4G2eFscsDq8qjugJXZoBN6YEa` | 139104000 | 124508260 | disable reporting of unresolved SBF symbols at runtime |
| `disable_builtin_loader_ownership_chains` | `4UDcAfQ6EcA6bdcadkeHpkarkhZGJ7Bpq7wTAiRMjkoi` | 207360004 | 199244256 | disable builtin loader ownership chains #29956 |
| `disable_cpi_setting_executable_and_rent_epoch` | `B9cdB55u4jQsDNsdTK525yE9dmSc5Ga7YBaBrDFvEhM9` | 225936004 | 219116256 | disable setting is_executable and_rent_epoch in CPI #26987 |
| `disable_deploy_of_alloc_free_syscall` | `79HWsX9rpnnJBPcdNURVqygpMAfxdrAirzAGAVmf92im` | 209088008 | 195356264 | disable new deployments of deprecated sol_alloc_free_ syscall |
| `disable_deprecated_loader` | `GTUMCZ8LTNxVfxdrw7ZsDFTxXb7TutYkzJnFwinpE6dg` | 167184008 | 158204260 | disable the deprecated BPF loader |
| `disable_fee_calculator` | `2jXx2yDmGysmBKfKYNgLj2DQyAQv6mMk2BPh4eSbyB4H` | 147744004 | 114140256 | deprecate fee calculator |
| `disable_partitioned_rent_collection` | `2B2SBNbUcr438LtGXNcJNBP2GBSxjx81F945SdSkUSfC` | 349056000 | 337916256 | SIMD-0175: Disable partitioned rent collection #4562 |
| `disable_rehash_for_rent_epoch` | `DTVTkmw3JSofd8CJVJte8PXEbxNQ2yZijvVr3pe2APPj` | 204336000 | 195356264 | on accounts hash calculation, do not try to rehash accounts #28934 |
| `disable_rent_fees_collection` | `CJzY83ggJHqPGDq8VisV3U91jDJLuEaALZooBrXtnnLU` | 326592000 | 316748256 | SIMD-0084: Disable rent fees collection |
| `disable_turbine_fanout_experiments` | `turbnbNRp22nwZCmgVVXFSshz7H7V23zMzQgA46YpmQ` | 380160000 | 368156256 | disable turbine fanout experiments #29393 |
| `discard_unexpected_data_complete_shreds` | `disCA4efguFL6Wqa4pGdG7jpjC7C5uiKzKnhEBqchBe` | 434160000 | 416972256 | SIMD-0337: Markers for Alpenglow Fast Leader Handover, DATA_COMPLETE_SHRED placement rules |
| `do_support_realloc` | `75m6ysz33AfLA5DDEzWM1obBrnPQRSsdVQ2nRmc8Vuu1` | 133920008 | 125804256 | support account data reallocation |
| `drop_legacy_shreds` | `GV49KKQdBNaiv2pgqhS2Dy3GWYJGXMTVYbYkdk91orRy` | 259200004 | 255836256 | drops legacy shreds #34328 |
| `drop_redundant_turbine_path` | `4Di3y24QFLt5QEUPZtbnjyfQKfm6ZMTfa6Dw1psfoMKU` | 199152000 | 196220264 | drop redundant turbine path |
| `drop_unchained_merkle_shreds` | `5KLGJSASDVxKPjLCDWNtnABLpZjsQSrYZ8HKwcEdAMC8` | 354672000 | 344828256 | drops unchained Merkle shreds #2149 |
| `ed25519_precompile_verify_strict` | `ed9tNscbWLYBooxWA7FE2B5KHWs8A6sxfY8EzezEcoo` | 308448000 | 299900256 | SIMD-0152: Use strict verification in ed25519 precompile |
| `ed25519_program_enabled` | `6ppMXNYLhVd7GcsZ5uV11wQEW7spppiMVfqQv5SXhDpX` | 117936008 | 112412257 | enable builtin ed25519 signature verify program |
| `enable_bpf_loader_extend_program_ix` | `8Zs9W7D9MpSEtUWSQdGniZk2cNmV22y6FLJwCx53asme` | 229824000 | 217388260 | enable bpf upgradeable loader ExtendProgram instruction #25234 |
| `enable_chained_merkle_shreds` | `7uZBkJXJ1HkuP6R3MJfZs7mLwymBcDbKdqbF51ZWLier` | 306720000 | 299036260 | Enable chained Merkle shreds #34916 |
| `enable_durable_nonce` | `4EJQtF2pkRyawwcTVfQutzq4Sa5hRhibF6QAK1QXhtEX` | 138240000 | 137036260 | enable durable nonce #25744 |
| `enable_early_verification_of_account_modifications` | `7Vced912WrRnfjaiKRiNBcbuFw7RrnLv3E3z95Y4GTNc` | 222048008 | 217388260 | enable early verification of account modifications #25899 |
| `enable_extend_program_checked` | `ExtendProgCheckedWi11BeDe1eted11111111111111` | not activated | not activated | Enable ExtendProgramChecked instruction |
| `enable_gossip_duplicate_proof_ingestion` | `FNKCMBzYUdjhHyPdsKG2LSmdzH8TCHXn3ytj8RNBS4nG` | 284688004 | 285212256 | enable gossip duplicate proof ingestion #32963 |
| `enable_loader_v4` | `LoaderV4WasAbandoned11111111111111111111111` | not activated | not activated | SIMD-0167: Enable Loader-v4 |
| `enable_program_redeployment_cooldown` | `J4HFT8usBxpcF63y46t1upYobJgChmKyZPm5uTBRg25Z` | 228960012 | 217388260 | enable program redeployment cooldown #29135 |
| `enable_request_heap_frame_ix` | `Hr1nUA9b7NJ6eChS26o7Vi8gYYDDwWD3YeBfzJkTbU86` | 217296000 | 220844260 | Enable transaction to request heap frame using compute budget instruction #30076 |
| `enable_sbpf_v1_deployment_and_execution` | `JE86WkYvTrzW8HgNmrHY7dFYpCmSptUpKupbo2AdQ9cG` | 349488000 | 338780256 | SIMD-0166: Enable deployment and execution of SBPFv1 programs |
| `enable_sbpf_v2_deployment_and_execution` | `F6UVKh1ujTEFK3en2SyAL3cdVnqko1FVEXWhmdLRu6WP` | 356400000 | 346124256 | SIMD-0173 and SIMD-0174: Enable deployment and execution of SBPFv2 programs |
| `enable_sbpf_v3_deployment_and_execution` | `5cC3foj77CWun58pC51ebHFUWavHWKarWyR5UUik7dnC` | 428976000 | 406604256 | SIMD-0178, SIMD-0189 and SIMD-0377: Enable deployment and execution of SBPFv3 programs |
| `enable_secp256r1_precompile` | `srremy31J5Y25FrAApwVb9kZcfXbusYMMsvTK9aWv5q` | 345600000 | 332732256 | SIMD-0075: Enable secp256r1 precompile |
| `enable_tower_sync_ix` | `tSynMCspg4xFiCj1v3TDb4c7crMR5tSBhLz4sF7rrNA` | 323568000 | 316748256 | Enable tower sync vote instruction |
| `enable_transaction_loading_failure_fees` | `PaymEPK2oqwT9TXAVfadjztH2H6KfLEB9Hhd5Q5frvP` | 327024000 | 317612256 | SIMD-0082: Enable fees for some additional transaction failures |
| `enable_turbine_fanout_experiments` | `D31EFnLgdiysi84Woo3of4JMu7VmasUS3Z7j9HYXCeLY` | 247104008 | 247628260 | enable turbine fanout experiments #29393 |
| `enable_tx_v1` | `txv1aq4pp281K9um3tnPgkfX8UqtFT6wcVW3hNezGLL` | 447120000 | 437276256 | SIMD-0385: Transaction V1 |
| `enable_vote_address_leader_schedule` | `5JsG4NWH8Jbrqdd8uL6BNwnyZK3dQSoieRXG5vmofj9y` | 363312000 | 352604256 | SIMD-0180: Enable vote address leader schedule #4573 |
| `enable_zk_proof_from_account` | `zkiTNuzBKxrCLMKehzuQeKZyLtX2yvFcEKMML8nExU8` | not activated | pending | Enable zk token proof program to read proof from accounts instead of instruction data #34750 |
| `enable_zk_transfer_with_fee` | `zkNLP7EQALfC1TYeB3biDU7akDckj8iPkvh9y2Mt2K3` | not activated | not activated | enable Zk Token proof program transfer with fee |
| `enforce_fixed_fec_set` | `fixfecLZYMfkGzwq6NJA11Yw6KYztzXiK9QcL3K78in` | 403920000 | 385868256 | SIMD-0317: Enforce 32 data + 32 coding shreds |
| `epoch_accounts_hash` | `5GpmAKxaGsWWbPp4bNXFLJxZVvG92ctxf7jQnzTQjF3n` | 228528004 | 217388260 | enable epoch accounts hash calculation #27539 |
| `error_on_syscall_bpf_function_hash_collisions` | `8199Q2gMD2kwgfopK5qqVWuDbegLgpuFUFHCcUJQDN8b` | 280800004 | 283484257 | error on bpf function hash collisions |
| `evict_invalid_stakes_cache_entries` | `EMX9Q7TVFAmQ9V1CggAkhMzhXSg8ECp7fHrWQX2G1chf` | 117072004 | 113708264 | evict invalid stakes cache entries on epoch boundaries |
| `executables_incur_cpi_data_cost` | `7GUcYgq4tVtaqNCKT3dho9r4665Qp5TxCZ27Qgjx3829` | 139536000 | 137468257 | Executables incur CPI data costs |
| `filter_stake_delegation_accounts` | `GE7fRxmW46K6EmCD9AMZSbnaJ2e3LfqCZzdHi9hmYAgi` | 57888004 | 54092257 | filter stake_delegation_accounts #14062 |
| `filter_votes_outside_slot_hashes` | `3gtZPqvPpsbXZVCx6hceMfWxtsmrjMzmg8C7PLKSxS2d` | 157680012 | 130556260 | filter vote slots older than the slot hashes history |
| `fix_recent_blockhashes` | `6iyggb5MTcsvdcugX7bEKbHV8c6jdLbpHwkncrgLMhfo` | 204768000 | 195356264 | stop adding hashes for skipped slots to recent blockhashes |
| `fixed_memcpy_nonoverlapping_check` | `36PRUK2Dz6HWYdG9SpjeAsF5F3KxnFCakA2BZMbtMhSb` | 137808012 | 136172256 | use correct check for nonoverlapping regions in memcpy syscall |
| `include_account_index_in_rent_error` | `2R72wpcQ7qV7aTJWUumdn8u5wmmTyXbK7qzEy7YSAgyY` | 154224000 | 144812257 | include account index in rent tx error #25190 |
| `include_loaded_accounts_data_size_in_fee_calculation` | `EaQpmC6GtRssaZ3PCUM5YksGqUdMLeZ46BQXYtHYakDS` | not activated | not activated | include transaction loaded accounts data size in base fee calculation #30657 |
| `incremental_snapshot_only_incremental_hash_calculation` | `25vqsfjk7Nv1prsQJmA4Xu1bN61s8LXCBGUPp8Rfy1UF` | 243648004 | 247628260 | only hash accounts in incremental snapshot during incremental snapshot creation #26799 |
| `index_erasure_conflict_duplicate_proofs` | `dupPajaLy2SSn8ko42aZz4mHANDNrLe8Nw8VQgFecLa` | 260496000 | 261020256 | generate duplicate proofs for index and erasure conflicts #34360 |
| `instructions_sysvar_owned_by_sysvar` | `H3kBSaKdeiUsyHmeHqjJYNc27jesXZ6zWj3zWkowQbkV` | 152496000 | 113708264 | fix owner for instructions sysvar |
| `leave_nonce_on_success` | `E8MkiWZNNPGU6n55jkGzyj8ghUmjCHRmDFdYYFYHxWhQ` | 133056012 | 114140256 | leave nonce as is on success |
| `libsecp256k1_0_5_upgrade_enabled` | `DhsYfRjxfnh2g7HKJYSzT79r74Afa1wbHkAgHndrA1oy` | 110592000 | 86060263 | upgrade libsecp256k1 to v0.5.0 |
| `libsecp256k1_fail_on_bad_count` | `8aXvSuopd1PUj7UhehfXJRg6619RHp8ZvwTyyJHdUYsj` | not activated | not activated | fail libsecp256k1_verify if count appears wrong |
| `libsecp256k1_fail_on_bad_count2` | `54KAoNiUERNoWWUhTWWwXgym94gzoXFVnHyQwPA18V9A` | 200880004 | 195356264 | fail libsecp256k1_verify if count appears wrong |
| `limit_instruction_accounts` | `6aHuNsUmwSzCEMjrBzBCYaxHAyAcQBjVES92JigHBDuC` | 432000000 | 416540256 | SIMD-0406: Maximum instruction accounts |
| `limit_max_instruction_trace_length` | `GQALDaC48fEhZGWRj9iL5Q889emJKcj3aCvHF7VCbbF4` | 224208000 | 217388260 | limit max instruction trace length #27939 |
| `limit_secp256k1_recovery_id` | `7g9EUwj4j7CS21Yx1wvgWLjSZeh5aPq8x9kpoPwXM8n8` | 142560008 | 138764256 | limit secp256k1 recovery id |
| `loosen_cpi_size_restriction` | `GDH5TVdbTPUpRnXaRyQqiKUa7uZAbZ28Q2N9bhbKoMLm` | 312768000 | 301628256 | loosen cpi size restrictions #26641 |
| `mask_out_rent_epoch_in_vm_serialization` | `RENtePQcDLrAbxAsP3k8dwVcnNYQ466hi2uKvALjnXx` | 346032000 | 333596256 | SIMD-0267: Sets rent_epoch to a constant in the VM |
| `max_tx_account_locks` | `CBkDroRDqm8HwHe6ak9cguPjUomrASEkfmxEaZ5CNNxz` | 140400004 | 113708264 | enforce max number of locked accounts per transaction |
| `merge_nonce_error_into_system_error` | `21AWDosvp3pBamFW91KB35pNoaoZVTM7ess8nr2nt53B` | 151632012 | 86060263 | merge NonceError into SystemError |
| `merkle_conflict_duplicate_proofs` | `mrkPjRg79B2oK2ZLgd7S3AfEJaX9B6gAF3H9aEykRUS` | 283824004 | 284780260 | generate duplicate proofs for merkle root conflicts #34270 |
| `migrate_address_lookup_table_program_to_core_bpf` | `C97eKZygrkU4JxJsZdjgbUY7iQR7rKTr4NyDWo2E5pRm` | 329184000 | 321068256 | SIMD-0128: Migrate Address Lookup Table program to Core BPF |
| `migrate_config_program_to_core_bpf` | `2Fr57nzzkLYXW695UdDxDeR5fhnZWSttZeZYemrnpGFV` | 325296000 | 316748256 | SIMD-0140: Migrate Config program to Core BPF |
| `migrate_feature_gate_program_to_core_bpf` | `4eohviozzEeivk1y9UbrnekbAFMDQyJz5JjA9Y6gyvky` | 324864000 | 316748256 | SIMD-0089: Migrate Feature Gate program to Core BPF (programify) |
| `migrate_stake_program_to_core_bpf` | `6M4oQ6eXneVhtLoiAr4yRYQY43eVLjrKbiDZDJc892yk` | 355536000 | 345692256 | SIMD-0196: Migrate Stake program to Core BPF #3655 |
| `move_serialized_len_ptr_in_cpi` | `74CoWuBmt3rUVUrCb2JiSTvh6nXyBWUsK4SaMj3CtE3T` | 202608000 | 195356264 | cpi ignore serialized_len_ptr #29592 |
| `native_programs_consume_cu` | `8pgXCMNXC8qyEFypuwpXyRxLXZdpM4Qo72gJ6k87A6wL` | 240624008 | 245900264 | Native program should consume compute units #30620 |
| `no_overflow_rent_distribution` | `4kpdyrcj5jS47CZb2oJGfVxjYbsMm2Kx97gFyZrxxwXz` | 51408000 | 47612256 | no overflow rent distribution |
| `nonce_must_be_advanceable` | `3u3Er5Vc2jVcwz4xr2GJeSAXT3fAj6ADHZ4BJMZiScFd` | 138240000 | 137036260 | durable nonces must be advanceable |
| `nonce_must_be_authorized` | `HxrEu1gXuH7iD3Puua1ohd5n4iUKJyFNtNxk9DVJkvgr` | 138240000 | 137036260 | nonce must be authorized |
| `nonce_must_be_writable` | `BiCU7M5w8ZCMykVSyhZ7Q3m2SWoR2qrEQ86ERcDX77ME` | 136944004 | 114140256 | nonce must be writable |
| `on_load_preserve_rent_epoch_for_rent_exempt_accounts` | `CpkdQmspsaZZ8FVAouQTtTWZkc8eeQ7V3uj7dWz543rZ` | 204336000 | 195356264 | on bank load account, do not try to fix up rent_epoch #28541 |
| `optimize_epoch_boundary_updates` | `265hPS8k8xJ37ot82KEgjRunsUp5w4n4Q4VwwiN9i9ps` | 109728000 | 99452256 | optimize epoch boundary updates |
| `partitioned_epoch_rewards_superfeature` | `PERzQrt5gBD1XEe2c9XdFWqwgHY3mr7cYWbm5V772V8` | 305424000 | 299468256 | SIMD-0118: replaces enable_partitioned_epoch_reward to enable partitioned rewards at epoch boundary |
| `preserve_rent_epoch_for_rent_exempt_accounts` | `HH3MUYReL2BvqqA3oEcAa7txju5GY6G4nxJ51zvsEjEZ` | 156384000 | 145676256 | preserve rent epoch for rent exempt accounts #26479 |
| `prevent_calling_precompiles_as_programs` | `4ApgRX3ud6p7LNMJmsuaAcZY5HWctGPr5obAsjB3A54d` | 143424004 | 114140256 | prevent calling precompiles as programs |
| `prevent_crediting_accounts_that_end_rent_paying` | `812kqX67odAp5NFwM8D2N24cku7WTm9CHUTFUXaDkWPn` | 161136000 | 146972256 | prevent crediting rent paying accounts #26606 |
| `prevent_rent_paying_rent_recipients` | `Fab5oP3DmsLYCiQZXdjyqT3ukFFPrsmqhXU4WU1AWVVF` | 234144000 | 217388260 | prevent recipients of rent rewards from ending in rent-paying state #30151 |
| `provide_instruction_data_offset_in_vm_r2` | `5xXZc66h4UdB6Yq7FzdBxBiRAFMMScMLwHxk2QZDaNZL` | 410400000 | 388028256 | SIMD-0321: Provide instruction data offset in VM r2 |
| `quick_bail_on_panic` | `DpJREPyuMZ5nDfU6H3WTqSqUFSXAfw8u7xqmWtEwJDcP` | 140400004 | 138332256 | quick bail on panic |
| `raise_account_cu_limit` | `htsptAwi2yRoZH83SKaUXykeZGtZHgxkS2QwW1pssR8` | 379296000 | 368156256 | SIMD-0306: Raise account CU limit to 40% max |
| `raise_block_limits_to_50m` | `5oMCU3JPaFLr8Zr4ct7yFA7jdk6Mw1RmB8K4u9ZbS42z` | 332640000 | 324524256 | SIMD-0207: Raise block limit to 50M |
| `raise_block_limits_to_60m` | `6oMCUgfY6BzZ6jwB681J6ju5Bh6CjVXbd7NeWYqiXBSu` | 355104000 | 345260256 | SIMD-0256: Raise block limit to 60M |
| `record_instruction_in_transaction_context_push` | `3aJdcZqxoLpSBxgeYGjPwaYS1zzcByxUDqJkbzWAH1Zb` | 141696000 | 138764256 | move the CPI stack overflow check to the end of push |
| `reduce_required_deploy_balance` | `EBeznQDjcPG8491sFsKZYBi5S5jTVXMpAKNDJMQPS2kq` | 102816004 | 96860257 | reduce required payer balance for program deploys |
| `reject_callx_r10` | `3NKRSwpySNwD3TvP5pHnRmkAQRsdkXWRr1WaQh8p4PWX` | 279072000 | 283052256 | Reject bpf callx r10 instructions |
| `reject_empty_instruction_without_program` | `9kdtFSrXHQg3hKkbXkQ6trJ3Ja1xpJ22CTFSNAciEwmL` | 137376016 | 113708264 | fail instructions which have native_loader as program_id directly |
| `reject_non_rent_exempt_vote_withdraws` | `7txXZZD6Um59YoLMF7XUNimbMjsqsWhc7g2EniiTrmp1` | 117072004 | 113708264 | fail vote withdraw instructions which leave the account non-rent-exempt |
| `reject_vote_account_close_unless_zero_credit_epoch` | `ALBk3EWdeAg2WAGf6GPDUf1nynyNqCdEVmgouG7rpuCj` | 170640000 | 140924260 | fail vote account withdraw to 0 unless account earned 0 credits in last completed epoch |
| `relax_authority_signer_check_for_lookup_table_creation` | `FKAcEvNgSY79RpqsPNUV5gDyumopH4cEHqUxyfm8b8Ap` | 249264000 | 247628260 | relax authority signer check for lookup table creation #27205 |
| `relax_intrabatch_account_locks` | `4WeHX6QoXCCwqbSFgi6dxnB6QsPo6YApaNTH7P4MLQ99` | 411696000 | 389756256 | SIMD-0083: Allow batched transactions to read/write and write/write the same accounts |
| `remove_accounts_delta_hash` | `LTdLt9Ycbyoipz5fLysCi1NnDnASsZfmJLJXts5ZxZz` | 348624000 | 337484256 | SIMD-0223: removes accounts delta hash |
| `remove_accounts_executable_flag_checks` | `FXs1zh47QbNnhXcnB6YiAQoJ4sGB91tKF3UFHLcKT7PM` | 350352004 | 339212256 | SIMD-0162: Remove checks of accounts is_executable flag |
| `remove_congestion_multiplier_from_fee_calculation` | `A8xyMHZovGXFkorFqEmVH2PKGLiBip5JD7jt4zsUWo4H` | 236736000 | 218684268 | Remove congestion multiplier from transaction fee calculation #29881 |
| `remove_deprecated_request_unit_ix` | `EfhYd3SafzGT472tYQDUc4dPd2xdEfKs5fwkowUgVt4W` | 233280000 | 217388260 | remove support for RequestUnitsDeprecated instruction #27500 |
| `remove_native_loader` | `HTTgmruMYRZEntyL3EdCDdnS6e4D5wRq1FA7kQsb66qq` | 117072004 | 106796256 | remove support for the native loader |
| `remove_rounding_in_fee_calculation` | `BtVN7YjDzNE6Dk7kTT7YTDgMNUZTNgiSJgsdzAeTg2jF` | 317088000 | 303356256 | Removing unwanted rounding in fee calculation #34982 |
| `remove_simple_vote_from_cost_model` | `2GCrNXbzmt4xrwdcKS2RdsLzsgu4V5zHAemW57pcHT6a` | 423792000 | 406604256 | stop use static SimpleVote transaction cost, issue #10227 |
| `rent_for_sysvars` | `BKCPBQQBZqggVnFso5nQ8rQ4RwwogYwjuUt9biBjxwNF` | 104976000 | 100748256 | collect rent from accounts owned by sysvars |
| `requestable_heap_size` | `CCu4boMmfLuqcmfTLPHQiUo22ZdUsXjgzPAURYaWt1Bw` | 135216004 | 124508260 | Requestable heap frame size |
| `require_custodian_for_locked_stake_authorize` | `D4jsDcXaqdW8tDAWn8H4R25Cdns2YwLneujSL1zvjW6R` | 71712000 | 67052260 | require custodian to authorize withdrawer change for locked stake |
| `require_rent_exempt_accounts` | `BkFDxiJQWZXGTZaJQxH7wVEHkAmwCgSEVkrvswFfRJPD` | 133488000 | 125804256 | require all new transaction accounts with data to be rent-exempt |
| `require_rent_exempt_split_destination` | `D2aip4BBr8NPWtU9vLrwrBvbuaQ8w1zV38zFLxx4pfBV` | 254016004 | 247628260 | Require stake split destination account to be rent exempt |
| `require_static_nonce_account` | `7VVhpg5oAjAmnmz1zCcSHb2Z9ecZB2FQqpnEwReka9Zm` | 384480000 | 371180256 | SIMD-0242: Static Nonce Account Only |
| `require_static_program_ids_in_transaction` | `8FdwgyHFEjhAdjWfV2vfqk7wA1g9X3fQpKH7SBpEv3kC` | 153360000 | 144812257 | require static program ids in versioned transactions |
| `reserve_minimal_cus_for_builtin_instructions` | `C9oAhLxDBm3ssWtJx1yBGzPY55r2rArHmN1pbQn6HogH` | 327456000 | 318476256 | SIMD-0170: Reserve minimal CUs for builtin instructions #2562 |
| `return_data_syscall_enabled` | `DwScAzPUjuv65TMbDnFY7AgwmotzWy3xpEJMXM3hZFaB` | 117936008 | 112412257 | enable sol_{set,get}_return_data syscall |
| `revise_turbine_epoch_stakes` | `BTWmtJC8U5ZLMbBUUA1k6As62sYjPEjAiNAT55xYGdJU` | 244944000 | 247628260 | revise turbine epoch stakes |
| `reward_full_priority_fee` | `3opE3EzAKnUftUDURkzMgwpNgimBAypW1mNDYH4x4Zg7` | 320112000 | 315884256 | SIMD-0096: Reward full priority fee to validators |
| `round_up_heap_size` | `CE2et8pqgyQMP2mQRg3CgvX8nJBKUArMu3wfiQiQKY1y` | 235440004 | 217388260 | round up heap size when calculating heap cost #30679 |
| `secp256k1_program_enabled` | `E3PHP7w8kB7np3CTQ1qQ2tW3KCtjRSXBQgW9vM2mWv2Y` | 41040000 | 39404256 | secp256k1 program |
| `secp256k1_recover_syscall_enabled` | `6RvdSWHh8oh72Dp7wMTS2DBkf3fRPtChfNrAo3cZZoXJ` | 104976000 | 86060263 | secp256k1_recover syscall |
| `send_to_tpu_vote_port` | `C5fh68nJ7uyKAuYZg2x9sEQ5YrVf3dkW6oojNBSc3Jvo` | 101088000 | 96860257 | send votes to the tpu vote port |
| `separate_nonce_from_blockhash` | `Gea3ZkK2N4pHuVZVxWcnAtS6UEDdyumdYt4pFcKjA3ar` | 138240000 | 137036260 | separate durable nonce and blockhash domains #25744 |
| `simplify_writable_program_account_check` | `5ZCcFAzJ1zsFKe1KSZa9K92jhx7gkcKj97ci2DBo1vwj` | 241056004 | 245900264 | Simplify checks performed for writable upgradeable program accounts #30559 |
| `skip_rent_rewrites` | `CGB2jM8pwZkeeiXQ66kBMyBR6Np61mggL7XUsmLjVcrw` | 326160000 | 316748256 | SIMD-0183: Skip rent rewrites |
| `snapshots_lt_hash` | `LTsNAP8h1voEVVToMNBNqoiNQex4aqfUrbFhRH3mSQ2` | 353376000 | 344396256 | SIMD-0220: snapshots use lattice-based accounts hash |
| `sol_log_data_syscall_enabled` | `6uaHcKPGUy4J7emLBgUTeufhJdiwhngW6a1R9B7c2ob9` | 117936008 | 112412257 | enable sol_log_data syscall |
| `spl_associated_token_account_v1_0_4` | `FaTa4SpiaSNH44PGC4z8bnGVTkSRYaWvrBs3KTu8XQQq` | 130464000 | 122348260 | SPL Associated Token Account Program release version 1.0.4, tied to token 3.3.0 #22648 |
| `spl_associated_token_account_v1_1_0` | `FaTa17gVKoqbh38HcfiQonPsAaQViyDCCSg71AubYZw8` | 144288004 | 143084256 | SPL Associated Token Account Program version 1.1.0 release #24741 |
| `spl_token_v2_multisig_fix` | `E5JiFDQCwyC6QfT9REFyMpfK2mHcmv1GUDySU1Ue7TYv` | 41040000 | 39836256 | spl-token multisig fix |
| `spl_token_v2_self_transfer_fix` | `BL99GYhdjjcv6ys22C9wPgn2aTVERDbPHHo4NbS3hgp7` | 66528004 | 64028256 | spl-token self-transfer fix |
| `spl_token_v2_set_authority_fix` | `FToKNBYyiF4ky9s8WsmLBXHCht17Ek7RXaLZGHzzQhJ1` | 93312000 | 89084265 | spl-token set_authority fix |
| `spl_token_v3_3_0_release` | `Ftok2jhqAqxUWEiCVRrfRs9DPppWP8cgTB7NQNKL88mS` | 117072004 | 112844260 | spl-token v3.3.0 release |
| `spl_token_v3_4_0` | `Ftok4njE8b7tDffYkC5bAbCaQv5sL6jispYrprzatUwN` | 144288004 | 143084256 | SPL Token Program version 3.4.0 release #24740 |
| `stake_allow_zero_undelegated_amount` | `sTKz343FM8mqtyGvYWvbLpTThw3ixRM4Xk8QvZ985mw` | 200016004 | 195356264 | Allow zero-lamport undelegated amount for initialized stakes #24670 |
| `stake_deactivate_delinquent_instruction` | `437r62HoAdUb63amq3D7ENnBLDhHT2xY8eFkLJYVKK4x` | 198720004 | 195356264 | enable the deactivate delinquent stake instruction #23932 |
| `stake_merge_with_unmatched_credits_observed` | `meRgp4ArRPhD3KtCY9c5yAf2med7mBLsjKTPeVUHqBL` | 104112000 | 96860257 | allow merging active stakes with unmatched credits_observed #18985 |
| `stake_program_advance_activating_credits_observed` | `SAdVFw3RZvzbo6DvySbSdBnHN4gkzSTH9dSxesyKKPj` | 104112000 | 96860257 | Enable advancing credits observed for activation epoch #19309 |
| `stake_raise_minimum_delegation_to_1_sol` | `9onWzzvCzNC2jfhxxeqRgs5q7nFAAKpCUvkj6T6GJK9i` | not activated | not activated | Raise minimum stake delegation to 1.0 SOL #24357 |
| `stake_split_uses_rent_sysvar` | `FQnc7U4koHqWgRvFaBJjZnV8VPg6L6wWK33yJeDp4yvV` | 203904008 | 195356264 | stake split instruction uses rent sysvar |
| `stakes_remove_delegation_if_inactive` | `HFpdDDNQjvcXnXKec697HDDsyk6tFoWS2o8fkxuhQZpL` | 110592000 | 96860257 | remove delegations from stakes cache when inactive |
| `static_instruction_limit` | `64ixypL1HPu8WtJhNSMb9mSgfFaJvsANuRkTbHyuLfnx` | 404352000 | 385868256 | SIMD-0160: static instruction limit |
| `stop_sibling_instruction_search_at_parent` | `EYVpEP7uzH1CoXzbD6PubGhYmnxRXPeq3PPsm1ba3gpo` | 236304016 | 217388260 | stop the search in get_processed_sibling_instruction when the parent instruction is reached #27289 |
| `stop_truncating_strings_in_syscalls` | `16FMCmgLzCNNz6eTwGanbyN2ZxvTBSLuQ6DZhgeMshg` | 240192004 | 245900264 | Stop truncating strings in syscalls #31029 |
| `switch_to_new_elf_parser` | `Cdkc8PPTeTNUPoZEfCY5AyetUrEdkZtNPMgz58nqyaHD` | 273888000 | 247628260 | switch to new ELF parser #30497 |
| `syscall_saturated_math` | `HyrbKftCdJ5CrUfEti6x26Cj7rZLNe32weugk7tLcWb8` | 150768000 | 140060257 | syscalls use saturated math |
| `system_transfer_zero_check` | `BrTR9hzw4WBGFP65AJMbpAo64DcA3U6jdPSga9fMV5cS` | 93312000 | 84332260 | perform all checks for transfers of 0 lamports |
| `tx_wide_compute_cap` | `5ekBxc8itEnPv4NzGJtr8BVVQLNMQuLMNQQj7pHoLNZ9` | 135216004 | 124508260 | transaction wide compute cap |
| `update_hashes_per_tick` | `3uFHb9oKdGfgZGJK9EHaAXN4USvnQtAFC13Fh5gGFS5B` | 232848000 | 217388260 | Update desired hashes per tick on epoch boundary |
| `update_hashes_per_tick2` | `EWme9uFqfy1ikK1jhJs8fM5hxWnK336QJpbscNtizkTU` | 253584001 | 248492264 | Update desired hashes per tick to 2.8M |
| `update_hashes_per_tick3` | `8C8MCtsab5SsfammbzvYz65HHauuUYdbY2DZ4sznH6h5` | 255312004 | 248924256 | Update desired hashes per tick to 4.4M |
| `update_hashes_per_tick4` | `8We4E7DPwF2WfAN8tRTtWQNhi98B99Qpuj7JoZ3Aikgg` | 255744008 | 249788256 | Update desired hashes per tick to 7.6M |
| `update_hashes_per_tick5` | `BsKLKAn1WM4HVhPRDsjosmqSg2J8Tq5xP2s2daDS6Ni4` | 257040000 | 250220256 | Update desired hashes per tick to 9.2M |
| `update_hashes_per_tick6` | `FKu1qYwLQSiehz644H6Si65U5ZQ2cp9GxsyFUfYcuADv` | 257904000 | 251084256 | Update desired hashes per tick to 10M |
| `update_rewards_from_cached_accounts` | `28s7i3htzhahXQKqmS2ExzbEoUypg9krwvtK2M9UWXh9` | 206064004 | 197516256 | update rewards from cached accounts |
| `update_syscall_base_costs` | `2h63t332mGCCsWK2nqqqHhN4U9ayyqhLVFvczznHDoTZ` | 138672000 | 124508260 | update syscall base costs |
| `upgrade_bpf_stake_program_to_v5_1` | `s51VGwCAgebo2745DSUris72RavoLkXGUmVJosESCXr` | 443232000 | 431228256 | SIMD-0391: Upgrade BPF Stake Program to v5.1.0 (fixed-point warmup/cooldown) |
| `use_default_units_in_fee_calculation` | `8sKQrMQoUHtQSUP83SPG4ta2JDjSAiWs7t5aJ9uEd6To` | 206496008 | 195356264 | use default units per instruction in fee calculation #26785 |
| `validate_chained_block_id` | `vcmrbYbiMVKaq1snKP6eCacNDcr6qZvpCNUjmk6gxvZ` | 428544000 | 406604256 | SIMD-0340: Validate chained block ID |
| `validate_chained_block_id_2` | `vcmrw431aNM8ngQ46derkZXipoTGQdbHkEygBDh12dA` | 428544000 | 416540256 | SIMD-340: Encompassing check for validate chained block ID |
| `validate_fee_collector_account` | `prpFrMtgNmzaNzkPJg9o753fVvbHKqNrNTm76foJ2wm` | 258336004 | 251948256 | validate fee collector account #33888 |
| `validator_admission_ticket` | `VAT9huvhPjRN9cyrPytq9rwvEJ3J4ADtjdncgZRyANJ` | 434592000 | 417404256 | SIMD-0357: Alpenglow VAT implementation |
| `verify_tx_signatures_len` | `EVW9B5xD9FFK7vw1SBARwMA4s5eRo5eKJdKpsBikzKBz` | 102816004 | 94700260 | prohibit extra transaction signatures |
| `versioned_tx_message_enabled` | `3KZZ6Ks1885aGBQ45fwRcPXVBCtzUvxhUTkwKMR41Tca` | 154656004 | 127100256 | enable versioned transaction message processing |
| `vote_only_full_fec_sets` | `ffecLRhhakKSGhMuc6Fz2Lnfq4uT9q3iu9ZsNaPLxPc` | 332208000 | 324092256 | vote only full fec sets |
| `vote_only_retransmitter_signed_fec_sets` | `RfEcA95xnhuwooVAhUUksEJLZBF7xKCLuqrJoqk4Zph` | not activated | not activated | vote only on retransmitter signed fec sets |
| `vote_stake_checked_instructions` | `BcWknVcgvonN8sL4HE4XFuEVgfcee5MwxWPAgP6ZV89X` | 92448000 | 89516256 | vote/state program checked instructions #18345 |
| `vote_state_add_vote_latency` | `7axKe5BTYBDD87ftzWbk5DfzWMGyRvqmWTduuo22Yaqy` | 252720000 | 247628260 | replace Lockout with LandedVote (including vote latency) in vote state #31264 |
| `vote_state_update_credit_per_dequeue` | `CveezY6FDLVBToHDcvJRmtMouqzsmj4UXYh5ths5G5Uv` | 212112000 | 217388260 | Calculate vote credits for VoteStateUpdate per vote dequeue to match credit awards for Vote instruction |
| `vote_state_update_root_fix` | `G74BkWBzmsByZ1kxHy44H3wjwp5hp7JbrGRuDpco22tY` | 202176000 | 195356264 | fix root in vote state updates #27361 |
| `vote_withdraw_authority_may_change_authorized_voter` | `AVZS3ZsN4gi6Rkx2QUibYuSJG3S6QHib7xCYhG6vGJxU` | 138672000 | 124076256 | vote account withdraw authority may change the authorized voter #22521 |
| `warp_timestamp_again` | `GvDsGDkH5gyzwpDhxNixx8vtx1kwYHH13RiNAPw27zXb` | 66528004 | not activated | warp timestamp again, adjust bounding to 25% fast 80% slow #15204 |
| `warp_timestamp_with_a_vengeance` | `3BX6SBeEBibHaVQXywdkcgyUk6evfYZkHdztXiDtEpFS` | 136512012 | 135308256 | warp timestamp again, adjust bounding to 150% slow #25666 |
