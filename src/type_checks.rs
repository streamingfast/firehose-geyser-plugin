/// Compile-time type checks to ensure we're aware of all fields in external structs.
///
/// These functions are never called at runtime, but the compiler will fail to build
/// if any fields are added, removed, or renamed in the external types we use.
///
/// This helps us catch breaking changes in dependencies during upgrades.
///
/// # How it works
///
/// Each check function uses exhaustive pattern matching on struct fields or enum variants.
/// If a field is added, removed, or renamed in the external crate, Rust's compiler will
/// produce an error because the pattern is no longer exhaustive.
///
/// Example error when a field is missing:
/// ```text
/// error[E0027]: pattern does not mention field `new_field`
///   --> src/type_checks.rs:XX:XX
///    |
/// XX |     let TransactionStatusMeta { status: _, fee: _, ... } = meta;
///    |         ^^^^^^^^^^^^^^^^^^^^^ missing field `new_field`
/// ```
///
/// # Testing
///
/// To verify the checks work, temporarily comment out a field in one of the patterns
/// and run `cargo check` - it should fail to compile.

#[allow(dead_code, invalid_value)]
fn check_transaction_status_meta() {
    let meta: solana_transaction_status::TransactionStatusMeta = unsafe { std::mem::zeroed() };

    // Exhaustively match all fields - compiler will fail if fields are added/removed
    let solana_transaction_status::TransactionStatusMeta {
        status: _,
        fee: _,
        pre_balances: _,
        post_balances: _,
        inner_instructions: _,
        log_messages: _,
        pre_token_balances: _,
        post_token_balances: _,
        rewards: _,
        loaded_addresses: _,
        return_data: _,
        compute_units_consumed: _,
        cost_units: _,
    } = meta;
}

#[allow(dead_code, invalid_value)]
fn check_transaction_token_balance() {
    let balance: solana_transaction_status::TransactionTokenBalance = unsafe { std::mem::zeroed() };

    let solana_transaction_status::TransactionTokenBalance {
        account_index: _,
        mint: _,
        ui_token_amount: _,
        owner: _,
        program_id: _,
    } = balance;
}

#[allow(dead_code, invalid_value)]
fn check_ui_token_amount() {
    let amount: solana_rpc_client_api::response::UiTokenAmount = unsafe { std::mem::zeroed() };

    let solana_rpc_client_api::response::UiTokenAmount {
        ui_amount: _,
        decimals: _,
        amount: _,
        ui_amount_string: _,
    } = amount;
}

#[allow(dead_code, invalid_value)]
fn check_reward() {
    let reward: solana_transaction_status::Reward = unsafe { std::mem::zeroed() };

    let solana_transaction_status::Reward {
        pubkey: _,
        lamports: _,
        post_balance: _,
        reward_type: _,
        commission: _,
    } = reward;
}

#[allow(dead_code)]
fn check_reward_type() {
    let reward_type: solana_transaction_status::RewardType = unsafe { std::mem::zeroed() };

    // Match all variants - compiler will fail if variants are added/removed
    match reward_type {
        solana_transaction_status::RewardType::Fee => {}
        solana_transaction_status::RewardType::Rent => {}
        solana_transaction_status::RewardType::Voting => {}
        solana_transaction_status::RewardType::Staking => {}
    }
}

#[allow(dead_code, invalid_value)]
fn check_inner_instructions() {
    let inner: solana_transaction_status::InnerInstructions = unsafe { std::mem::zeroed() };

    let solana_transaction_status::InnerInstructions {
        index: _,
        instructions: _,
    } = inner;
}

#[allow(dead_code, invalid_value)]
fn check_inner_instruction() {
    let inner: solana_transaction_status::InnerInstruction = unsafe { std::mem::zeroed() };

    let solana_transaction_status::InnerInstruction {
        instruction: _,
        stack_height: _,
    } = inner;
}

#[allow(dead_code, invalid_value)]
fn check_transaction_return_data() {
    let return_data: solana_transaction_context::TransactionReturnData =
        unsafe { std::mem::zeroed() };

    let solana_transaction_context::TransactionReturnData {
        program_id: _,
        data: _,
    } = return_data;
}

#[allow(dead_code, invalid_value)]
fn check_loaded_addresses() {
    let loaded: solana_sdk::message::v0::LoadedAddresses = unsafe { std::mem::zeroed() };

    let solana_sdk::message::v0::LoadedAddresses {
        writable: _,
        readonly: _,
    } = loaded;
}

#[allow(dead_code)]
fn check_message_header() {
    let header: solana_sdk::message::MessageHeader = unsafe { std::mem::zeroed() };

    let solana_sdk::message::MessageHeader {
        num_required_signatures: _,
        num_readonly_signed_accounts: _,
        num_readonly_unsigned_accounts: _,
    } = header;
}

#[allow(dead_code, invalid_value)]
fn check_compiled_instruction() {
    let instruction: solana_message::compiled_instruction::CompiledInstruction =
        unsafe { std::mem::zeroed() };

    let solana_message::compiled_instruction::CompiledInstruction {
        program_id_index: _,
        accounts: _,
        data: _,
    } = instruction;
}

#[allow(dead_code, invalid_value)]
fn check_message_address_table_lookup() {
    let lookup: solana_sdk::message::v0::MessageAddressTableLookup = unsafe { std::mem::zeroed() };

    let solana_sdk::message::v0::MessageAddressTableLookup {
        account_key: _,
        writable_indexes: _,
        readonly_indexes: _,
    } = lookup;
}

#[allow(dead_code)]
fn check_versioned_message() {
    let msg: solana_message::VersionedMessage = unsafe { std::mem::zeroed() };

    // Match all variants - compiler will fail if variants are added/removed
    match msg {
        solana_message::VersionedMessage::Legacy(_) => {}
        solana_message::VersionedMessage::V0(_) => {}
    }
}

#[allow(dead_code, invalid_value)]
fn check_versioned_transaction() {
    let tx: solana_transaction::versioned::VersionedTransaction = unsafe { std::mem::zeroed() };

    let solana_transaction::versioned::VersionedTransaction {
        signatures: _,
        message: _,
    } = tx;
}

// Geyser plugin interface types
#[allow(dead_code)]
fn check_replica_transaction_info_versions() {
    let versions: agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoVersions = unsafe { std::mem::zeroed() };

    match versions {
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoVersions::V0_0_1(_) => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoVersions::V0_0_2(_) => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoVersions::V0_0_3(_) => {}
    }
}

#[allow(dead_code)]
fn check_replica_account_info_versions() {
    let versions: agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions = unsafe { std::mem::zeroed() };

    match versions {
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions::V0_0_1(_) => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions::V0_0_2(_) => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions::V0_0_3(_) => {}
    }
}

#[allow(dead_code)]
fn check_replica_block_info_versions() {
    let versions: agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions =
        unsafe { std::mem::zeroed() };

    match versions {
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions::V0_0_1(_) => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions::V0_0_2(_) => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions::V0_0_3(_) => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions::V0_0_4(_) => {}
    }
}

#[allow(dead_code)]
fn check_slot_status() {
    let status: agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus =
        unsafe { std::mem::zeroed() };

    match status {
        agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus::Processed => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus::Rooted => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus::Confirmed => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus::Completed => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus::FirstShredReceived => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus::Dead(_) => {}
        agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus::CreatedBank => {}
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn type_checks_compile() {
        // This test just needs to compile - it doesn't need to run
        // The checks above are all dead code but will cause compilation errors
        // if the external types change
    }
}
