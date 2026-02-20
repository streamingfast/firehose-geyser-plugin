use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions, ReplicaEntryInfoVersions,
    ReplicaTransactionInfoVersions, Result as PluginResult, SlotStatus,
};
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

mod config;
use config::Config;

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_millis()
}

/// Tracks first/last dedup state for a single event type.
/// The first event for a new slot is printed immediately; subsequent events
/// for the same slot are held in `pending_line`. The pending line is flushed
/// to the writer before the next unrelated write occurs.
#[derive(Default)]
struct DedupState {
    last_slot: Option<u64>,
    pending_line: Option<String>,
}

struct Inner {
    writer: BufWriter<std::fs::File>,
    accounts: DedupState,
    transactions: DedupState,
}

impl Inner {
    /// Flush any suppressed (last-seen) lines for both accounts and transactions.
    /// Called before writing any unrelated event so the log stays in order.
    fn flush_pending(&mut self) {
        if let Some(pending) = self.accounts.pending_line.take() {
            let _ = writeln!(self.writer, "{}", pending);
        }
        if let Some(pending) = self.transactions.pending_line.take() {
            let _ = writeln!(self.writer, "{}", pending);
        }
    }

    /// Write an arbitrary line, flushing all pending dedup lines first.
    fn write_line(&mut self, line: &str) {
        self.flush_pending();
        let _ = writeln!(self.writer, "{}", line);
        let _ = self.writer.flush();
    }

    /// Handle a deduplicated account event for `slot`.
    /// - First event for a new slot → flush all pending, print immediately.
    /// - Repeated events for the same slot → store as pending (overwrites previous).
    fn handle_account(&mut self, slot: u64, line: String) {
        if self.accounts.last_slot == Some(slot) {
            self.accounts.pending_line = Some(line);
        } else {
            self.flush_pending();
            let _ = writeln!(self.writer, "{}", line);
            let _ = self.writer.flush();
            self.accounts.last_slot = Some(slot);
        }
    }

    /// Handle a deduplicated transaction event for `slot`.
    /// - First event for a new slot → flush all pending, print immediately.
    /// - Repeated events for the same slot → store as pending (overwrites previous).
    fn handle_transaction(&mut self, slot: u64, line: String) {
        if self.transactions.last_slot == Some(slot) {
            self.transactions.pending_line = Some(line);
        } else {
            self.flush_pending();
            let _ = writeln!(self.writer, "{}", line);
            let _ = self.writer.flush();
            self.transactions.last_slot = Some(slot);
        }
    }
}

#[derive(Default)]
pub struct LoggerPlugin {
    inner: Option<Mutex<Inner>>,
    with_accounts: bool,
    with_transactions: bool,
}

impl std::fmt::Debug for LoggerPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoggerPlugin")
            .field("with_accounts", &self.with_accounts)
            .field("with_transactions", &self.with_transactions)
            .finish_non_exhaustive()
    }
}

impl LoggerPlugin {
    fn log(&self, event: &str, slot: u64) {
        if let Some(mutex) = &self.inner {
            let mut g = mutex.lock().expect("logger inner mutex poisoned");
            let line = format!("{} {} slot={}", now_ms(), event, slot);
            g.write_line(&line);
        }
    }

    fn log_no_slot(&self, event: &str) {
        if let Some(mutex) = &self.inner {
            let mut g = mutex.lock().expect("logger inner mutex poisoned");
            let line = format!("{} {}", now_ms(), event);
            g.write_line(&line);
        }
    }
}

impl GeyserPlugin for LoggerPlugin {
    fn name(&self) -> &'static str {
        "logger-geyser-plugin"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        let config = Config::load_from_file(config_file)?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.log_file)
            .map_err(|e| {
                agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError::ConfigFileReadError {
                    msg: format!("failed to open log file '{}': {}", config.log_file, e),
                }
            })?;

        self.with_accounts = config.with_accounts;
        self.with_transactions = config.with_transactions;
        self.inner = Some(Mutex::new(Inner {
            writer: BufWriter::new(file),
            accounts: DedupState::default(),
            transactions: DedupState::default(),
        }));
        self.log_no_slot("on_load");
        Ok(())
    }

    fn on_unload(&mut self) {
        self.log_no_slot("on_unload");
    }

    fn update_account(
        &self,
        _account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        if is_startup {
            return Ok(());
        }
        if let Some(mutex) = &self.inner {
            let mut g = mutex.lock().expect("logger inner mutex poisoned");
            let line = format!("{} update_account slot={}", now_ms(), slot);
            g.handle_account(slot, line);
        }
        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        self.log_no_slot("notify_end_of_startup");
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: u64,
        _parent: Option<u64>,
        status: &SlotStatus,
    ) -> PluginResult<()> {
        let event = match status {
            SlotStatus::Processed => "update_slot_status:processed",
            SlotStatus::Confirmed => "update_slot_status:confirmed",
            SlotStatus::Rooted => "update_slot_status:rooted",
            SlotStatus::FirstShredReceived => "update_slot_status:first_shred_received",
            SlotStatus::Completed => "update_slot_status:completed",
            SlotStatus::CreatedBank => "update_slot_status:created_bank",
            SlotStatus::Dead(_) => "update_slot_status:dead",
        };
        self.log(event, slot);
        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> PluginResult<()> {
        let event = match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(_) => "notify_transaction:v1",
            ReplicaTransactionInfoVersions::V0_0_2(_) => "notify_transaction:v2",
            ReplicaTransactionInfoVersions::V0_0_3(_) => "notify_transaction:v3",
        };

        if let Some(mutex) = &self.inner {
            let mut g = mutex.lock().expect("logger inner mutex poisoned");
            let line = format!("{} {} slot={}", now_ms(), event, slot);
            g.handle_transaction(slot, line);
        }
        Ok(())
    }

    fn notify_entry(&self, _entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        Ok(())
        //let slot = match entry {
        //    ReplicaEntryInfoVersions::V0_0_1(e) => e.slot,
        //    ReplicaEntryInfoVersions::V0_0_2(e) => e.slot,
        //};
        //self.log("notify_entry", slot);
    }

    fn notify_block_metadata(&self, block_info: ReplicaBlockInfoVersions<'_>) -> PluginResult<()> {
        let (slot, block_time) = match block_info {
            ReplicaBlockInfoVersions::V0_0_1(b) => (b.slot, b.block_time),
            ReplicaBlockInfoVersions::V0_0_2(b) => (b.slot, b.block_time),
            ReplicaBlockInfoVersions::V0_0_3(b) => (b.slot, b.block_time),
            ReplicaBlockInfoVersions::V0_0_4(b) => (b.slot, b.block_time),
        };

        if let Some(mutex) = &self.inner {
            let mut g = mutex.lock().expect("logger inner mutex poisoned");
            let block_time_str = block_time
                .map(|t| t.to_string())
                .unwrap_or_else(|| "none".to_string());
            let line = format!(
                "{} notify_block_metadata slot={} block_time={}",
                now_ms(),
                slot,
                block_time_str,
            );
            g.write_line(&line);
        }
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        self.with_accounts
    }

    fn transaction_notifications_enabled(&self) -> bool {
        self.with_transactions
    }

    fn entry_notifications_enabled(&self) -> bool {
        false
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = LoggerPlugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
