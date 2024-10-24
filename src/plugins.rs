use {
    crate::{
        config::Config,
        grpc::{GrpcService, Message},
        metrics::{self, PrometheusService, MESSAGE_QUEUE_SIZE},
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult,
        SlotStatus,
    },
    std::{
        concat, env,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::{
        runtime::{Builder, Runtime},
        sync::{mpsc, Notify},
    },
};

#[derive(Debug)]
pub struct PluginInner {
    runtime: Runtime,
    snapshot_channel: Mutex<Option<crossbeam_channel::Sender<Box<Message>>>>,
    snapshot_channel_closed: AtomicBool,
    grpc_channel: mpsc::UnboundedSender<Arc<Message>>,
    grpc_shutdown: Arc<Notify>,
    prometheus: PrometheusService,
}

impl PluginInner {
    fn send_message(&self, message: Message) {
        if self.grpc_channel.send(Arc::new(message)).is_ok() {
            MESSAGE_QUEUE_SIZE.inc();
        }
    }
}

#[derive(Debug, Default)]
pub struct Plugin {
    inner: Option<PluginInner>,
}

impl Plugin {
    fn with_inner<F>(&self, f: F) -> PluginResult<()>
    where
        F: FnOnce(&PluginInner) -> PluginResult<()>,
    {
        let inner = self.inner.as_ref().expect("initialized");
        f(inner)
    }
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, config_file: &str, is_reload: bool) -> PluginResult<()> {
        self.inner = Some(PluginInner {
            runtime,
            snapshot_channel: Mutex::new(snapshot_channel),
            snapshot_channel_closed: AtomicBool::new(false),
            grpc_channel,
            grpc_shutdown,
            prometheus,
        });

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.grpc_shutdown.notify_one();
            drop(inner.grpc_channel);
            inner.prometheus.shutdown();
            inner.runtime.shutdown_timeout(Duration::from_secs(30));
        }
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> PluginResult<()> {
        Ok(())
    }

    fn notify_entry(&self, entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        Ok(())
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions<'_>) -> PluginResult<()> {
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = Plugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}