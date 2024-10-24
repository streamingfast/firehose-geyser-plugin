use agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use {
    crate::{
        state::State,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult,
    },
    std::{
        concat, env, sync::RwLock
    },
};

#[derive(Debug, Default)]
pub struct Plugin {
    state: RwLock<State>,
}

impl Plugin {
}
impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, config_file: &str, is_reload: bool) -> PluginResult<()> {
        println!("ON_LOADING WITH SOL ACCOUNTS PLUGIN");
        self.state = RwLock::new(State::new());
        Ok(())
    }

    fn on_unload(&mut self) {
    }

    fn update_account(&self,account: ReplicaAccountInfoVersions, slot: u64, is_startup: bool) -> PluginResult<()> {
        panic!("SOL ACCOUNTS PANICING");
        
        match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => {
                let account_key = account.pubkey.to_vec();
                let account_data = account.data.to_vec();

                self.state.write().unwrap().set_account_data(slot, account_key, account_data);
            },

            ReplicaAccountInfoVersions::V0_0_2(account) => {
                let account_key = account.pubkey.to_vec();
                let account_data = account.data.to_vec();

                self.state.write().unwrap().set_account_data(slot, account_key, account_data);
            },

            ReplicaAccountInfoVersions::V0_0_3(account) => {
                let account_key = account.pubkey.to_vec();
                let account_data = account.data.to_vec();

                self.state.write().unwrap().set_account_data(slot, account_key, account_data);
            },

            _ => {
                panic!("Unsupported account version");
            },

        }

        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        Ok(())
    }

    fn update_slot_status(&self,slot: u64, parent: Option<u64>, status: SlotStatus) -> PluginResult<()> {
        if status == SlotStatus::Confirmed {
            self.state.write().unwrap().set_last_confirmed_block(slot);
        }

        self.state.write().unwrap().stats();
        self.state.write().unwrap().purge_confirmed_blocks(slot);
        
        Ok(())
    }

    fn notify_transaction(&self,transaction: ReplicaTransactionInfoVersions<'_>, slot: u64) -> PluginResult<()> {
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