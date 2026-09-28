mod block_printer;
mod config;
mod heap;
mod pb;
pub mod plugins;
mod state;
mod stats;
mod type_checks;
mod utils;

pub use plugins::_create_plugin;

// mimalloc manages the plugin's heap, account cache included
#[global_allocator]
static GLOBAL: heap::CountingMiMalloc = heap::CountingMiMalloc;
