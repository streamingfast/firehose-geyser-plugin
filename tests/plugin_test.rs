use agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;
use firehose_geyser_plugin::plugins::Plugin;
use pretty_assertions::assert_eq;
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn test_plugin_as_geyser_plugin_basic() {
    let plugin = Plugin::new(false, false);
    let plugin: &dyn GeyserPlugin = &plugin;

    let name = plugin.name();
    assert!(name.contains("firehose-geyser-plugin"));

    assert_eq!(plugin.account_data_notifications_enabled(), true);
    assert_eq!(plugin.account_data_snapshot_notifications_enabled(), true);
    assert_eq!(plugin.transaction_notifications_enabled(), true);
    assert_eq!(plugin.entry_notifications_enabled(), false);
}

fn test_plugin_config(
    get_config: fn(cursor_path: &str) -> String,
    run_test: fn(config_path: &str, plugin: &mut Plugin) -> (),
) {
    let mut firehose_plugin = Plugin::new(false, false);

    // Create a temporary cursor file
    let cursor_file = NamedTempFile::new().expect("Failed to create cursor temp file");
    let cursor_path = cursor_file
        .path()
        .to_str()
        .expect("Failed to get cursor path");

    let mut config_file = NamedTempFile::new().expect("Failed to create config temp file");
    // let config_json = format!(config_format, cursor_path);
    let content = get_config(&cursor_path);

    config_file
        .write_all(content.as_bytes())
        .expect("Failed to write config");
    config_file.flush().expect("Failed to flush");

    let config_path = config_file
        .path()
        .to_str()
        .expect("Failed to get config path");

    run_test(config_path, &mut firehose_plugin);
}

#[test]
fn test_plugin_as_geyser_plugin_load_no_dev_config() {
    test_plugin_config(
        |cursor_path| {
            // Create a temporary config file with multiline JSON using raw string literal (r#"..."#)
            // Note: dev field is optional and will use default value if not present
            format!(
                r#"{{
                "log": {{
                    "level": "info"
                }},
                "local_rpc_client": {{
                    "endpoint": "http://localhost:8899"
                }},
                "remote_rpc_client": {{
                    "endpoint": "http://localhost:8899"
                }},
                "cursor_file": "{}",
                "block_destination_file": "",
                "account_block_destination_file": "",
                "send_processed": false,
                "noop": true
            }}"#,
                cursor_path
            )
        },
        |config_path, plugin| {
            plugin.on_load(config_path, false).expect("Should succeed");

            assert_eq!(false, plugin.is_send_processed());

            let state = plugin.state_copy();
            assert_eq!(None, state.first_block_to_process);
            assert_eq!(None, state.first_received_blockmeta);
        },
    );
}

#[test]
fn test_plugin_as_geyser_plugin_load_with_dev_config_force_send() {
    test_plugin_config(
        |cursor_path| {
            // Create a temporary config file with multiline JSON using raw string literal (r#"..."#)
            // Note: dev field is optional and will use default value if not present
            format!(
                r#"{{
                "log": {{
                    "level": "info"
                }},
                "local_rpc_client": {{
                    "endpoint": "http://localhost:8899"
                }},
                "remote_rpc_client": {{
                    "endpoint": "http://localhost:8899"
                }},
                "cursor_file": "{}",
                "block_destination_file": "",
                "account_block_destination_file": "",
                "send_processed": false,
                "noop": true,
                "dev": {{
                    "force_send": true
                }}
            }}"#,
                cursor_path
            )
        },
        |config_path, plugin| {
            plugin.on_load(config_path, false).expect("Should succeed");

            assert_eq!(false, plugin.is_send_processed());

            let state = plugin.state_copy();
            assert_eq!(Some(0), state.first_block_to_process);
            assert_eq!(Some(0), state.first_received_blockmeta);
        },
    );
}
