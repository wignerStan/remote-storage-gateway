//! Process entry: explicit configuration, assembly and native Pingora lifecycle.
#[cfg(feature = "l7")]
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    match start(std::env::args_os().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("network-data-plane-worker: {error}");
            ExitCode::FAILURE
        }
    }
}
#[cfg(feature = "l7")]
fn start_l7(args: Vec<std::ffi::OsString>) -> Result<(), String> {
    if args.len() == 1 && matches!(args[0].to_str(), Some("--help" | "-h")) {
        println!(
            "network-data-plane-worker --config <file.json> [--check-config] [--upgrade]\n\nRuns the native proxy, configured callbacks and optional loopback management entry.\n--check-config validates configuration without opening listeners or loading plugins.\n--upgrade receives native listening sockets from the previous generation; requires upgrade.socket."
        );
        return Ok(());
    }
    let mut path = None;
    let mut check = false;
    let mut upgrade = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--config") if path.is_none() => {
                path = Some(PathBuf::from(
                    args.next().ok_or("--config requires a path")?,
                ));
            }
            Some("--check-config") if !check => check = true,
            Some("--upgrade") if !upgrade => upgrade = true,
            _ => {
                return Err(
                    "expected --config <file.json> [--check-config] [--upgrade]; use --help".into(),
                );
            }
        }
    }
    let path = path.ok_or("--config is required; use --help")?;
    let config = network_data_plane_worker::config::Config::load(&path)?;
    let options = config.process_options(upgrade)?;
    if check {
        println!("configuration valid");
        return Ok(());
    }
    network_data_plane_worker::assembly::run_with_options(config, options)
}

fn start(mut args: Vec<std::ffi::OsString>) -> Result<(), String> {
    if args.len() == 1 && args[0] == "--print-capabilities" {
        let mut lanes = Vec::new();
        if cfg!(feature = "l4") {
            lanes.push("l4");
        }
        if cfg!(feature = "l7") {
            lanes.push("l7");
        }
        println!(
            "{}",
            serde_json::json!({"schema":"network-data-plane-worker/capabilities/v1","implementation":"rust","lanes":lanes,"plugin_runtimes":["native"],"plugin_descriptors":[]})
        );
        return Ok(());
    }
    let mut lane = if cfg!(feature = "l7") {
        "l7".to_string()
    } else {
        "l4".to_string()
    };
    if let Some(index) = args.iter().position(|v| v == "--lane") {
        if index + 1 >= args.len() {
            return Err("--lane requires l4 or l7".into());
        }
        lane = args
            .remove(index + 1)
            .to_str()
            .ok_or("invalid lane")?
            .to_string();
        args.remove(index);
    }
    match lane.as_str() {
        #[cfg(feature = "l4")]
        "l4" => network_data_plane_worker::l4::start(args),
        #[cfg(feature = "l7")]
        "l7" => start_l7(args),
        _ => Err("requested lane is absent; select l4 or l7 in the application build".into()),
    }
}
