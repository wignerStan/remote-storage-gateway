//! Process entry: explicit configuration, assembly and native Pingora lifecycle.
use std::{path::PathBuf, process::ExitCode};

fn main() -> ExitCode {
    match start(std::env::args_os().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("pingora-worker: {error}");
            ExitCode::FAILURE
        }
    }
}
fn start(args: Vec<std::ffi::OsString>) -> Result<(), String> {
    if args.len() == 1 && matches!(args[0].to_str(), Some("--help" | "-h")) {
        println!(
            "pingora-worker --config <file.json> [--check-config] [--upgrade]\n\nRuns the native proxy, configured callbacks and optional loopback management entry.\n--check-config validates configuration without opening listeners or loading plugins.\n--upgrade receives native listening sockets from the previous generation; requires upgrade.socket."
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
    let config = pingora_worker::config::Config::load(&path)?;
    let options = config.process_options(upgrade)?;
    if check {
        println!("configuration valid");
        return Ok(());
    }
    pingora_worker::assembly::run_with_options(config, options)
}
