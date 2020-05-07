use std::thread;
use std::env;

use futures::prelude::*;
#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;
use colored::Colorize;


const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() -> anyhow::Result<()> {
    use env_logger::Env;
    env_logger::from_env(Env::default().default_filter_or("dars=info")).init();

    info!("𓆣 𓃢  (DARS DAP v{})", VERSION);

    let mut dap = tide::new();
    dap.at("/").get(|_| async move { Ok("DAP!") });

    // Create execution pool for the `smol` runtime
    for _ in 0..num_cpus::get().max(1) {
        thread::spawn(|| smol::run(future::pending::<()>()));
    }

    // Listen
    info!("Listening on {}", "127.0.0.1:8001".yellow());
    Ok(smol::block_on(async { dap.listen("127.0.0.1:8001").await })?)
}
