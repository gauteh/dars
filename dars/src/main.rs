use futures::prelude::*;
use std::env;
use std::thread;

#[macro_use]
extern crate log;
// #[macro_use]
// extern crate anyhow;

use colored::Colorize;

mod dataset;
mod hdf5;

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct Server {
    data: dataset::Datasets,
}

pub type Request = tide::Request<Server>;

// #[async_std::main]
fn main() -> anyhow::Result<()> {
    use env_logger::Env;
    env_logger::from_env(Env::default().default_filter_or("dars=debug")).init();

    info!("𓆣 𓃢  (DARS DAP v{})", VERSION);

    let server = Server {
        data: dataset::Datasets::default(),
    };

    let mut dap = tide::with_state(server);
    dap.at("/").get(|_| async move { Ok("DAP!") });
    dap.at("/data")
        .get(|req: Request| async move { req.state().data.datasets().await });
    dap.at("/data/:dataset")
        .get(|req: Request| async move { req.state().data.dataset(&req).await });

    // Create execution pool for the `smol` runtime
    for _ in 0..num_cpus::get().max(1) {
        thread::spawn(|| smol::run(future::pending::<()>()));
    }
    debug!("Spawned {} workers.", num_cpus::get().max(1));

    // Listen
    info!("Listening on {}", "127.0.0.1:8001".yellow());
    Ok(smol::block_on(async {
        dap.listen("127.0.0.1:8001").await
    })?)

    // Ok(dap.listen("127.0.0.1:8001").await?)
}
