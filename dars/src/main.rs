use futures::prelude::*;
use std::env;
use std::thread;

#[macro_use]
extern crate log;
// #[macro_use]
// extern crate anyhow;

use colored::Colorize;
use env_logger::Env;

mod dataset;
mod hdf5;

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct Server {
    data: dataset::Datasets,
}

pub type Request = tide::Request<Server>;

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    env_logger::from_env(Env::default().default_filter_or("dars=debug")).init();

    info!("DARS 𓆣 𓃢  v{}", VERSION);

    let server = Server {
        data: dataset::Datasets::default(),
    };

    let mut dars = tide::with_state(server);
    dars.at("/").get(|_| async move { Ok("DAP!") });
    dars.at("/data")
        .get(|req: Request| async move { req.state().data.datasets().await });
    dars.at("/data/:dataset")
        .get(|req: Request| async move { req.state().data.dataset(&req).await });

    info!("Listening on {}", "127.0.0.1:8001".yellow());
    dars.listen("127.0.0.1:8001").await?;

    Ok(())
}
