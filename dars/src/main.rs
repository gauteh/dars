#![recursion_limit = "512"]
#![feature(async_closure)]
#![feature(test)]
extern crate test;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

use std::sync::Arc;

use colored::Colorize;
use env_logger::Env;
use warp::Filter;

mod config;
mod data;
mod hdf5;
mod ncml;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::from_env(Env::default().default_filter_or("dars=info")).init();

    info!("𓃢   DARS v{}", VERSION);

    let config = config::load_config_with_args()?;
    let data =
        Arc::new(data::Datasets::new_with_datadir(config.root_url.clone(), config.data).await?);
    let dars = data::filters::datasets(data).with(warp::log::custom(data::request_log));

    info!(
        "Listening on {} {}",
        format!("http://{}", config.address).yellow(),
        config
            .root_url
            .map(|r| format!("({})", r.blue()))
            .unwrap_or_else(|| "".to_string())
    );

    warp::serve(dars).run(config.address).await;

    Ok(())
}
