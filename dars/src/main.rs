#![recursion_limit = "512"]
#![feature(async_closure)]
#![feature(test)]
extern crate test;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[macro_use]
extern crate log;

use std::sync::Arc;
use colored::Colorize;
use env_logger::Env;
use warp::Filter;

const VERSION: &str = env!("CARGO_PKG_VERSION");

use dars::{data, config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::from_env(Env::default().default_filter_or("dars=info")).init();
    info!("𓃢   DARS v{}", VERSION);

    #[cfg(debug_assertions)]
    info!("Debug build");

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
