#![recursion_limit = "512"]
#![feature(async_closure)]
#![feature(test)]
extern crate test;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[macro_use]
extern crate log;

use colored::Colorize;
use env_logger::Env;
use std::sync::Arc;
use warp::Filter;

const VERSION: &str = env!("CARGO_PKG_VERSION");

use dars::{config, data};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("dars=info")).init();
    info!("𓃢   DARS v{}", VERSION);

    #[cfg(debug_assertions)]
    info!("Debug build");

    let config = config::load_config_with_args()?;

    info!(
        "Opening sled db: {}..",
        config.db.path.to_string_lossy().yellow()
    );
    let db = sled::open(config.db.path)?;

    let data =
        Arc::new(data::Datasets::new_with_datadir(config.root_url.clone(), config.data, db).await?);
    let dars = data::filters::datasets(data.clone()).with(warp::log::custom(data::request_log));

    #[cfg(feature = "catalog")]
    let dars =
        dars_catalog::catalog(config.root_url.clone().unwrap_or_else(|| "".into()), data)?.or(dars);

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
