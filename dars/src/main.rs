use std::env;

#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

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

    info!("𓃢   Welcome to DARS v{}", VERSION);

    let mut data = dataset::Datasets::default();
    data.datasets.insert("coads_climatology.nc4".to_string(),
        dataset::DatasetType::HDF5(
            hdf5::Hdf5Dataset::open(
                "../data/coads_climatology.nc4")?));

    let server = Server { data };

    let mut dars = tide::with_state(server);
    dars.at("/").get(|_| async move { Ok("𓃢 ") });
    dars.at("/data")
        .get(|req: Request| async move { req.state().data.datasets().await });
    dars.at("/data/:dataset")
        .get(|req: Request| async move { req.state().data.dataset(&req).await });

    info!("Listening on {}", "127.0.0.1:8001".yellow());
    dars.listen("127.0.0.1:8001").await?;

    Ok(())
}
