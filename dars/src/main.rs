#![recursion_limit="256"]
#![feature(async_closure)]
#![feature(test)]
extern crate test;

use std::env;
use std::sync::Arc;

#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

use env_logger::Env;
use warp::Filter;

mod data;
mod hdf5;
mod mlog;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::from_env(Env::default().default_filter_or("dars=info")).init();

    info!("𓃢   Welcome to DARS v{}", VERSION);

    let mut data = data::Datasets::default();
    data.datasets.insert(
        "coads_climatology.nc4".to_string(),
        Arc::new(data::DatasetType::HDF5(hdf5::Hdf5Dataset::open(
            "../data/coads_climatology.nc4",
        )?)),
    );
    data.datasets.insert(
        "dmrpp/chunked_string_array.h5".to_string(),
        Arc::new(data::DatasetType::HDF5(hdf5::Hdf5Dataset::open(
            "../data/dmrpp/chunked_string_array.h5",
        )?)),
    );
    data.datasets.insert(
        "meps_det_vc_2_5km_latest.nc".to_string(),
        Arc::new(data::DatasetType::HDF5(hdf5::Hdf5Dataset::open(
            "../data/meps_det_vc_2_5km_latest.nc",
        )?)),
    );

    let data = Arc::new(data);
    let dars = data::filters::datasets(data).with(warp::log::custom(mlog::mlog));

    warp::serve(dars).run(([0, 0, 0, 0], 8001)).await;

    Ok(())
}
