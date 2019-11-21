#[macro_use] extern crate log;

mod catalog;
pub mod datasets;
mod nc;

use datasets::{Data, Dataset};

fn main() -> Result<(), std::io::Error> {
    std::env::set_var("RUST_LOG", "dredds=debug");
    env_logger::init();

    info!("Hello, world!");

    let mut data = Data::init();

    data.datasets.push(
        Box::new(
            nc::NcDataset::open("data/coads_climatology.nc".to_string())?));

    let mut dr = tide::App::with_state(data);
    dr.middleware(tide::middleware::RootLogger::new());

    dr.at("/catalog.xml").get(catalog::catalog);
    dr.at("/data/:dataset").get(datasets::Data::dataset);
    // dr.at("/data").nest(
    //     |r| data.datasets.iter().for_each(
    //         |d| d.at(r) ));

    /* - cache datasets
     * - das
     * - dds
     * - dods (netcdf)
     * - full file
     * - ascii (optional)
     */

    dr.serve("127.0.0.1:8001")
}

