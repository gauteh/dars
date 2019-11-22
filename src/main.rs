#[macro_use] extern crate log;
#[macro_use] extern crate anyhow;

use hyper::{
    Server, Body, Response, Error,
    service::{service_fn, make_service_fn}
};

// mod catalog;
// pub mod datasets;
// mod nc;

// use datasets::{Data, Dataset};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    std::env::set_var("RUST_LOG", "dredds=debug");
    env_logger::init();

    info!("Hello, world!");

    // let mut data = Data::init();

    // data.datasets.push(
    //     Box::new(
    //         nc::NcDataset::open("data/coads_climatology.nc".to_string()).unwrap()));

    let addr = ([127, 0, 0, 1], 8001).into();

    let msvc = make_service_fn(|_| async {
        Ok::<_, Error>(
            service_fn(|req| async {
                Ok::<_, Error>(Response::new(Body::from("Hello World")))
            }))
    });


    let server = Server::bind(&addr)
        .serve(msvc);

    info!("Listening on http://{}", addr);
    server.await.map_err(|e| anyhow!("SDf"))

    // let mut dr = tide::App::with_state(data);
    // dr.middleware(tide::middleware::RootLogger::new());

    // dr.at("/catalog.xml").get(catalog::catalog);
    // dr.at("/data/:dataset").get(datasets::Data::dataset);
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

    // dr.serve("127.0.0.1:8001").or_else(|_e| Err(anyhow!("Failed to run server")))
}

