#[macro_use] extern crate log;

mod catalog;

fn main() -> Result<(), std::io::Error> {
    std::env::set_var("RUST_LOG", "dredds=debug");
    env_logger::init();

    info!("Hello, world!");

    let mut dr = tide::App::new();
    dr.middleware(tide::middleware::RootLogger::new());

    dr.at("/catalog.xml").get(catalog::catalog);
    dr.serve("127.0.0.1:8001")
}

