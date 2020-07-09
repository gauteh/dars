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

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use colored::Colorize;
use env_logger::Env;
use getopts::Options;
use warp::Filter;

mod data;
mod hdf5;
mod ncml;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::from_env(Env::default().default_filter_or("dars=info")).init();

    info!("𓃢   Welcome to DARS v{}", VERSION);

    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt(
        "a",
        "address",
        "listening socket address (default: 127.0.0.1:8001)",
        "ADDR",
    );
    opts.optopt(
        "",
        "root-url",
        "root URL of service (default: empty)",
        "ROOT",
    );
    opts.optflag("h", "help", "print this help menu");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };

    if matches.opt_present("h") {
        let brief = format!("Usage: {} [options] [data..]", program);
        print!("{}", opts.usage(&brief));
        println!(
            r#"
The directories specified with DATA is searched for supported datasets.
If DATA is specified with a trailing "/" (e.g. "data/"), the folder
name is not included at the end-point for the dataset. All datasets are
available under the /data root. A list of datasets may be queried at /data.

If no DATA is specified, "data/" is used."#
        );
        return Ok::<_, anyhow::Error>(());
    }

    let datadir: String = if !matches.free.is_empty() {
        matches.free[0].clone()
    } else {
        "data/".to_string()
    };

    let addr: SocketAddr = matches.opt_get_default("a", "127.0.0.1:8001".parse()?)?;
    let root: Option<String> = matches.opt_str("root-url");

    let data = Arc::new(data::Datasets::new_with_datadir(root.clone(), datadir).await);
    let dars = data::filters::datasets(data).with(warp::log::custom(data::request_log));

    info!(
        "Listening on {} {}",
        format!("http://{}", addr).yellow(),
        root.map(|r| format!("({})", r.blue()))
            .unwrap_or_else(|| "".to_string())
    );

    warp::serve(dars).run(addr).await;

    Ok(())
}
