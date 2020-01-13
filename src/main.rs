#![recursion_limit = "1024"]
#![feature(async_closure)]
#![feature(test)]
extern crate test;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

use colored::Colorize;
use futures::FutureExt;
use getopts::Options;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Error, Method, Response, Server, StatusCode,
};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

mod dap2;
pub mod datasets;
mod nc;
mod ncml;
mod testcommon;

use datasets::{Data, Dataset};

lazy_static! {
    pub static ref DATA: Arc<RwLock<Data>> = Arc::new(RwLock::new(Data::new()));
}

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    use env_logger::Env;
    env_logger::from_env(Env::default().default_filter_or("dars=info")).init();

    info!("𓆣 𓃢  (DARS DAP v{})", VERSION);

    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt(
        "a",
        "address",
        "listening socket address (default: 127.0.0.1:8001)",
        "ADDR",
    );
    opts.optflag("w", "watch", "watch for changes in data dir");
    opts.optflag("h", "help", "print this help menu");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };

    if matches.opt_present("h") {
        let brief = format!("Usage: {} [options] DATA", program);
        print!("{}", opts.usage(&brief));
        println!(
            "\nThe directory specified with DATA is searched for supported datasets.\n\
                    If DATA is specified with a trailing \"/\" (e.g. \"data/\"), the folder\n\
                    name is not included at the end-point for the dataset. All datasets are\n\
                    available under the /data root. A list of datasets may be queried at /data.\n\
                    \n\
                    If no DATA is specified, \"data/\" is used."
        );
        return Ok::<_, anyhow::Error>(());
    }

    let watch = matches.opt_present("w");

    let datadir: String = if !matches.free.is_empty() {
        matches.free[0].clone()
    } else {
        "data/".to_string()
    };

    let addr: SocketAddr = matches.opt_get_default("a", "127.0.0.1:8001".parse()?)?;

    {
        let rdata = DATA.clone();
        let mut data = rdata.write().await;
        data.init_root(datadir.clone(), watch);
    }

    let msvc = make_service_fn(|_| async move {
        Ok::<_, Error>(service_fn(|req| async move {
            let m = req.method().clone();
            let u = req.uri().clone();

            let r = match (req.method(), req.uri().path()) {
                (&Method::GET, "/") => {
                    Response::builder().body(Body::from("DAP!\n\n(checkout /data)"))
                }
                (&Method::GET, "/data") | (&Method::GET, "/data/") => {
                    DATA.read().await.datasets(req).await
                }
                (&Method::GET, p) if p.starts_with("/data/") => {
                    DATA.read().await.dataset(req).await
                }
                _ => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty()),
            };

            let s = match &r {
                Ok(ir) => match ir.status().is_success() {
                    true => ir.status().to_string().yellow(),
                    false => ir.status().to_string().red(),
                },
                Err(e) => e.to_string().red(),
            };

            debug!("{} {} -> {}", m.to_string().blue(), u, s);

            r
        }))
    });

    let server = Server::bind(&addr)
        .serve(msvc)
        .map(|r| r.map_err(|e| anyhow!(e)));

    info!("Listening on {}", format!("http://{}", addr).yellow());

    server.await
}
