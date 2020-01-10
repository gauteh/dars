#![recursion_limit="1024"]
#![feature(async_closure)]

#![feature(test)]
extern crate test;

#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;
#[macro_use] extern crate anyhow;

use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use futures::FutureExt;
use std::time::Duration;
use hyper::{
    Server, Body, Response, Error, Method, StatusCode,
    service::{service_fn, make_service_fn}
};
use colored::Colorize;
use notify::Watcher;
use getopts::Options;

pub mod datasets;
mod dap2;
mod nc;
mod ncml;
mod testcommon;

use datasets::{Data, Dataset};

lazy_static! {
    pub static ref DATA: Arc<RwLock<Data>> = Arc::new(RwLock::new(Data::new()));
}

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

// This watches for new files and changes, deletions of loaded
// files. It would be safer to handle change detection / re-load
// on each access, but that would involve a syscall for every DAS
// and DDS request.
//
// On systems where the actual file is not removed untill all file handles
// are closed this should work fairly well.
async fn watch(data: String) -> Result<(), anyhow::Error> {
    info!("Watching {}", data.yellow());

    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = notify::watcher(tx, Duration::from_secs(2))?;
    watcher.watch(data, notify::RecursiveMode::Recursive)?;

    let rx = Arc::new(Mutex::new(rx));

    loop {
        let irx = rx.clone();
        match tokio::task::spawn_blocking(move ||
            irx.lock().unwrap().recv()).await {
            Ok(Ok(o)) => Data::data_event(o),
            _ => break Err(anyhow!("Error while watching data"))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    use env_logger::Env;
    env_logger::from_env(Env::default().default_filter_or("dars=info")).init();

    info!("𓆣 𓃢  (DARS DAP v{})", VERSION);

    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("a", "address", "listening socket address (default: 127.0.0.1:8001)", "ADDR");
    opts.optflag("w", "watch", "watch for changes in data dir");
    opts.optflag("h", "help", "print this help menu");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!(f.to_string()) }
    };

    if matches.opt_present("h") {
        let brief = format!("Usage: {} [options] DATA", program);
        print!("{}", opts.usage(&brief));
        println!("\nThe directory specified with DATA is searched for supported datasets.\n\
                    If DATA is specified with a trailing \"/\" (e.g. \"data/\"), the folder\n\
                    name is not included at the end-point for the dataset. All datasets are\n\
                    available under the /data root. A list of datasets may be queried at /data.\n\
                    \n\
                    If no DATA is specified, \"data/\" is used.");
        return Ok::<_,anyhow::Error>(());
    }

    let datadir: String = if !matches.free.is_empty() {
        matches.free[0].clone()
    } else {
        "data/".to_string()
    };

    let addr: SocketAddr = matches.opt_get_default("a", "127.0.0.1:8001".parse()?)?;

    {
        let rdata = DATA.clone();
        let mut data = rdata.write().unwrap();
        data.init_root(datadir.clone());
    }

    let msvc = make_service_fn(|_| async move {
        Ok::<_, Error>(
            service_fn(|req| async move {
                let m = req.method().clone();
                let u = req.uri().clone();

                let r = match (req.method(), req.uri().path()) {
                    (&Method::GET, "/") => Response::builder().body(Body::from("DAP!\n\n(checkout /data)")),
                    (&Method::GET, "/data") | (&Method::GET, "/data/") => Data::datasets(req),
                    (&Method::GET, p) if p.starts_with("/data/") => Data::dataset(req).await,
                    _ => Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty())
                };

                let s = match &r {
                    Ok(ir) => match ir.status().is_success() {
                        true => ir.status().to_string().yellow(),
                        false => ir.status().to_string().red()
                    },
                    Err(e) => e.to_string().red()
                };

                debug!("{} {} -> {}", m.to_string().blue(), u, s);

                r
            }
            ))
    });

    let server = Server::bind(&addr)
        .serve(msvc)
        .map(|r| r.map_err(|e| anyhow!(e)));

    info!("Listening on {}", format!("http://{}", addr).yellow());

    use futures::future::{Abortable, AbortHandle};
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    let server = Abortable::new(server, abort_registration);

    if matches.opt_present("w") {
        tokio::task::spawn(watch(datadir).then(async move |e| {
            error!("Error while watching data directory: {:?}", e);

            abort_handle.abort()
        }));
    }

    server.map(|r| match r {
            Ok(r) => r,
            Err(e) => Err(anyhow!(e))
        }).inspect(|r| match r {
            Ok(_) => info!("Shutting down server."),
            Err(e) => error!("Server aborted: {:?}", e)
        }).await
}

