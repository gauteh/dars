/* Some notes:
 *
 * host env:
 * - Allow lots of open files in host env
 * - chroot process
 * - not root, not even in docker
 * - mount data RO into chroot
 *
 * design:
 * - probably preload meta-data like attributes and variables in order
 *   to avoid file opens.
 * - need a way to determine if file has changed (mtime), is this
 *   syscall as slow as open? hopefully not.
 * - use mtime on dir to track new files.
 *
 * testing:
 * - use wrk or ab to test, w/o file open wrk gives about 70k request/sec. even including the
 * arc and locks.
 *
 */
#![recursion_limit="1024"]

#![feature(test)]
extern crate test;

#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;
#[macro_use] extern crate anyhow;

use std::sync::{Arc, Mutex, RwLock};
use futures::{future, FutureExt};
use std::time::Duration;
use hyper::{
    Server, Body, Response, Error, Method, StatusCode,
    service::{service_fn, make_service_fn}
};
use colored::Colorize;
use notify::Watcher;

pub mod datasets;
mod dap2;
mod nc;
mod ncml;

use datasets::{Data, Dataset};

lazy_static! {
    pub static ref DATA: Arc<RwLock<Data>> = Arc::new(RwLock::new(Data::init()));
}

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

// This watches for new files and changes, deletions of loaded
// files. It would be safer to handle change detection / re-load
// on each access, but that would involve a syscall for every DAS
// and DDS request.
//
// On systems where the actual file is not removed untill all file handles
// are closed this should work fairly well.
async fn watch() -> Result<(), anyhow::Error> {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = notify::watcher(tx, Duration::from_secs(2))?;
    watcher.watch("data", notify::RecursiveMode::Recursive)?;

    let rx = Arc::new(Mutex::new(rx));

    info!("Watching ./data/");
    loop {
        let irx = rx.clone();
        match async_std::task::spawn_blocking(move ||
            irx.lock().unwrap().recv()).await {
            Ok(o) => warn!("{:?} happened (not implemented yet)", o),
            Err(_) => break Err(anyhow!("Error while watching data"))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    use env_logger::Env;
    env_logger::from_env(Env::default().default_filter_or("dars=debug")).init();

    info!("𓆣 𓃢  (DARS DAP v{})", VERSION);

    let addr = ([0, 0, 0, 0], 8001).into();

    let msvc = make_service_fn(|_| async move {
        Ok::<_, Error>(
            service_fn(|req| async move {
                let m = req.method().clone();
                let u = req.uri().clone();

                let r = match (req.method(), req.uri().path()) {
                    (&Method::GET, "/catalog.xml") => Response::builder().status(StatusCode::NOT_IMPLEMENTED).body(Body::empty()),
                    (&Method::GET, "/") => Response::builder().body(Body::from("DAP!")),
                    _ => {
                        if req.uri().path().starts_with("/data/") {
                            Data::dataset(req).await
                        } else {
                            Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty())
                        }
                    }
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

    info!("Listening on http://{}", addr);
    future::join(server, watch()).await.0
}

