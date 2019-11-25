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
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;
#[macro_use] extern crate anyhow;

use std::sync::{Arc,RwLock};
use hyper::{
    Server, Body, Response, Error, Request, Method, StatusCode,
    service::{service_fn, make_service_fn}
};
use futures::FutureExt;

pub mod datasets;
mod nc;

use datasets::{Data, Dataset};

lazy_static! {
    pub static ref DATA: Arc<RwLock<Data>> = Arc::new(RwLock::new(Data::init()));
}

// TODO: experiment with a larger thread-pool. a lot of requests will rely on file IO which
// are just going to be blocking anyway. or are these handled by blocking_threads(..)
// anyways?
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    use env_logger::Env;
    env_logger::from_env(Env::default().default_filter_or("dars=debug")).init();

    info!("Hello, world!");

    {
        let rdata = DATA.clone();
        let mut data = rdata.write().unwrap();

        data.datasets.push(
            Arc::new(
                nc::NcDataset::open("data/coads_climatology.nc".to_string()).unwrap()));
    }

    let addr = ([127, 0, 0, 1], 8001).into();

    let msvc = make_service_fn(|_| async move {
        Ok::<_, Error>(
            service_fn(|req| async move {
                match (req.method(), req.uri().path()) {
                    (&Method::GET, "/catalog.xml") => Response::builder().status(StatusCode::NOT_IMPLEMENTED).body(Body::empty()),
                    (&Method::GET, "/") => Response::builder().body(Body::from("Hello world")),
                    _ => {
                        if req.uri().path().starts_with("/data/") {
                            Data::dataset(req).await
                        } else {
                            Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty())
                        }
                    }
                }
            }
            ))
    });


    let server = Server::bind(&addr)
        .serve(msvc);

    info!("Listening on http://{}", addr);
    server.await.map_err(|e| anyhow!("SDf"))
}

