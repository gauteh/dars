use hyper::{Response, Body, StatusCode};
use futures_util::stream::{self, Stream, StreamExt};
use futures::task::{Context, Poll};
use futures::{Future, FutureExt, future::Ready};
use futures::stream::FuturesOrdered;
use std::iter::FromIterator;
use futures_util::future::*;
use std::sync::{Arc, Mutex};
use std::pin::Pin;
use netcdf;
use anyhow;

use super::Dataset;

struct NcDas {
    f: Arc<Mutex<netcdf::file::File>>
}

impl NcDas {
    pub fn build(ds: &NcDataset) -> NcDas {
        debug!("opening: {} to build das", ds.filenames[0]);
        let p = format!("data/{}", ds.filenames[0]);
        NcDas {
            f: Arc::new(Mutex::new(netcdf::open(p).unwrap()))
        }
    }

    pub fn stream(&self) -> impl Stream<Item=Result<String, std::io::Error>> + 'static {
        let f = self.f.clone();
        let m = f.lock().unwrap();
        let globals: Vec<Result<String, std::io::Error>> = m.attributes().map(|a|
            ok(format!("\t\tString {} {:?}\n", a.name(), a.value().unwrap()))
            ).collect();

        stream::once(async { ok("\tNC_GLOBAL {\n") }).chain(
        stream::iter(globals)).chain(
        stream::once(async { ok("\t}\n") }))
    }
}

pub struct NcDataset {
    /* a dataset may consist of several files */
    pub filenames: Vec<String>,
    pub mtime: std::time::SystemTime
}

impl Dataset for NcDataset {
    fn name(&self) -> String {
        self.filenames[0].clone()
    }
}

fn ok<S>(x: S) -> Result<String, std::io::Error>
    where S: Into<String>
{
    Ok::<_, std::io::Error>(x.into())
}

impl NcDataset {
    pub fn open(filename: String) -> anyhow::Result<NcDataset> {
        info!("opening: {}", filename);
        use std::fs;

        let md = fs::metadata(&filename)?;
        let mtime = md.modified()?;
        debug!("{}: mtime: {:?}", filename, mtime.elapsed().unwrap());

        // read attributes
        let f = netcdf::open(filename.clone())?;

        debug!("attributes:");
        for a in f.attributes() {
            debug!("attribute: {}: {:?}", a.name(), a.value());
        }

        Ok(NcDataset {
            filenames: vec![String::from(filename.trim_start_matches("data/"))],
            mtime: mtime
        })
    }

    pub fn das(&self) -> Result<Response<Body>, hyper::http::Error> {
        debug!("building Data Attribute Structure (DAS)");

        let s = stream::once(
            async { ok(String::from("Attributes {\n")) })
            .chain(NcDas::build(self).stream())
            .chain(stream::once(
                async { ok(String::from("}\n")) }));

        let body = Body::wrap_stream(s);

        Response::builder().body(body)
    }

}

#[cfg(test)]
mod test {
    use super::*;

    fn init () {
        std::env::set_var("RUST_LOG", "dars=debug");
        let _ = env_logger::builder().is_test(true).try_init ();
    }

    #[test]
    fn open_dataset() {
        init();

        let f = NcDataset::open("data/coads_climatology.nc".to_string()).unwrap();
    }
}

