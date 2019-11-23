use hyper::{Response, Body, StatusCode};
use futures_util::stream::{self, Stream, StreamExt};
use futures::task::{Context, Poll};
use futures::{Future, FutureExt, future::Ready};
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

        stream::once(async { ok("Attributes {") })
        .chain(
            stream::once( async move {
                let vec: Vec<String> = {
                    let s = f.lock().unwrap();
                    s.attributes().map(|a| String::from(a.name())).collect()
                };
                let k = vec.iter().next().unwrap();
                // let k: String = "asdf".to_string();
                ok(String::from(k))
            }))

        .chain(stream::once(async { ok("}") }))
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


//     fn attributes(&self) -> impl Iterator<Item=Result<&str, std::io::Error>> {
//         use std::iter;

//         iter::once("NC_GLOBAL {\n")
//             .chain(iter::once("}"))
//             .map(|c| Ok::<_, std::io::Error>(c))
//     }

    pub fn das(&self) -> Result<Response<Body>, hyper::http::Error> {
        debug!("building Data Attribute Structure (DAS)");

        use std::iter;
        use std::io::Error;
        use itertools::Itertools;

        // let a = self.attributes().map(|a| a.unwrap());

        // let a = iter::once("NC_GLOBAL {\n")
        //     .chain(iter::once("}"));

        // let attrs = iter::once(
        //     "Attributes {")
        //     .chain(a)
        //     .chain(iter::once("}"))
        //     .intersperse("\n")
        //     .map(|c| Ok::<_,Error>(c));

        // let s = stream::iter(attrs);
        let s = stream::once(
            async { ok(String::from("Attributes {")) })
            .chain(NcDas::build(self).stream());
        // .chain(
        //     stream::once(async {
        //         let n = self.name().clone();
        //         Ok::<_,std::io::Error>(n) }));

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

