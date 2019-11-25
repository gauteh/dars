use hyper::{Response, Body, StatusCode};
use futures_util::stream::{self, Stream, StreamExt};
use futures::{Future, FutureExt};
use std::iter::FromIterator;
use std::sync::{Arc, Mutex};
use std::pin::Pin;
use netcdf;
use anyhow;
use async_trait::async_trait;
use async_std::task;

use super::Dataset;

fn ok<S>(x: S) -> Result<String, std::io::Error>
    where S: Into<String>
{
    Ok::<_, std::io::Error>(x.into())
}

struct NcDas {
    f: String,
    globals:Arc<Mutex<Vec<String>>>,
}

impl NcDas {
    fn format_attr(a: &netcdf::Attribute) -> String {
        use netcdf::attribute::AttrValue::*;

        match a.value() {
            Ok(Str(s)) => format!("String {} \"{}\"\n", a.name(), s), // TODO: escape

            _ => "".to_string()
        }
    }

    pub async fn build(ds: &NcDataset) -> anyhow::Result<NcDas> {
        debug!("opening: {} to build das", ds.filenames[0]);
        let p = format!("data/{}", ds.filenames[0]);

        let k = p.clone();
        let nc = task::spawn_blocking(move || {
            netcdf::open(k)
        }).await?;


        let mut n = NcDas {
            f: p.clone(),
            globals: Arc::new(Mutex::new(nc.attributes().map(NcDas::format_attr).collect()))

        };

        Ok(n)
    }

    pub async fn stream(&self) -> impl Stream<Item=Result<String, std::io::Error>> + 'static {
        // let globals: Vec<Result<String, std::io::Error>> = self.globals.iter().map(ok).collect();

        let gl = self.globals.lock().unwrap();
        let g = gl.clone();

        let globals = g.iter().map(ok);


        stream::once(async { ok("   NC_GLOBAL {\n") })
        .chain(stream::iter(globals))
        .chain(stream::once(async { ok("   }\n") }))
    }
}

pub struct NcDataset {
    /* a dataset may consist of several files */
    pub filenames: Vec<String>,
    pub mtime: std::time::SystemTime
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
}

#[async_trait]
impl Dataset for NcDataset {
    fn name(&self) -> String {
        self.filenames[0].clone()
    }

    async fn das(&self) -> Result<Response<Body>, hyper::http::Error> {
        debug!("building Data Attribute Structure (DAS)");

        let s = stream::once(
            async { ok(String::from("Attributes {\n")) })
            .chain(NcDas::build(self).await.unwrap().stream().await)
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

