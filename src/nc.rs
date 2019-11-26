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

struct NcDas {
    das: Arc<String>
}

impl NcDas {
    fn format_attr(indent: usize, a: &netcdf::Attribute) -> String {
        use netcdf::attribute::AttrValue::*;

        match a.value() {
            Ok(Str(s)) =>   format!("{}String {} \"{}\";\n", " ".repeat(indent), a.name(), s), // TODO: escape
            Ok(Float(f)) => format!("{}Float32 {} {:+E};\n", " ".repeat(indent), a.name(), f),

            _ => "".to_string()
        }
    }

    fn push_attr<'a>(indent: usize, das: &mut String, a: impl Iterator<Item = &'a netcdf::Attribute>) -> () {
        das.push_str(&a.map(|aa| NcDas::format_attr(indent, aa)).collect::<String>());
    }

    pub fn build(f: String) -> anyhow::Result<NcDas> {
        debug!("building Data Attribute Structure (DAS) for {}", f);

        let nc = netcdf::open(f)?;

        let indent = 4;
        let mut das: String = "Attributes {\n".to_string();

        if let Some(_) = nc.attributes().next() {
            das.push_str("    NC_GLOBAL {\n");
            NcDas::push_attr(2*indent, &mut das, nc.attributes());
            das.push_str("    }\n");
        }

        for var in nc.variables() {
            das.push_str(&format!("    {} {{\n", var.name()));
            NcDas::push_attr(2*indent, &mut das, var.attributes());
            das.push_str("    }\n");
        }

        das.push_str("}");

        Ok(NcDas {
            das: Arc::new(das)
        })
    }
}

pub struct NcDataset {
    /* a dataset may consist of several files */
    pub filenames: Vec<String>,
    pub mtime: std::time::SystemTime,
    das: NcDas
}

impl NcDataset {
    pub fn open(filename: String) -> anyhow::Result<NcDataset> {
        info!("opening: {}", filename);
        use std::fs;

        let md = fs::metadata(&filename)?;
        let mtime = md.modified()?;
        debug!("{}: mtime: {:?}", filename, mtime.elapsed().unwrap());

        let das = NcDas::build(filename.clone())?;

        Ok(NcDataset {
            filenames: vec![String::from(filename.trim_start_matches("data/"))],
            mtime: mtime,
            das: das
        })
    }
}

#[async_trait]
impl Dataset for NcDataset {
    fn name(&self) -> String {
        self.filenames[0].clone()
    }

    async fn das(&self) -> Result<Response<Body>, hyper::http::Error> {
        let a = self.das.das.clone();

        Response::builder().body(Body::from(a.to_string()))
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

