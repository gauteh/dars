use hyper::{Response, Body};
use std::sync::Arc;
use netcdf;
use anyhow;
use async_trait::async_trait;

use super::Dataset;

mod dds;
mod dods;

use dds::*;
use dods::*;

struct NcDas {
    das: Arc<String>
}

impl NcDas {
    fn format_attr(indent: usize, a: &netcdf::Attribute) -> String {
        use netcdf::attribute::AttrValue::*;

        // TODO: escape names and values

        match a.value() {
            Ok(Str(s)) =>   format!("{}String {} \"{}\";\n", " ".repeat(indent), a.name(), s),
            Ok(Float(f)) => format!("{}Float32 {} {:+E};\n", " ".repeat(indent), a.name(), f),
            Ok(Double(f)) => format!("{}Float64 {} {:+E};\n", " ".repeat(indent), a.name(), f),

            Ok(v) => format!("{}Unimplemented {} {:?};\n", " ".repeat(indent), a.name(), v),
            Err(_) => "Err".to_string()
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

        // TODO: Groups

        if nc.dimensions().any(|d| d.is_unlimited()) {
            das.push_str("    DODS_EXTRA {\n");
            for dim in nc.dimensions() {
                das.push_str(&format!("        String Unlimited_Dimension \"{}\";\n", dim.name()));
            }
            das.push_str("    }\n");
        }

        das.push_str("}");

        Ok(NcDas {
            das: Arc::new(das)
        })
    }
}

pub struct NcDataset {
    pub filename: String,
    pub mtime: std::time::SystemTime,
    das: NcDas,
    dds: NcDds
}

impl NcDataset {
    pub fn open(filename: String) -> anyhow::Result<NcDataset> {
        info!("opening: {}", filename);
        use std::fs;

        let md = fs::metadata(&filename)?;
        let mtime = md.modified()?;
        debug!("{}: mtime: {:?}", filename, mtime.elapsed().unwrap());

        let das = NcDas::build(filename.clone())?;
        let dds = NcDds::build(filename.clone())?;

        Ok(NcDataset {
            filename: String::from(filename.trim_start_matches("data/")),
            mtime: mtime,
            das: das,
            dds: dds
        })
    }
}

#[async_trait]
impl Dataset for NcDataset {
    fn name(&self) -> String {
        self.filename.clone()
    }

    async fn das(&self) -> Result<Response<Body>, hyper::http::Error> {
        let a = self.das.das.clone();

        Response::builder().body(Body::from(a.to_string()))
    }

    async fn dds(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        let query = query.map(|s| s.split(",").map(|s| s.to_string()).collect());
        Response::builder().body(Body::from(self.dds.dds(&query)))
    }

    async fn dods(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        let query = query.map(|s| s.split(",").map(|s| s.to_string()).collect());

        let dds = self.dds.dds(&query);

        use futures::stream::{self, Stream, StreamExt};
        let i = (1..5).map(|x| async move { Ok::<_, std::io::Error>(x.to_string()) });
        let f: futures::stream::FuturesOrdered<_> = i.collect();

        let b = Body::wrap_stream(f);

        Response::builder().body(b)
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

