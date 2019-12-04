use hyper::{Response, Body, StatusCode};
use std::sync::Arc;
use netcdf;
use anyhow;
use async_trait::async_trait;

use super::Dataset;

mod dds;
mod dods;

use dds::*;

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

        // XXX: maybe not needed for RO?
        // if nc.dimensions().any(|d| d.is_unlimited()) {
        //     das.push_str("    DODS_EXTRA {\n");
        //     for dim in nc.dimensions() {
        //         das.push_str(&format!("        String Unlimited_Dimension \"{}\";\n", dim.name()));
        //     }
        //     das.push_str("    }\n");
        // }

        das.push_str("}");

        Ok(NcDas {
            das: Arc::new(das)
        })
    }
}

pub struct NcDataset {
    pub filename: String,
    pub mtime: std::time::SystemTime,
    f: Arc<netcdf::File>,
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
            f: Arc::new(netcdf::open(filename).unwrap()),
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
        Response::builder().body(Body::from(self.das.das.to_string()))
    }

    async fn dds(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        let query = query.map(|s| s.split(",").map(|s| s.to_string()).collect());
        match self.dds.dds(&self.f.clone(), &query) {
            Ok(dds) => Response::builder().body(Body::from(dds)),
            _ => Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty())
        }
    }

    async fn dods(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        use futures::stream::{self, StreamExt};

        let query: Vec<String> = match query {
            Some(q) => q.split(",").map(|s| s.to_string()).collect(),
            None =>    self.dds.vars.keys().map(|s| s.to_string()).collect()
        };

        let squery = Some(query.clone()); // not pretty

        let dds = if let Ok(r) = self.dds.dds(&self.f.clone(), &squery) {
            r.into_bytes()
        } else {
            return Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty());
        };

        let dods = dods::xdr(self.f.clone(), query);

        let s = stream::once(async move { Ok::<_,anyhow::Error>(dds) })
            .chain(
                stream::once(async { Ok::<_,anyhow::Error>(String::from("\nData:\r\n").into_bytes()) }))
            .chain(dods);

        Response::builder().body(Body::wrap_stream(s))
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

        NcDataset::open("data/coads_climatology.nc".to_string()).unwrap();
    }
}

