use hyper::{Response, Body, StatusCode};
use std::sync::Arc;
use async_trait::async_trait;
use percent_encoding::percent_decode_str;

use super::Dataset;

mod das;
mod dds;
mod dods;

use dds::NcDds;
use das::NcDas;

/// NetCDF dataset for DAP server.
///
/// Currently does not implement sub-groups.
pub struct NcDataset {
    pub filename: std::path::PathBuf,
    pub mtime: std::time::SystemTime,
    f: Arc<netcdf::File>,
    das: NcDas,
    dds: NcDds
}

impl NcDataset {
    pub fn open<P>(filename: P) -> anyhow::Result<NcDataset>
        where P: Into<std::path::PathBuf>
    {
        let filename = filename.into();
        info!("Adding netCDF dataset: {:?}", filename);
        use std::fs;

        let md = fs::metadata(&filename)?;
        let mtime = md.modified()?;

        let fstr = filename.to_string_lossy().to_string();
        let das = NcDas::build(fstr.clone())?;
        let dds = NcDds::build(fstr.clone())?;

        Ok(NcDataset {
            filename: filename.strip_prefix("data/").unwrap().into(),
            mtime: mtime,
            f: Arc::new(netcdf::open(filename).unwrap()),
            das: das,
            dds: dds
        })
    }

    /// Parses and decodes list of variables and constraints submitted
    /// through the URL query part.
    fn parse_query(&self, query: Option<String>) -> Vec<String> {
        match query {
            Some(q) => q.split(",").map(|s|
                    percent_decode_str(s).decode_utf8_lossy().into_owned()
                ).collect(),

            None => self.dds.default_vars()
        }
    }
}

#[async_trait]
impl Dataset for NcDataset {
    fn name(&self) -> String {
        self.filename.to_string_lossy().to_string()
    }

    async fn das(&self) -> Result<Response<Body>, hyper::http::Error> {
        Response::builder().body(Body::from(self.das.to_string()))
    }

    async fn dds(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        let query = self.parse_query(query);

        match self.dds.dds(&self.f.clone(), &query) {
            Ok(dds) => Response::builder().body(Body::from(dds)),
            _ => Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty())
        }
    }

    async fn dods(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        use futures::stream::{self, StreamExt};
        let query = self.parse_query(query);

        let dds = if let Ok(r) = self.dds.dds(&self.f.clone(), &query) {
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

    async fn nc(&self) -> Result<Response<Body>, hyper::http::Error> {
        use tokio_util::codec;
        use tokio::fs::File;
        use futures::StreamExt;

        let filename = std::path::Path::new("data").join(self.filename.clone());

        File::open(filename)
            .await
            .map(|file|
                Response::new(
                    Body::wrap_stream(
                        codec::FramedRead::new(
                            file, codec::BytesCodec::new())
                        .map(|r| r.map(|bytes| bytes.freeze())))))
            .or(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty()))

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

        NcDataset::open("data/coads_climatology.nc").unwrap();
    }
}

