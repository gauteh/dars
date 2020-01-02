use hyper::{Response, Body, StatusCode};
use std::sync::Arc;
use async_trait::async_trait;
use percent_encoding::percent_decode_str;

use super::Dataset;

pub mod das;
pub mod dds;
pub mod dods;

use dds::{NcDds, Dds};
use das::NcDas;

/// NetCDF dataset for DAP server.
///
/// Currently does not implement sub-groups.
pub struct NcDataset {
    pub filename: std::path::PathBuf,
    f: Arc<netcdf::File>,
    das: NcDas,
    dds: NcDds,
}

impl NcDataset {
    pub fn open<P>(filename: P) -> anyhow::Result<NcDataset>
        where P: Into<std::path::PathBuf>
    {
        let filename = filename.into();
        info!("Loading {:?}..", filename);

        let f = Arc::new(netcdf::open(filename.clone())?);
        let das = NcDas::build(f.clone())?;
        let dds = NcDds::build(filename.clone())?;

        Ok(NcDataset {
            filename: filename,
            f: f,
            das: das,
            dds: dds
        })
    }

    /// Parses and decodes list of variables and constraints submitted
    /// through the URL query part.
    fn parse_query(&self, query: Option<String>) -> Vec<String> {
        match query {
            Some(q) => q.split(",")
                        .map(|s|
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
        let mut query = self.parse_query(query);

        match self.dds.dds(&self.f, &mut query) {
            Ok(dds) => Response::builder().body(Body::from(dds)),
            _ => Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty())
        }
    }

    async fn dods(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        use futures::stream::{self, StreamExt};
        let mut query = self.parse_query(query);

        let dds = if let Ok(r) = self.dds.dds(&self.f.clone(), &mut query) {
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

        let filename = self.filename.clone();

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

