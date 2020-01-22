use async_trait::async_trait;
use hyper::{Body, Response, StatusCode};
use percent_encoding::percent_decode_str;
use std::sync::Arc;

use super::{datasets::FileEvent, Dataset};

pub mod das;
pub mod dds;
pub mod dods;

use das::NcDas;
use dds::{Dds, NcDds};

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
    where
        P: Into<std::path::PathBuf>,
    {
        let filename = filename.into();
        info!("Loading {:?}..", filename);

        let f = Arc::new(netcdf::open(filename.clone())?);
        let das = NcDas::build(&f)?;
        let dds = NcDds::build(filename.clone(), &f)?;

        Ok(NcDataset {
            filename,
            f,
            das,
            dds,
        })
    }

    /// Parses and decodes list of variables and constraints submitted
    /// through the URL query part.
    fn parse_query(&self, query: Option<String>) -> Vec<String> {
        match query {
            Some(q) => q
                .split(',')
                .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
                .collect(),

            None => self.dds.default_vars(),
        }
    }
}

#[async_trait]
impl Dataset for NcDataset {
    fn name(&self) -> String {
        self.filename.to_string_lossy().to_string()
    }

    async fn das(&self) -> Result<Response<Body>, hyper::http::Error> {
        Response::builder()
            .header("Content-Type", "text/plain")
            .header("Content-Description", "dods-das")
            .header("XDODS-Server", "dars")
            .body(Body::from(self.das.to_string()))
    }

    async fn dds(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        let mut query = self.parse_query(query);

        match self.dds.dds(&self.f, &mut query) {
            Ok(dds) => Response::builder()
                .header("Content-Type", "text/plain")
                .header("Content-Description", "dods-dds")
                .header("XDODS-Server", "dars")
                .body(Body::from(dds)),
            _ => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty()),
        }
    }

    async fn dods(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        use futures::stream::{self, StreamExt};
        let mut query = self.parse_query(query);

        let dds = if let Ok(r) = self.dds.dds(&self.f.clone(), &mut query) {
            r.into_bytes()
        } else {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty());
        };

        let s = stream::once(async move { Ok::<_, anyhow::Error>(dds) })
            .chain(stream::once(async {
                Ok::<_, anyhow::Error>(String::from("\nData:\r\n").into_bytes())
            }))
            .chain(dods::xdr(self.f.clone(), query))
            .inspect(|e| {
                if let Err(e) = e {
                    error!("error while streaming: {:?}", e);
                }
            });

        Response::builder()
            .header("Content-Type", "application/octet-stream")
            .header("Content-Description", "dods-data")
            .header("XDODS-Server", "dars")
            .body(Body::wrap_stream(s))
    }

    async fn raw(&self) -> Result<Response<Body>, hyper::http::Error> {
        use futures::StreamExt;
        use tokio::fs::File;
        use tokio_util::codec;

        let filename = self.filename.clone();

        File::open(filename)
            .await
            .map(|file| {
                Response::builder()
                    .header("Content-Type", "application/octet-stream")
                    .header("Content-Disposition", "attachment")
                    .header("XDODS-Server", "dars")
                    .body(Body::wrap_stream(
                        codec::FramedRead::new(file, codec::BytesCodec::new())
                            .map(|r| r.map(|bytes| bytes.freeze())),
                    ))
            })
            .unwrap_or_else(|_| {
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
            })
    }

    fn file_event(&mut self, _: FileEvent) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::testcommon::init;

    #[test]
    fn open_dataset() {
        init();

        NcDataset::open("data/coads_climatology.nc").unwrap();
    }
}
