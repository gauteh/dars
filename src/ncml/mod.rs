use hyper::{Response, Body, StatusCode};
use std::sync::Arc;
use async_trait::async_trait;
use percent_encoding::percent_decode_str;

use super::Dataset;

pub struct NcmlDataset {
    pub filename: std::path::PathBuf,

}

impl NcmlDataset {
    pub fn open<P>(filename: P) -> anyhow::Result<NcmlDataset>
        where P: Into<std::path::PathBuf>
    {
        let filename = filename.into();
        info!("Adding ncml dataset: {:?}", filename);
        // let filename = String::from(filename.to_str().unwrap());

        Ok(NcmlDataset {
            filename: filename.strip_prefix("data/").unwrap().into(),
        })
    }
}
#[async_trait]
impl Dataset for NcmlDataset {
    fn name(&self) -> String {
        self.filename.to_string_lossy().to_string()
    }

    async fn das(&self) -> Result<Response<Body>, hyper::http::Error> {
        Response::builder()
            .status(StatusCode::NOT_IMPLEMENTED)
            .body(Body::empty())
    }

    async fn dds(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        Response::builder()
            .status(StatusCode::NOT_IMPLEMENTED)
            .body(Body::empty())
    }

    async fn dods(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        Response::builder()
            .status(StatusCode::NOT_IMPLEMENTED)
            .body(Body::empty())
    }

    async fn nc(&self) -> Result<Response<Body>, hyper::http::Error> {
        Response::builder()
            .status(StatusCode::NOT_IMPLEMENTED)
            .body(Body::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ncml_open() {
        let nm = NcmlDataset::open("data/ncml/aggExisting.ncml").unwrap();
    }

}
