use hyper::{Response, Body, StatusCode};
use std::sync::Arc;
use async_trait::async_trait;
use percent_encoding::percent_decode_str;
use std::path::PathBuf;

use super::Dataset;
use super::nc;

mod member;

pub enum AggregationType {
    JoinExisting,
}

/// # NCML aggregated datasets
///
/// Reference: https://www.unidata.ucar.edu/software/netcdf-java/current/ncml/Aggregation.html
///
/// ## JoinExisting
///
/// The aggregating dimension must already have a coordinate variable. Only the outer (slowest varying) dimension
/// (first index) may be joined.
///
/// The coordinate variable may be overlapping between the dataset, the priority is last first.
///
pub struct NcmlDataset {
    filename: PathBuf,
    aggregation_type: AggregationType,
    aggregation_dim: String,
    files: Vec<PathBuf>,
    das: nc::das::NcDas
}

impl NcmlDataset {
    pub fn open<P>(filename: P) -> anyhow::Result<NcmlDataset>
        where P: Into<PathBuf>
    {
        let filename = filename.into();
        info!("Adding ncml dataset: {:?}", filename);

        let base = filename.parent();

        let xml = std::fs::read_to_string(filename.clone())?;
        let xml = roxmltree::Document::parse(&xml)?;
        let root = xml.root_element();

        let aggregation = root.first_element_child().expect("no aggregation tag found");
        ensure!(aggregation.tag_name().name() == "aggregation", "expected aggregation tag");

        let aggregation_type = aggregation.attribute("type").expect("aggregation type not specified");
        ensure!(aggregation_type == "joinExisting", "only 'joinExisting' type aggregation supported");

        let aggregation_dim = aggregation.attribute("dimName").expect("aggregation dimension not specified");

        let files: Vec<PathBuf> = aggregation.children()
            .filter(|c| c.is_element())
            .map(|e| e.attribute("location").map(|l| {
                let l = PathBuf::from(l);
                match l.is_relative() {
                    true => base.map_or(l.clone(), |b| b.join(l)),
                    false => l
                }
            })).collect::<Option<Vec<PathBuf>>>().expect("could not parse file list");

        // DAS should be same for all members (hopefully), using first.
        let first = files.first().expect("no members in aggregate");
        let das = nc::das::NcDas::build(first)?;

        // Add each dataset and identify the coodinate dimension. Check that it is the
        // first in all variables. Identify the range (only accept monotonically
        // increasing) and overlap.

        Ok(NcmlDataset {
            filename: filename.strip_prefix("data/").unwrap().into(),
            aggregation_type: AggregationType::JoinExisting,
            aggregation_dim: aggregation_dim.to_string(),
            files: files,
            das: das
        })
    }
}
#[async_trait]
impl Dataset for NcmlDataset {
    fn name(&self) -> String {
        self.filename.to_string_lossy().to_string()
    }

    async fn das(&self) -> Result<Response<Body>, hyper::http::Error> {
        Response::builder().body(Body::from(self.das.to_string()))
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

        println!("files: {:#?}", nm.files);
    }

}
