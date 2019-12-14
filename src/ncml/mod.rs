use hyper::{Response, Body, StatusCode};
use async_trait::async_trait;
use std::path::PathBuf;
use percent_encoding::percent_decode_str;

use super::Dataset;
use super::nc::{self, dds::Dds};

mod member;
mod dds;
mod dods;

use member::NcmlMember;

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
pub struct NcmlDataset {
    filename: PathBuf,
    _aggregation_type: AggregationType,
    aggregation_dim: String,
    members: Vec<NcmlMember>,
    das: nc::das::NcDas,
    dds: dds::NcmlDds
}

impl NcmlDataset {
    pub fn open<P>(filename: P) -> anyhow::Result<NcmlDataset>
        where P: Into<PathBuf>
    {
        let filename = filename.into();
        info!("Loading {:?}..", filename);

        let base = filename.parent();

        let xml = std::fs::read_to_string(filename.clone())?;
        let xml = roxmltree::Document::parse(&xml)?;
        let root = xml.root_element();

        let aggregation = root.first_element_child().expect("no aggregation tag found");
        ensure!(aggregation.tag_name().name() == "aggregation", "expected aggregation tag");

        // TODO: use match to enum
        let aggregation_type = aggregation.attribute("type").expect("aggregation type not specified");
        ensure!(aggregation_type == "joinExisting", "only 'joinExisting' type aggregation supported");

        // TODO: only available on certain aggregation types
        let aggregation_dim = aggregation.attribute("dimName").expect("aggregation dimension not specified");

        let files: Vec<PathBuf> = aggregation.children()
            .filter(|c| c.is_element() && c.tag_name().name() == "netcdf")
            .map(|e| e.attribute("location").map(|l| {
                let l = PathBuf::from(l);
                match l.is_relative() {
                    true => base.map_or(l.clone(), |b| b.join(l)),
                    false => l
                }
            })).collect::<Option<Vec<PathBuf>>>().expect("could not parse file list");

        let members = files.iter().map(|p| NcmlMember::open(p, aggregation_dim)).collect::<Result<Vec<NcmlMember>,_>>()?;

        // DAS should be same for all members (hopefully), using first.
        let first = files.first().expect("no members in aggregate");
        let das = nc::das::NcDas::build(first)?;

        let dim_n: usize = members.iter().map(|m| m.n).sum();
        let dds = dds::NcmlDds::build(first, &filename, aggregation_dim, dim_n)?;

        Ok(NcmlDataset {
            filename: filename.clone(),
            _aggregation_type: AggregationType::JoinExisting,
            aggregation_dim: aggregation_dim.to_string(),
            members: members,
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
impl Dataset for NcmlDataset {
    fn name(&self) -> String {
        self.filename.to_string_lossy().to_string()
    }

    async fn das(&self) -> Result<Response<Body>, hyper::http::Error> {
        Response::builder().body(Body::from(self.das.to_string()))
    }

    async fn dds(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        let query = self.parse_query(query);

        match self.dds.dds(&self.members[0].f.clone(), &query) {
            Ok(dds) => Response::builder().body(Body::from(dds)),
            _ => Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty())
        }
    }

    async fn dods(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        use futures::stream::{self, StreamExt};
        let query = self.parse_query(query);

        let dds = if let Ok(r) = self.dds.dds(&self.members[0].f.clone(), &query) {
            r.into_bytes()
        } else {
            return Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty());
        };

        let dods = dods::xdr(&self, query);

        let s = stream::once(async move { Ok::<_,anyhow::Error>(dds) })
            .chain(
                stream::once(async { Ok::<_,anyhow::Error>(String::from("\nData:\r\n").into_bytes()) }))
            .chain(dods);

        Response::builder().body(Body::wrap_stream(s))
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

        println!("files: {:#?}", nm.members);
    }

}
