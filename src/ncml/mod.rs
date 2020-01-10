use hyper::{Response, Body, StatusCode};
use async_trait::async_trait;
use std::path::PathBuf;
use percent_encoding::percent_decode_str;
use walkdir::WalkDir;

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
///
/// No handling of overlapping coordinate variable is done, it is concatenated in order listed.
pub struct NcmlDataset {
    filename: PathBuf,
    _aggregation_type: AggregationType,
    aggregation_dim: String,
    members: Vec<NcmlMember>,
    das: nc::das::NcDas,
    dds: dds::NcmlDds,
    dim_n: usize
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

        let aggregation = root.first_element_child().ok_or(anyhow!("no aggregation tag found"))?;
        ensure!(aggregation.tag_name().name() == "aggregation", "expected aggregation tag");

        // TODO: use match to enum
        let aggregation_type = aggregation.attribute("type").ok_or(anyhow!("aggregation type not specified"))?;
        ensure!(aggregation_type == "joinExisting", "only 'joinExisting' type aggregation supported");

        // TODO: only available on certain aggregation types
        let aggregation_dim = aggregation.attribute("dimName").ok_or(anyhow!("aggregation dimension not specified"))?;

        let mut files: Vec<Vec<PathBuf>> = aggregation.children()
            .filter(|c| c.is_element())
            .map(|e|
                match e.tag_name().name() {
                    "netcdf" => e.attribute("location").map(|l| {
                        let l = PathBuf::from(l);
                        match l.is_relative() {
                            true => vec!(base.map_or(l.clone(), |b| b.join(l))),
                            false => vec!(l)
                        }
                    }),
                    "scan" => e.attribute("location").map(|l| {
                        let l: PathBuf = match PathBuf::from(l) {
                            l if l.is_relative() => base.map_or(l.clone(), |b| b.join(l)),
                            l => l
                        };

                        if let Some(sf) = e.attribute("suffix") {
                            debug!("Scanning {:?}, suffix: {}", l, sf);

                            let mut v = Vec::new();

                            for entry in WalkDir::new(l)
                                .follow_links(true)
                                .into_iter()
                                .filter_entry(|entry|
                                    !entry.file_name().to_str().map(|s| s.starts_with(".")).unwrap_or(false))
                                {
                                    if let Ok(entry) = entry {
                                        match entry.metadata() {
                                            Ok(m) if m.is_file() && entry.path().to_str().map(|s| s.ends_with(sf)).unwrap_or(false) => v.push(entry.into_path()),
                                            _ => ()
                                        }
                                    };
                                }
                            v.sort();
                            v
                        } else {
                            error!("no suffix specified in ncml scan tag");
                            Vec::new()
                        }
                    }),
                    t => { error!("unknown tag: {}", t); None }
                }
            ).collect::<Option<Vec<Vec<PathBuf>>>>().ok_or(anyhow!("could not parse file list"))?;
        files.sort();

        let mut members = files.iter().flatten().map(|p| NcmlMember::open(p, aggregation_dim)).collect::<Result<Vec<NcmlMember>,_>>()?;
        members.sort_by(|a, b| a.rank.partial_cmp(&b.rank).unwrap_or(std::cmp::Ordering::Equal));

        // DAS should be same for all members (hopefully), using first.
        let first = members.first().ok_or(anyhow!("no members in aggregate"))?;
        let das = nc::das::NcDas::build(first.f.clone())?;

        let dim_n: usize = members.iter().map(|m| m.n).sum();
        let dds = dds::NcmlDds::build(first.f.clone(), &filename, aggregation_dim, dim_n)?;

        Ok(NcmlDataset {
            filename: filename.clone(),
            _aggregation_type: AggregationType::JoinExisting,
            aggregation_dim: aggregation_dim.to_string(),
            members: members,
            das: das,
            dds: dds,
            dim_n: dim_n
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
        let mut query = self.parse_query(query);

        match self.dds.dds(&self.members[0].f.clone(), &mut query) {
            Ok(dds) => Response::builder().body(Body::from(dds)),
            _ => Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty())
        }
    }

    async fn dods(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error> {
        use futures::stream::{self, StreamExt};
        let mut query = self.parse_query(query);

        let dds = if let Ok(r) = self.dds.dds(&self.members[0].f.clone(), &mut query) {
            r.into_bytes()
        } else {
            return Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty());
        };

        let dods = dods::xdr(&self, query);

        let s = stream::once(async move { Ok::<_,anyhow::Error>(dds) })
            .chain(
                stream::once(async { Ok::<_,anyhow::Error>(String::from("\nData:\r\n").into_bytes()) }))
            .chain(dods)
            .inspect(|e| match e {
                Err(ee) => error!("error while streaming: {:?}", ee),
                _ => ()
            });

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
        crate::testcommon::init();
        let nm = NcmlDataset::open("data/ncml/aggExisting.ncml").unwrap();

        println!("files: {:#?}", nm.members);
    }

}
