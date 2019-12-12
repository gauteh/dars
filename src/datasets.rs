use hyper::{Response, Body, StatusCode};
use async_trait::async_trait;
use std::sync::Arc;
use std::collections::HashMap;
use walkdir::WalkDir;

use super::{
    nc::NcDataset,
    ncml::NcmlDataset
};

pub struct Data {
    pub root: String,
    pub datasets: HashMap<String, Arc<dyn Dataset + Send + Sync>>
}

enum DsRequestType {
    Das,
    Dds,
    Dods,
    Nc
}

struct DsRequest(String, DsRequestType);

impl Data {
    pub fn init() -> Data {
        let mut map: HashMap<String, Arc<dyn Dataset + Send + Sync>> = HashMap::new();

        for entry in WalkDir::new("data")
            .follow_links(true)
            .into_iter()
            .filter_entry(|entry| !entry.file_name().to_str().map(|s| s.starts_with(".")).unwrap_or(false))
        {
            if let Ok(entry) = entry {
                match entry.metadata() {
                    Ok(m) if m.is_file() => {
                        match entry.path().extension() {
                            Some(ext) if ext == "nc" => {
                                match NcDataset::open(entry.path()) {
                                    Ok(ds) => { map.insert(entry.path().to_str().unwrap().to_string().trim_start_matches("data/").to_string(),
                                    Arc::new(ds)); },
                                    _ => warn!("Could not open: {:?}", entry.path())
                                }
                            },
                            Some(ext) if ext == "ncml" => {
                                match NcmlDataset::open(entry.path()) {
                                    Ok(ds) => { map.insert(entry.path().to_str().unwrap().to_string().trim_start_matches("data/").to_string(),
                                    Arc::new(ds)); },
                                    _ => warn!("Could not open: {:?}", entry.path())
                                }
                            },
                            _ => ()
                        }
                    },
                    _ => ()
                }
            }
        }

        Data {
            root: String::from("./data"),
            datasets: map
        }
    }

    fn parse_request(ds: String) -> DsRequest {
        if ds.ends_with(".das") {
            DsRequest(String::from(ds.trim_end_matches(".das")), DsRequestType::Das)
        } else if ds.ends_with(".dds") {
            DsRequest(String::from(ds.trim_end_matches(".dds")), DsRequestType::Dds)
        } else if ds.ends_with(".dods") {
            DsRequest(String::from(ds.trim_end_matches(".dods")), DsRequestType::Dods)
        } else {
            DsRequest(String::from(&ds), DsRequestType::Nc)
        }
    }

    pub async fn dataset(req: hyper::Request<Body>) -> Result<Response<Body>, hyper::http::Error> {
        use super::DATA;

        let ds: String = req.uri().path().trim_start_matches("/data/").to_string();
        let DsRequest(ds, dst) = Data::parse_request(ds);

        let ds = {
            let rdata = DATA.clone();
            let data = rdata.read().unwrap();

            match data.datasets.get(&ds) {
                Some(ds) => Some(ds.clone()),
                None => None
            }
        };

        match ds {
            Some(ds) => {
                match dst {
                    DsRequestType::Das => ds.das().await,
                    DsRequestType::Dds => ds.dds(req.uri().query().map(|s| s.to_string())).await,
                    DsRequestType::Dods => ds.dods(req.uri().query().map(|s| s.to_string())).await,
                    DsRequestType::Nc => ds.nc().await,
                }
            },
            None => {
                Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty())
            }
        }
    }
}

#[async_trait]
pub trait Dataset {
    fn name(&self) -> String;

    async fn das(&self) -> Result<Response<Body>, hyper::http::Error>;
    async fn dds(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error>;
    async fn dods(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error>;
    async fn nc(&self) -> Result<Response<Body>, hyper::http::Error>;
}
