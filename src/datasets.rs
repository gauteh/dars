use hyper::{Response, Body, StatusCode};
use async_trait::async_trait;
use std::sync::Arc;

pub struct Data {
    pub root: String,
    pub datasets: Vec<Arc<dyn Dataset + Send + Sync>>
}

enum DsRequestType {
    Das,
    Dds,
    Dods,
    Raw
}

struct DsRequest(String, DsRequestType);

impl Data {
    pub fn init() -> Data {
        Data {
            root: String::from("./data"),
            datasets: vec![]
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
            DsRequest(String::from(&ds), DsRequestType::Raw)
        }
    }

    pub async fn dataset(req: hyper::Request<Body>) -> Result<Response<Body>, hyper::http::Error> {
        use super::DATA;

        let ds: String = req.uri().path().trim_start_matches("/data/").to_string();
        let DsRequest(ds, dst) = Data::parse_request(ds);

        debug!("looking for dataset: {}", ds);
        let ds = {
            let rdata = DATA.clone();
            let data = rdata.read().unwrap();

            match data.datasets.iter().find(|&d| d.name() == ds) {
                Some(ds) => Some(ds.clone()),
                _ => None
            }
        };

        match ds {
            Some(ds) => {
                debug!("found dataset: {}", ds.name());
                match dst {
                    DsRequestType::Das => ds.das().await,

                    _ => Response::builder().status(StatusCode::NOT_IMPLEMENTED).body(Body::empty())
                }
            },
            None => {
                debug!("dataset not found.");
                Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty())
            }
        }
    }
}

#[async_trait]
pub trait Dataset {
    fn name(&self) -> String;

    async fn das(&self) -> Result<Response<Body>, hyper::http::Error>;

    fn dds(&self) -> String {
        panic!("Not implemented.")
    }

    fn dods(&self) -> String {
        panic!("Not implemented.")
    }

    fn ascii(&self) -> String {
        panic!("Not implemented.")
    }

    fn nc(&self) -> String {
        panic!("Not implemented.")
    }
}
