use hyper::{Response, Body, StatusCode};
use async_trait::async_trait;
use std::sync::Arc;
use std::collections::HashMap;

pub struct Data {
    pub root: String,
    pub datasets: HashMap<String, Arc<dyn Dataset + Send + Sync>>
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
            datasets: HashMap::new()
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

        let data = DATA.clone();
        let ds = data.datasets.get(&ds);

        match ds {
            Some(ds) => {
                match dst {
                    DsRequestType::Das => ds.das().await,
                    DsRequestType::Dds => ds.dds(req.uri().query().map(|s| s.to_string())).await,
                    DsRequestType::Dods => ds.dods(req.uri().query().map(|s| s.to_string())).await,

                    _ => Response::builder().status(StatusCode::NOT_IMPLEMENTED).body(Body::empty())
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

    // async fn nc(&self) -> String {
    //     // serve full file
    //     unimplemented!();
    // }
}
