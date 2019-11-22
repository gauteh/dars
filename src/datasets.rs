use http::status::StatusCode;
use tide::error::ResultExt;
use http_service::Body;
use futures::stream;

pub struct Data {
    pub root: String,
    pub datasets: Vec<Box<dyn Dataset + Send + Sync>>
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

    pub async fn dataset(cx: tide::Context<Data>) -> tide::EndpointResult {
        let ds: String = cx.param("dataset").client_err()?;
        let DsRequest(ds, dst) = Data::parse_request(ds);

        debug!("looking for dataset: {}", ds);

        let ds = match cx.state().datasets.iter().find(|&d| d.name() == ds) {
            Some(dataset) => Ok(dataset),
            None => Err(StatusCode::NOT_FOUND)
        }?;

        debug!("found dataset: {}", ds.name());

        match dst {
            DsRequestType::Das => Ok(ds.das(&cx)),
            _ => Err(StatusCode::NOT_IMPLEMENTED)?
        }
    }
}

pub trait Dataset {
    fn name(&self) -> String;
    // fn attributes(&self) -> impl Iterator<Item=String>;

    fn das(&self, cx: &tide::Context<Data>) -> tide::Response {
        // Get all attributes (query string does not matter)
        use std::iter;

        // Ok(iter::once("Attributes {").chain(iter::once("}")))
        // tide::Response::new(
        //     Body::from_stream(
        //         stream::iter(vec![1, 2, 3])))

        let s = Body::from_stream(stream::iter("asdf".as_bytes()));

        tide::Response::new(Body::from("asdf"))

        // tide::Response::with_err_status(StatusCode::NOT_IMPLEMENTED)
    }

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
