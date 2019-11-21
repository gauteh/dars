use http::status::StatusCode;
use tide::error::ResultExt;

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
            DsRequestType::Das => ds.das(&cx),
            _ => Err(StatusCode::NOT_IMPLEMENTED)?
        }
    }
}

pub trait Dataset {
    fn name(&self) -> String;

    fn das(&self, cx: &tide::Context<Data>) -> tide::EndpointResult {
        Err(StatusCode::NOT_IMPLEMENTED)?
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
