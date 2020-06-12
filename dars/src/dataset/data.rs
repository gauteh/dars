///! This module holds the collection of datasets which are available. It utilizes the `dap2`
///! module to parse queries and dispatch metadata or data requests to the `Dataset` implementation
///! on each dataset-source.

use std::collections::HashMap;

use crate::Request;
use crate::hdf5;
use dap2::Constraint;

#[derive(Default)]
pub struct Datasets {
    pub datasets: HashMap<String, DatasetType>,
}

pub enum DatasetType {
    HDF5(hdf5::Hdf5Dataset),
}

impl Datasets {
    pub async fn datasets(&self) -> tide::Result {
        Ok(format!(
            "Index of datasets:\n\n{}",
            self.datasets
                .keys()
                .map(|s| &**s)
                .collect::<Vec<&str>>()
                .join("\n")
        )
        .into())
    }

    pub async fn dataset(&self, req: &Request) -> tide::Result {
        let dset = req.param::<String>("dataset")?;
        let (dset, daprequest) = Datasets::request(&dset);

        let query = req.url().query();
        let constraint = Constraint::parse(req.url().query());
        info!("dataset: {} [{:?}] ({:?})", dset, daprequest, query);
        Ok("".into())
    }

    fn request(dataset: &str) -> (&str, DapRequest) {
        match dataset {
            _ if dataset.ends_with(".das") => (&dataset[..dataset.len()-4], DapRequest::Das),
            _ if dataset.ends_with(".dds") => (&dataset[..dataset.len()-4], DapRequest::Dds),
            _ if dataset.ends_with(".dods") => (&dataset[..dataset.len()-5], DapRequest::Dods),
            _ => (&dataset, DapRequest::Raw),
        }
    }
}

#[derive(Debug)]
enum DapRequest {
    Das,
    Dds,
    Dods,
    Raw
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_types() {
        assert!(matches!(Datasets::request("test.nc.das"), ("test.nc", DapRequest::Das)));
        assert!(matches!(Datasets::request("test.nc.dds"), ("test.nc", DapRequest::Dds)));
        assert!(matches!(Datasets::request("test.nc.dods"), ("test.nc", DapRequest::Dods)));
        assert!(matches!(Datasets::request("test.nc"), ("test.nc", DapRequest::Raw)));
        assert!(matches!(Datasets::request("test.nc.asdf"), ("test.nc.asdf", DapRequest::Raw)));
        assert!(matches!(Datasets::request(".das"), ("", DapRequest::Das)));
        assert!(matches!(Datasets::request(".dds"), ("", DapRequest::Dds)));
        assert!(matches!(Datasets::request(".dods"), ("", DapRequest::Dods)));
        assert!(matches!(Datasets::request(""), ("", DapRequest::Raw)));
        assert!(matches!(Datasets::request(".nc"), (".nc", DapRequest::Raw)));
        assert!(matches!(Datasets::request(".dods.nc"), (".dods.nc", DapRequest::Raw)));
    }
}

