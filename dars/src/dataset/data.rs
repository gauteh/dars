///! This module holds the collection of datasets which are available. It utilizes the `dap2`
///! module to parse queries and dispatch metadata or data requests to the `Dataset` implementation
///! on each dataset-source.

use std::collections::HashMap;

use crate::Request;
use crate::hdf5;

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
        let ds: String = req.param("dataset")?;
        let query = req.url().query().unwrap();
        info!("dataset: {}, {}", ds, query);
        Ok("".into())
    }

    fn request(dataset: &str) -> (&str, DapRequest) {
        match dataset {
            _ if dataset.ends_with(".das") => (&dataset[..dataset.len()-4], DapRequest::DAS),
            _ if dataset.ends_with(".dds") => (&dataset[..dataset.len()-4], DapRequest::DDS),
            _ if dataset.ends_with(".dods") => (&dataset[..dataset.len()-5], DapRequest::DODS),
            _ => (&dataset, DapRequest::RAW),
        }
    }
}

#[derive(Debug)]
enum DapRequest {
    DAS,
    DDS,
    DODS,
    RAW
}

