use std::collections::HashMap;
use tide::Result;

use crate::Request;

///! This module holds the collection of datasets which are available. It utilizes the `dap2`
///! module to parse queries and dispatch metadata or data requests to the `Dataset` implementation
///! on each dataset-source.

#[derive(Default)]
pub struct Datasets {
    pub datasets: HashMap<String, DatasetType>,
}

pub enum DatasetType {
    HDF5,
}

impl Datasets {
    pub async fn datasets(&self) -> Result {
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

    pub async fn dataset(&self, req: &Request) -> Result {
        let ds: String = req.param("dataset")?;
        let query = req.url().query().unwrap();
        info!("dataset: {}, {}", ds, query);
        Ok("".into())
    }
}
