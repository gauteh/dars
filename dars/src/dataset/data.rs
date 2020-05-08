use std::collections::HashMap;
use tide::{prelude::*, Result};

use crate::Request;

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
        Ok("".into())
    }
}
