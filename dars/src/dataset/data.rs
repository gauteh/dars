///! This module holds the collection of datasets which are available. It utilizes the `dap2`
///! module to parse queries and dispatch metadata or data requests to the `Dataset` implementation
///! on each dataset-source.
use std::collections::HashMap;

use tide::{Error, StatusCode};

use super::Dataset;
use crate::hdf5;
use crate::Request;
use dap2::Constraint;

#[derive(Default)]
pub struct Datasets {
    pub datasets: HashMap<String, DatasetType>,
}

#[derive(Debug)]
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
        let (dset, dap_request) = Datasets::request(&dset);

        if let Some(dset) = self.datasets.get(dset) {
            let constraint = Constraint::parse(req.url().query()).or_else(|_| {
                Err(Error::from_str(
                    StatusCode::BadRequest,
                    "Invalid constraints in query.",
                ))
            })?;

            debug!("dataset: {:?} [{:?}] ({:?})", dset, dap_request, constraint);

            match dset {
                DatasetType::HDF5(dset) => self.dataset_dap_request(dset, dap_request, constraint).await,
            }
        } else {
            Err(Error::from_str(StatusCode::NotFound, "Dataset not found."))
        }
    }

    async fn dataset_dap_request<T: Dataset>(
        &self,
        dset: &T,
        dap_request: DapRequest,
        constraint: Constraint
    ) -> tide::Result {
        use DapRequest::*;

        match dap_request {
            Das => Ok(dset.das().await.0.as_str().into()),
            Dds => dset.dds().await.dds(&constraint)
                .map(|dds| Ok(dds.into()))
                .or_else(|e| Err(Error::from_str(StatusCode::BadRequest, format!("Invalid DDS request: {}", e.to_string()))))?,
            Raw => dset.raw().await,
            _ => unimplemented!(),
        }
    }

    fn request(dataset: &str) -> (&str, DapRequest) {
        match dataset {
            _ if dataset.ends_with(".das") => (&dataset[..dataset.len() - 4], DapRequest::Das),
            _ if dataset.ends_with(".dds") => (&dataset[..dataset.len() - 4], DapRequest::Dds),
            _ if dataset.ends_with(".dods") => (&dataset[..dataset.len() - 5], DapRequest::Dods),
            _ => (&dataset, DapRequest::Raw),
        }
    }
}

#[derive(Debug)]
enum DapRequest {
    Das,
    Dds,
    Dods,
    Raw,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_types() {
        assert!(matches!(
            Datasets::request("test.nc.das"),
            ("test.nc", DapRequest::Das)
        ));
        assert!(matches!(
            Datasets::request("test.nc.dds"),
            ("test.nc", DapRequest::Dds)
        ));
        assert!(matches!(
            Datasets::request("test.nc.dods"),
            ("test.nc", DapRequest::Dods)
        ));
        assert!(matches!(
            Datasets::request("test.nc"),
            ("test.nc", DapRequest::Raw)
        ));
        assert!(matches!(
            Datasets::request("test.nc.asdf"),
            ("test.nc.asdf", DapRequest::Raw)
        ));
        assert!(matches!(Datasets::request(".das"), ("", DapRequest::Das)));
        assert!(matches!(Datasets::request(".dds"), ("", DapRequest::Dds)));
        assert!(matches!(Datasets::request(".dods"), ("", DapRequest::Dods)));
        assert!(matches!(Datasets::request(""), ("", DapRequest::Raw)));
        assert!(matches!(Datasets::request(".nc"), (".nc", DapRequest::Raw)));
        assert!(matches!(
            Datasets::request(".dods.nc"),
            (".dods.nc", DapRequest::Raw)
        ));
    }
}
