use std::borrow::Borrow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use colored::Colorize;
use futures::Stream;
use std::pin::Pin;
use walkdir::WalkDir;

use crate::{hdf5, ncml};
use dap2::das::Das;
use dap2::dds::{self, Dds};
use dap2::Dap2;

/// The map of datasets.
pub struct Datasets {
    pub datasets: HashMap<String, Arc<DatasetType>>,
    pub url: Option<String>,
    pub db: sled::Db,
}

#[cfg(feature = "catalog")]
impl dars_catalog::Catalog for Datasets {
    fn paths<'a>(&'a self) -> Box<dyn Iterator<Item = &str> + 'a> {
        Box::new(self.datasets.keys().map(|s| s.as_str()))
    }
}

impl Datasets {
    pub fn get<Q>(&self, key: &Q) -> Option<&Arc<DatasetType>>
    where
        String: Borrow<Q>,
        Q: std::hash::Hash + std::cmp::Eq,
    {
        self.datasets.get(key)
    }

    /// Temporary State for tests.
    #[cfg(test)]
    pub fn temporary() -> Datasets {
        Datasets {
            datasets: HashMap::default(),
            url: None,
            db: super::test_db(),
        }
    }

    pub async fn new_with_datadir(
        url: Option<String>,
        datadir: PathBuf,
    ) -> anyhow::Result<Datasets> {
        info!("Opening sled db: {}..", "dars.db".yellow());
        let db = sled::open("dars.db")?;

        info!(
            "Scanning {} for datasets..",
            datadir.to_string_lossy().yellow()
        );

        let datasets: HashMap<_, _> = WalkDir::new(&datadir)
            .into_iter()
            .filter_entry(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|s| entry.depth() == 0 || !s.starts_with("."))
                    .unwrap_or(false)
            })
            .filter_map(|e| e.ok())
            .filter_map(|entry| {
                let path = entry.into_path();

                match path.extension() {
                    Some(ext) => {
                        if ext == "nc4" || ext == "nc" || ext == "h5" || ext == "ncml" {
                            Some(path)
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            })
            .filter_map(|path| {
                let key = if datadir.to_string_lossy().ends_with("/") {
                    path.strip_prefix(&datadir).ok()?.to_string_lossy()
                } else {
                    path.to_string_lossy()
                };

                let key = key.trim_start_matches('/').to_string();

                debug!(
                    "Loading {}: {}..",
                    key.yellow(),
                    path.to_string_lossy().blue()
                );

                if path.extension().expect("already filtered on extension") == "ncml" {
                    match ncml::NcmlDataset::open(path.clone(), key.clone(), db.clone()) {
                        Ok(d) => Some((key, Arc::new(DatasetType::NCML(d)))),
                        Err(e) => {
                            warn!(
                                "Could not load: {}, error: {}",
                                path.to_string_lossy().blue(),
                                e.to_string().red()
                            );
                            None
                        }
                    }
                } else {
                    match hdf5::Hdf5Dataset::open(path.clone(), key.clone(), &db) {
                        Ok(d) => Some((key, Arc::new(DatasetType::HDF5(d)))),
                        Err(e) => {
                            warn!(
                                "Could not load: {}, error: {}",
                                path.to_string_lossy().blue(),
                                e.to_string().red()
                            );
                            None
                        }
                    }
                }
            })
            .collect();

        info!("Loaded {} datasets.", datasets.len());

        Ok(Datasets { datasets, url, db })
    }
}

#[derive(Debug)]
pub enum DatasetType {
    HDF5(hdf5::Hdf5Dataset),
    NCML(ncml::NcmlDataset),
}

#[async_trait]
impl Dap2 for DatasetType {
    async fn das(&self) -> &Das {
        use DatasetType::*;

        match self {
            HDF5(ds) => ds.das().await,
            NCML(ds) => ds.das().await,
        }
    }

    async fn dds(&self) -> &Dds {
        use DatasetType::*;

        match self {
            HDF5(ds) => ds.dds().await,
            NCML(ds) => ds.dds().await,
        }
    }

    async fn variable(
        &self,
        variable: &dds::DdsVariableDetails,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Send + 'static>>,
        anyhow::Error,
    > {
        use DatasetType::*;

        match self {
            HDF5(ds) => ds.variable(variable).await,
            NCML(ds) => ds.variable(variable).await,
        }
    }

    async fn raw(
        &self,
    ) -> Result<
        (
            u64,
            Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static>>,
        ),
        std::io::Error,
    > {
        use DatasetType::*;

        match self {
            HDF5(ds) => ds.raw().await,
            NCML(ds) => ds.raw().await,
        }
    }
}
