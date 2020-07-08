use std::collections::HashMap;
use std::sync::Arc;

use crate::hdf5;
use colored::Colorize;
use walkdir::WalkDir;

/// The map of datasets.
#[derive(Default)]
pub struct Datasets {
    pub datasets: HashMap<String, Arc<DatasetType>>,
    pub url: Option<String>,
}

impl Datasets {
    pub fn new_with_datadir(url: Option<String>, datadir: String) -> Datasets {
        info!("Scanning {} for datasets..", datadir.yellow());

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
                        if ext == "nc4" || ext == "nc" || ext == "h5" {
                            Some(path)
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            })
            .filter_map(|path| {
                let key = path.to_string_lossy();

                let key = if datadir.ends_with("/") {
                    key[datadir.len()..].to_string()
                } else {
                    key.to_string()
                };

                let key = key.trim_start_matches('/').to_string();

                debug!(
                    "Loading {}: {}..",
                    key.yellow(),
                    path.to_string_lossy().blue()
                );

                match hdf5::Hdf5Dataset::open(path.clone()) {
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
            })
            .collect();

        info!("Loaded {} datasets.", datasets.len());

        Datasets { datasets, url }
    }
}

#[derive(Debug)]
pub enum DatasetType {
    HDF5(hdf5::Hdf5Dataset),
}
