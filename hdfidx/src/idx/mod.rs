use std::collections::HashMap;
use std::path::{Path, PathBuf};

use hdf5::File;

mod chunk;
mod dataset;

pub use chunk::Chunk;
pub use dataset::Dataset;

#[derive(Debug)]
pub struct Index {
    path: PathBuf,
    datasets: HashMap<String, Dataset>,
}

impl Index {
    /// Open an existing HDF5 file and index all variables.
    pub fn index<P>(path: P) -> Result<Index, anyhow::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();

        let hf = File::open(path)?;

        let datasets = hf
            .group("/")?
            .member_names()?
            .iter()
            .map(|m| hf.dataset(m).map(|d| (m, d)))
            .filter_map(Result::ok)
            .map(|(m, d)| Dataset::index(d).map(|d| (m.clone(), d)))
            .collect::<Result<HashMap<String, Dataset>, _>>()?;

        Ok(Index {
            path: path.into(),
            datasets,
        })
    }

    pub fn dataset(&self, s: &str) -> Option<&Dataset> {
        self.datasets.get(s)
    }

    pub fn path(&self) -> &Path {
        self.path.as_ref()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn index_t_float32() {
        let i = Index::index("test/data/t_float.h5").unwrap();

        println!("index: {:#?}", i);
    }

    #[test]
    fn chunked_1d() {
        let i = Index::index("test/data/chunked_oneD.h5").unwrap();

        println!("index: {:#?}", i);
    }

    #[test]
    fn chunked_2d() {
        let i = Index::index("test/data/chunked_twoD.h5").unwrap();

        println!("index: {:#?}", i);
    }
}