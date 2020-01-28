use hdf5::File;
use super::dataset::Dataset;

pub struct Index {
    path: std::path::PathBuf,
    datasets: Vec<Dataset>
}

impl Index {
    /// Open an existing HDF5 file and index all variables.
    pub fn index<P>(path: P) -> Result<Index, anyhow::Error>
        where P: AsRef<std::path::Path>
    {
        let path = path.as_ref();

        let hf = File::open(path)?;

        let datasets = hf.group("/")?
          .member_names()?
          .iter()
          .map(|m| hf.dataset(m))
          .filter_map(Result::ok)
          .map(Dataset::index)
          .collect::<Result<Vec<Dataset>, _>>()?;

        Ok(Index {
            path: path.into(),
            datasets
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn open_t_float32() {
        let i = Index::index("test/data/t_float.h5");
    }
}

