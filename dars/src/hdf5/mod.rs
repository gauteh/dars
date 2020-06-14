use std::fmt;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::AsyncBufRead;
use hidefix::idx;

use crate::dataset::Dataset;

mod das;
mod dds;
mod dods;

/// HDF5 dataset source.
///
/// This should be serializable and not keep any files open
pub struct Hdf5Dataset {
    path: PathBuf,
    idx: idx::Index,
    das: dap2::Das,
    dds: dap2::Dds,
}

impl fmt::Debug for Hdf5Dataset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hdf5Dataset <{:?}>", self.path)
    }
}

struct HDF5File(hdf5::File, PathBuf);

impl Hdf5Dataset {
    pub fn open<P: AsRef<Path>>(path: P) -> anyhow::Result<Hdf5Dataset> {
        let path = path.as_ref();
        let hf = HDF5File(hdf5::File::open(path)?, path.to_path_buf());
        let idx = idx::Index::index_file(&hf.0, Some(path))?;
        let das = (&hf).into();
        let dds = (&hf).into();

        Ok(Hdf5Dataset {
            path: path.into(),
            idx,
            das,
            dds,
        })
    }
}

#[async_trait]
impl Dataset for Hdf5Dataset {
    async fn das(&self) -> &dap2::Das {
        &self.das
    }

    async fn dds(&self) -> &dap2::Dds {
        &self.dds
    }

    async fn raw(
        &self,
    ) -> Result<
        (
            Box<dyn Send + Sync + Unpin + AsyncBufRead + 'static>,
            Option<usize>,
        ),
        anyhow::Error,
    > {
        let file = async_std::fs::File::open(self.path.clone()).await?;
        let len = file.metadata().await?.len();

        Ok((
            Box::new(async_std::io::BufReader::new(file)),
            Some(len as usize),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coads_read() {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();
        assert!(matches!(
            hd.idx.dataset("SST").unwrap().dtype,
            idx::Datatype::Float(4)
        ));
        let mut r = hd.idx.reader("SST").unwrap();
        let v = r.values::<f32>(None, None).unwrap();
        assert_eq!(180 * 90 * 12, v.len());
    }
}
