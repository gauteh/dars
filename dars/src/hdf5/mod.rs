use std::path::{Path, PathBuf};

use hidefix::idx;

use crate::dataset::Dataset;

pub struct Hdf5Dataset {
    path: PathBuf,
    idx: idx::Index,
}

impl Hdf5Dataset {
    pub fn open<P: AsRef<Path>>(path: P) -> anyhow::Result<Hdf5Dataset> {
        let path = path.as_ref();
        let idx = idx::Index::index(path)?;

        Ok(Hdf5Dataset {
            path: path.into(),
            idx,
        })
    }
}

impl Dataset for Hdf5Dataset {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_read_coads() {
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
