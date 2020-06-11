use std::path::{Path, PathBuf};

use crate::dataset::Dataset;
use hidefix::idx;

mod das;
mod dds;

/// HDF5 dataset source.
///
/// This should be serializable and not keep any files open
pub struct Hdf5Dataset {
    path: PathBuf,
    idx: idx::Index,
    das: dap2::Das,
    dds: dap2::Dds,
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

    #[test]
    fn coads_das() {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();
        println!("DAS:\n{}", hd.das);
    }

    #[test]
    fn coads_dds() {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();
        println!("DDS:\n{}", hd.dds.all());
    }

    #[test]
    fn dimensions_1d() {
        let hd = Hdf5Dataset::open("tests/h5/dims_1d.h5").unwrap();
    }

    #[test]
    fn dimensions_2d() {
        let hd = Hdf5Dataset::open("tests/h5/dims_2d.h5").unwrap();
    }
}
