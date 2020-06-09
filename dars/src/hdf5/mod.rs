use std::path::{Path, PathBuf};
use std::convert::TryInto;
use std::iter;

use hidefix::idx;
use dap2::das::{Attribute, ToDas};

use crate::dataset::Dataset;

pub struct Hdf5Dataset {
    path: PathBuf,
    idx: idx::Index,
    das: dap2::Das,
    // dds: dap2::Dds,
}

struct HDF5File(hdf5::File);

impl Hdf5Dataset {
    pub fn open<P: AsRef<Path>>(path: P) -> anyhow::Result<Hdf5Dataset> {
        let path = path.as_ref();
        let hf = HDF5File(hdf5::File::open(path)?);
        let idx = (&hf.0).try_into()?;
        let das = (&hf).into();

        Ok(Hdf5Dataset {
            path: path.into(),
            idx,
            das
        })
    }
}

impl Dataset for Hdf5Dataset {}


impl ToDas for &HDF5File {
    fn has_global_attributes(&self) -> bool {
        false
    }

    fn global_attributes(&self) -> Box<dyn Iterator<Item = Attribute>> {
        Box::new(iter::empty())
    }

    fn variables(&self) -> Box<dyn Iterator<Item = &str>> {
        Box::new(iter::empty())
    }

    fn variable_attributes(&self, variable: &str) -> Box<dyn Iterator<Item = Attribute>> {
        Box::new(iter::empty())
    }
}

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
