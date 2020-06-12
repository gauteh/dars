use std::path::{Path, PathBuf};

use async_trait::async_trait;
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

#[async_trait]
impl Dataset for Hdf5Dataset {
    async fn das(&self) -> &dap2::Das {
        &self.das
    }

    async fn dds(&self) -> &dap2::Dds {
        &self.dds
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

    #[test]
    fn coads_das() {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();
        println!("DAS:\n{}", hd.das);
    }

    #[test]
    fn coads_dds() {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();
        println!("DDS:\n{}", hd.dds.all());

        // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds
        //
        // filename updated
        // keys sorted by name

        let tds = r#"Dataset {
    Grid {
     ARRAY:
        Float32 AIRT[TIME = 12][COADSY = 90][COADSX = 180];
     MAPS:
        Float64 TIME[TIME = 12];
        Float64 COADSY[COADSY = 90];
        Float64 COADSX[COADSX = 180];
    } AIRT;
    Float64 COADSX[COADSX = 180];
    Float64 COADSY[COADSY = 90];
    Grid {
     ARRAY:
        Float32 SST[TIME = 12][COADSY = 90][COADSX = 180];
     MAPS:
        Float64 TIME[TIME = 12];
        Float64 COADSY[COADSY = 90];
        Float64 COADSX[COADSX = 180];
    } SST;
    Float64 TIME[TIME = 12];
    Grid {
     ARRAY:
        Float32 UWND[TIME = 12][COADSY = 90][COADSX = 180];
     MAPS:
        Float64 TIME[TIME = 12];
        Float64 COADSY[COADSY = 90];
        Float64 COADSX[COADSX = 180];
    } UWND;
    Grid {
     ARRAY:
        Float32 VWND[TIME = 12][COADSY = 90][COADSX = 180];
     MAPS:
        Float64 TIME[TIME = 12];
        Float64 COADSY[COADSY = 90];
        Float64 COADSX[COADSX = 180];
    } VWND;
} ../data/coads_climatology.nc4;"#;

        assert_eq!(hd.dds.all(), tds);
    }

    #[test]
    fn dimensions_1d() {
        let hd = Hdf5Dataset::open("tests/h5/dims_1d.h5").unwrap();
        println!("DDS:\n{}", hd.dds.all());

        let res =r#"Dataset {
    Float32 data[x1 = 2];
    Int64 x1[x1 = 2];
} tests/h5/dims_1d.h5;"#;

        assert_eq!(hd.dds.all(), res);
    }

    #[test]
    fn dimensions_2d() {
        let hd = Hdf5Dataset::open("tests/h5/dims_2d.h5").unwrap();
        println!("DDS:\n{}", hd.dds.all());

        let res = r#"Dataset {
    Grid {
     ARRAY:
        Float32 data[x1 = 2][y1 = 3];
     MAPS:
        Int64 x1[x1 = 2];
        Int64 y1[y1 = 3];
    } data;
    Int64 x1[x1 = 2];
    Int64 y1[y1 = 3];
} tests/h5/dims_2d.h5;"#;

        assert_eq!(hd.dds.all(), res);
    }
}
