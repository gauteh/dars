use hidefix::{idx, reader::stream};
use std::path::{Path, PathBuf};

use crate::hdf5::HDF5File;

/// One member of the NCML dataset.
pub struct NcmlMember {
    pub path: PathBuf,
    pub idx: idx::Index,
    pub modified: std::time::SystemTime,
    pub n: usize,
    pub rank: f64,
}

impl NcmlMember {
    pub fn open<P>(path: P, dimension: &str) -> anyhow::Result<NcmlMember>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        debug!("Opening member: {:?}", path);

        let modified = std::fs::metadata(path)?.modified()?;

        let hf = HDF5File(hdf5::File::open(path)?, path.to_path_buf());

        // Read size of aggregate dimension
        let agg = hf.0.dataset(dimension)?;
        let n = agg.size();

        // Read first value of aggregate dimension
        let rank: f64 = *agg
            .read_slice_1d::<f64, _>(ndarray::s![0..1])?
            .get(0)
            .ok_or_else(|| anyhow!("aggregate dimension is empty"))?;

        let mut idxpath = path.to_path_buf();
        idxpath.set_extension("idx.fx");

        let idx = if idxpath.exists() {
            trace!("Loading index from {:?}..", idxpath);

            let b = std::fs::read(idxpath)?;
            flexbuffers::from_slice(&b)?
        } else {
            debug!("Indexing: {:?}..", path);
            let idx = idx::Index::index_file(&hf.0, Some(path))?;
            use flexbuffers::FlexbufferSerializer as ser;
            use serde::ser::Serialize;

            trace!("Writing index to {:?}", idxpath);
            let mut s = ser::new();
            idx.serialize(&mut s)?;
            std::fs::write(idxpath, s.view())?;

            idx
        };

        Ok(NcmlMember {
            path: path.into(),
            idx,
            modified,
            n,
            rank,
        })
    }
}
