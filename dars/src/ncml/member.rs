use std::path::{Path, PathBuf};

use async_stream::stream;
use futures::{pin_mut, Stream, StreamExt};
use bytes::Bytes;

use hidefix::{idx, reader::stream};
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

    pub async fn stream(
        &self,
        variable: &str,
        indices: &[u64],
        counts: &[u64]
    ) -> Result<impl Stream<Item = Result<Bytes, anyhow::Error>> + Send + 'static, anyhow::Error>
    {
        let modified = std::fs::metadata(&self.path)?.modified()?;
        if modified != self.modified {
            warn!("{:?} has changed on disk", self.path);
            return Err(anyhow!("{:?} has changed on disk", self.path));
        }

        debug!(
            "streaming: {} [{:?} / {:?}]",
            variable, indices, counts
        );

        let reader = match self.idx.dataset(variable) {
            Some(ds) => stream::DatasetReader::with_dataset(&ds, &self.path),
            None => Err(anyhow!("dataset does not exist")),
        }?;
        let bytes = reader.stream(Some(indices), Some(counts));

        Ok(stream! {
            pin_mut!(bytes);

            while let Some(b) = bytes.next().await {
                yield b;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_int32() {
        let m1 = NcmlMember::open("../data/ncml/jan.nc4", "time").unwrap();
        assert_eq!(m1.rank, 0.);

        let m2 = NcmlMember::open("../data/ncml/feb.nc4", "time").unwrap();
        assert_eq!(m2.rank, 31.);
    }
}
