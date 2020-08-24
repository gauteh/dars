use std::path::{Path, PathBuf};

use async_stream::stream;
use bytes::Bytes;
use futures::{pin_mut, Stream, StreamExt};

use crate::hdf5::HDF5File;
use hidefix::idx;

/// One member of the NCML dataset.
pub struct NcmlMember {
    pub path: PathBuf,
    pub key: String,
    pub modified: std::time::SystemTime,
    pub n: usize,
    pub rank: f64,
}

impl NcmlMember {
    pub fn open<P>(path: P, dimension: &str, db: &sled::Db) -> anyhow::Result<NcmlMember>
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

        let key = path.to_string_lossy().to_string();
        if !db.contains_key(&key)? {
            debug!("Indexing: {:?}..", path);
            let idx = idx::Index::index_file(&hf.0, Some(path))?;
            let bts = bincode::serialize(&idx)?;

            trace!("Inserting index into db ({})", key);
            db.insert(&key, bts)?;
        } else {
            trace!("{} already indexed.", key);
        };


        Ok(NcmlMember {
            path: path.into(),
            key,
            modified,
            n,
            rank,
        })
    }

    pub async fn stream(
        &self,
        variable: &str,
        db: sled::Db,
        indices: &[u64],
        counts: &[u64],
    ) -> Result<impl Stream<Item = Result<Bytes, anyhow::Error>> + Send + 'static, anyhow::Error>
    {
        let modified = std::fs::metadata(&self.path)?.modified()?;
        if modified != self.modified {
            warn!("{:?} has changed on disk", self.path);
            return Err(anyhow!("{:?} has changed on disk", self.path));
        }

        debug!("streaming: {} [{:?} / {:?}]", variable, indices, counts);

        trace!("fetching index from db: {}", self.key);
        let bts = db.get(&self.key)?.unwrap();
        let idx = bincode::deserialize::<idx::Index>(&bts)?;
        trace!("creating streamer: {}", variable);

        let reader = match idx.dataset(variable) {
            Some(ds) => ds.as_streamer(&self.path),
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
    use crate::data::test_db;

    #[test]
    fn rank_int32() {
        let db = test_db();
        let m1 = NcmlMember::open("../data/ncml/jan.nc4", "time", &db).unwrap();
        assert_eq!(m1.rank, 0.);

        let m2 = NcmlMember::open("../data/ncml/feb.nc4", "time", &db).unwrap();
        assert_eq!(m2.rank, 31.);
    }

    #[test]
    fn db_key_indexed() {
        let db = test_db();
        let m1 = NcmlMember::open("../data/ncml/jan.nc4", "time", &db).unwrap();
        let m2 = NcmlMember::open("../data/ncml/feb.nc4", "time", &db).unwrap();

        assert!(db.contains_key(&m1.key).unwrap());
        assert!(db.contains_key(&m2.key).unwrap());
    }
}
