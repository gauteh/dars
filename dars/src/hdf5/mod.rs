use std::fmt;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use futures::{pin_mut, Stream, StreamExt};

use async_stream::stream;
use byte_slice_cast::IntoByteVec;

use hidefix::idx;

use dap2::dds::DdsVariableDetails;

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

        debug!("Building DAS of {:?}..", path);
        let das = (&hf).into();

        debug!("Building DDS of {:?}..", path);
        let dds = (&hf).into();

        debug!("Indexing: {:?}..", path);

        let mut idxpath = path.to_path_buf();
        idxpath.set_extension("idx.fx");

        let idx = if idxpath.exists() {
            debug!("Loading index from {:?}..", idxpath);

            let b = std::fs::read(idxpath)?;
            flexbuffers::from_slice(&b)?
        } else {
            debug!("Writing index to {:?}", idxpath);

            let idx = idx::Index::index_file(&hf.0, Some(path))?;
            use flexbuffers::FlexbufferSerializer as ser;
            use serde::ser::Serialize;

            let mut s = ser::new();
            idx.serialize(&mut s)?;
            std::fs::write(idxpath, s.view())?;

            idx
        };

        Ok(Hdf5Dataset {
            path: path.into(),
            idx,
            das,
            dds,
        })
    }

    pub async fn raw(
        &self,
    ) -> Result<impl Stream<Item = Result<hyper::body::Bytes, std::io::Error>>, std::io::Error>
    {
        use tokio::fs::File;
        use tokio_util::codec;
        use tokio_util::codec::BytesCodec;

        File::open(self.path.clone()).await.map(|file| {
            codec::FramedRead::new(file, BytesCodec::new()).map(|r| r.map(|bytes| bytes.freeze()))
        })
    }

    pub async fn das(&self) -> &dap2::Das {
        &self.das
    }

    pub async fn dds(&self) -> &dap2::Dds {
        &self.dds
    }

    pub async fn variable(
        &self,
        variable: &DdsVariableDetails,
    ) -> Result<impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static, anyhow::Error> {
        debug!(
            "streaming: {} [{:?} / {:?}]",
            variable.name, variable.indices, variable.counts
        );

        let reader = self.idx.streamer(&variable.name)?;

        let indices: Vec<u64> = variable.indices.iter().map(|c| *c as u64).collect();
        let counts: Vec<u64> = variable.counts.iter().map(|c| *c as u64).collect();

        let length = if !variable.is_scalar() {
            let len = (variable.len() as u32).to_be();
            Some(Bytes::from(vec![len, len].into_byte_vec()))
        } else { None };

        let bytes = reader.stream(Some(indices.as_slice()), Some(counts.as_slice()));

        Ok(stream! {
            if let Some(length) = length {
                yield Ok(length);
            }

            pin_mut!(bytes);

            while let Some(b) = bytes.next().await {
                yield b.map_err(|_| std::io::ErrorKind::UnexpectedEof.into());
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dap2::constraint::Constraint;
    use dap2::dds::ConstrainedVariable;
    use futures::executor::{block_on, block_on_stream};
    use futures::pin_mut;
    use test::Bencher;

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

    #[bench]
    fn coads_stream_sst_struct(b: &mut Bencher) {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();

        let c = Constraint::parse("SST.SST").unwrap();
        let dds = hd.dds.dds(&c).unwrap();

        assert_eq!(dds.variables.len(), 1);
        if let ConstrainedVariable::Structure {
            variable: _,
            member,
        } = &dds.variables[0]
        {
            b.iter(|| {
                let reader = block_on(hd.variable(&member)).unwrap();
                pin_mut!(reader);
                block_on_stream(reader).for_each(drop);
            });
        } else {
            panic!("wrong constrained variable");
        }
    }
}
