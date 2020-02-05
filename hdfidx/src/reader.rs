use std::cell::RefCell;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use byte_slice_cast::{FromByteVec, IntoVecOf};

use super::idx::Dataset;

pub struct DatasetReader<'a> {
    ds: &'a Dataset,
    fd: RefCell<File>,
}

impl<'a> DatasetReader<'a> {
    pub fn with_dataset<P>(ds: &'a Dataset, p: P) -> Result<DatasetReader, anyhow::Error>
    where
        P: AsRef<Path>,
    {
        let fd = RefCell::new(File::open(p)?);

        Ok(DatasetReader { ds, fd })
    }

    pub fn read(
        &self,
        indices: Option<&[u64]>,
        counts: Option<&[u64]>,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let sz = self.ds.dtype.size() as u64;

        let indices: Vec<u64> = indices
            .unwrap_or(&vec![0; self.ds.shape.len()])
            .iter()
            .map(|v| v * sz)
            .collect();
        let counts: Vec<u64> = counts
            .unwrap_or(&self.ds.shape)
            .iter()
            .map(|v| v * sz)
            .collect();

        let addr: u64 = self.ds.chunks[0].addr + indices.iter().product::<u64>();
        let sz: u64 = counts.iter().product();

        let mut fd = self.fd.borrow_mut();
        fd.seek(SeekFrom::Start(addr as u64))?;

        let mut buf = vec![0_u8; sz as usize];
        fd.read_exact(buf.as_mut_slice())?;

        Ok(buf)
    }

    pub fn values<T>(
        &self,
        indices: Option<&[u64]>,
        counts: Option<&[u64]>,
    ) -> Result<Vec<T>, anyhow::Error>
    where
        T: FromByteVec,
    {
        // TODO: BE, LE conversion
        // TODO: use as_slice_of() to avoid copy, or possible values_to(&mut buf) so that
        //       caller keeps ownership of slice too.
        Ok(self.read(indices, counts)?.into_vec_of::<T>()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::idx::Index;

    #[test]
    fn read_t_float32() {
        let i = Index::index("test/data/t_float.h5").unwrap();
        let r = DatasetReader::with_dataset(i.dataset("d32_1").unwrap(), i.path()).unwrap();

        let vs = r.values::<f32>(None, None).unwrap();

        let h = hdf5::File::open(i.path()).unwrap();
        let hvs = h.dataset("d32_1").unwrap().read_raw::<f32>().unwrap();

        assert_eq!(vs, hvs);
    }

    #[test]
    fn read_chunked_1d() {
        let i = Index::index("test/data/chunked_oneD.h5").unwrap();
        let r = DatasetReader::with_dataset(i.dataset("d_4_chunks").unwrap(), i.path()).unwrap();

        let vs = r.values::<f32>(None, None).unwrap();

        let h = hdf5::File::open(i.path()).unwrap();
        let hvs = h.dataset("d_4_chunks").unwrap().read_raw::<f32>().unwrap();

        assert_eq!(vs, hvs);
    }

    #[test]
    fn read_chunked_2d() {
        let i = Index::index("test/data/chunked_twoD.h5").unwrap();
        let r = DatasetReader::with_dataset(i.dataset("d_4_chunks").unwrap(), i.path()).unwrap();

        let vs = r.values::<f32>(None, None).unwrap();

        let h = hdf5::File::open(i.path()).unwrap();
        let hvs = h.dataset("d_4_chunks").unwrap().read_raw::<f32>().unwrap();

        assert_eq!(vs, hvs);
    }
}