use byte_slice_cast::IntoByteVec;
use futures::stream::{self, Stream, StreamExt};
use std::pin::Pin;

use hidefix::idx::Index;
use hidefix::reader::stream::DatasetReader;

use crate::dap2::dods::{StreamingDataset, XdrPack};

impl StreamingDataset for Index {
    fn get_var_size(&self, var: &str) -> Result<usize, anyhow::Error> {
        self.dataset(var)
            .map(|d| d.size())
            .ok_or_else(|| anyhow!("could not find variable"))
    }

    fn get_var_single_value(&self, var: &str) -> Result<bool, anyhow::Error> {
        self.dataset(var)
            .map(|d| d.shape.len() < 1)
            .ok_or_else(|| anyhow!("could not find variable: {}", var))
    }

    /// Stream a variable with a predefined chunk size. Chunk size is not guaranteed to be
    /// kept, and may be at worst half of specified size in order to fill up slabs.
    fn stream_variable<T>(
        &self,
        _vn: &str,
        _indices: Option<&[usize]>,
        _counts: Option<&[usize]>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<T>, anyhow::Error>> + Send + Sync + 'static>>
    where
        T: netcdf::Numeric + Unpin + Clone + std::default::Default + Send + Sync + 'static,
    {
        unimplemented!()
    }

    fn stream_encoded_variable(
        &self,
        v: &str,
        indices: Option<&[usize]>,
        counts: Option<&[usize]>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, anyhow::Error>> + Send + Sync + 'static>> {
        trace!("streaming: {}", v);
        let vn = if let Some(i) = v.find(".") {
            String::from(&v[i + 1..])
        } else {
            String::from(v)
        };
        let ds = self
            .dataset(&vn)
            .expect(&format!("could not find variable: {}", vn));

        let indices: Vec<u64> = indices
            .map(|i| i.iter().map(|c| *c as u64).collect())
            .unwrap_or(vec![0; ds.shape.len()]);
        let counts: Vec<u64> = counts
            .map(|i| i.iter().map(|c| *c as u64).collect())
            .unwrap_or(ds.shape.to_vec());

        let r = DatasetReader::with_dataset(ds, self.path()).unwrap();
        if self.get_var_single_value(&vn).unwrap() {
            // Box::pin(r.stream(Some(&indices), Some(&counts)))
            trace!("single value");
            Box::pin(stream::once(async { Ok(vec![0_u8; 8]) })) // TODO!!
        } else {
            let sz = counts.iter().product::<u64>() as usize;

            Box::pin(
                stream::once(async move {
                    let mut sz = vec![sz as u32, sz as u32];
                    sz.pack();
                    Ok(sz.into_byte_vec())
                })
                .chain(r.stream(Some(&indices), Some(&counts))),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;
    use futures::executor::block_on_stream;

    #[bench]
    fn encoded_streaming_variable(b: &mut Bencher) {

        let idx = Index::index("data/coads_climatology.nc4").unwrap();

        b.iter(|| {
            let v = idx.stream_encoded_variable("SST", None, None);
            block_on_stream(v).for_each(drop)
        });
    }

    #[test]
    fn stream_encoded_variable_group_member() {
        let idx = Index::index("data/coads_climatology.nc4").unwrap();

        let counts = vec![10usize, 30, 80];

        let v = idx.stream_encoded_variable("SST.SST", Some(&[0, 0, 0]), Some(&counts));

        block_on_stream(v).for_each(drop);
    }
}
