use super::chunk::Chunk;

use hdf5::Datatype;
use hdf5_sys::h5t::H5T_order_t;

use itertools::izip;

#[derive(Debug)]
pub struct Dataset {
    pub dtype: Datatype,
    pub order: H5T_order_t,
    pub chunks: Vec<Chunk>,
    pub shape: Vec<usize>,
    pub chunk_shape: Vec<usize>,
}

impl Dataset {
    pub fn index(ds: hdf5::Dataset) -> Result<Dataset, anyhow::Error> {
        if ds.filters().has_filters() {
            return Err(anyhow!("Filtered or compressed datasets not supported"));
        }

        let chunks: Vec<Chunk> = match (ds.is_chunked(), ds.offset()) {
            // Continuous
            (false, Some(offset)) => Ok::<_, anyhow::Error>(vec![Chunk {
                offset: vec![0; ds.ndim()],
                size: ds.storage_size(),
                addr: offset,
            }]),

            // Chunked
            (true, None) => {
                let n = ds.num_chunks().expect("weird..");

                (0..n)
                    .map(|i| {
                        ds.chunk_info(i)
                            .map(|ci| Chunk {
                                offset: ci.offset,
                                size: ci.size,
                                addr: ci.addr,
                            })
                            .ok_or_else(|| anyhow!("Could not get chunk info"))
                    })
                    .collect()
            }

            _ => Err(anyhow!("Unsupported data layout")),
        }?;

        let dtype = ds.dtype()?;
        let order = dtype.byte_order();
        let shape = ds.shape();
        let chunk_shape = ds.chunks().unwrap_or(shape.clone());

        Ok(Dataset {
            dtype,
            order,
            chunks,
            shape,
            chunk_shape,
        })
    }

    /// Returns an iterator over offset and size which if joined will make up the slice through the
    /// variable.
    pub fn chunk_slices(
        &self,
        indices: Option<&[usize]>,
        counts: Option<&[usize]>,
    ) -> impl Iterator<Item = (usize, usize)> {
        // Go through each chunk and figure out if there is a slice in it, skip if empty. if the
        // chunk is compressed or filtered the entire chunk needs to be read, and decompressed and
        // unfiltered, before being sliced.
        //
        // Note: HDF5 uses a default chunk cache of 1MB per dataset.
        //
        // Start with doing one read for each intersecting chunk, then move to joining reads.

        // | 1 | 1 | 1 |
        // | 2 | 2 | 2 |
        // | 3 | 3 | 3 |
        //
        // | 1 | 1 | 1 | 2 | 2 | 2 | 3 | 3 | 3 |
        //
        // input:  (0, 0), (1, 3)
        // output: | 1 | 1 | 1 |
        //
        // input:  (0, 0), (3, 1)
        // output: | 1 | 2 | 3 |

        let sz = self.dtype.size();

        let indices: Vec<usize> = indices
            .unwrap_or(&vec![0usize; self.shape.len()])
            .iter()
            .cloned()
            .collect();
        let counts: Vec<usize> = counts.unwrap_or(&self.shape).iter().cloned().collect();

        assert!(
            indices
                .iter()
                .zip(counts)
                .map(|(i, c)| i + c)
                .zip(&self.shape)
                .all(|(l, &s)| l < s),
            "out of bounds"
        );

        let mut slices: Vec<(&Chunk, usize, usize)> = Vec::new();

        std::iter::empty()
    }

    fn chunk_at_coord(&self, indices: &[usize]) -> &Chunk {
        self.chunks.iter().find(|c| {
            let lower = &c.offset;
            let upper = lower.iter().zip(&self.shape).map(|(l, &s)| *l + s as u64).collect::<Vec<u64>>();

            for (&i, &l, u) in izip!(indices, lower, upper) {
                if (i as u64) < l || (i as u64) >= u {
                    return false;
                }
            }

            return true;
        }).unwrap()
    }

    fn advance_chunk(chunk: &Chunk, indices: &[usize], counts: &[usize]) -> () {}
}
