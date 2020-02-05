use super::chunk::Chunk;

use hdf5::Datatype;
use hdf5_sys::h5t::H5T_order_t;

#[derive(Debug)]
pub struct Dataset {
    pub dtype: Datatype,
    pub order: H5T_order_t,
    pub chunks: Vec<Chunk>,
    pub shape: Vec<u64>,
    pub chunk_shape: Vec<u64>,
}

impl Dataset {
    pub fn index(ds: hdf5::Dataset) -> Result<Dataset, anyhow::Error> {
        if ds.filters().has_filters() {
            return Err(anyhow!("Filtered or compressed datasets not supported"));
        }

        let mut chunks: Vec<Chunk> = match (ds.is_chunked(), ds.offset()) {
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

        chunks.sort();

        let dtype = ds.dtype()?;
        let order = dtype.byte_order();
        let shape = ds
            .shape()
            .into_iter()
            .map(|u| u as u64)
            .collect::<Vec<u64>>();
        let chunk_shape = ds
            .chunks()
            .map(|cs| cs.into_iter().map(|u| u as u64).collect())
            .unwrap_or(shape.clone());

        Ok(Dataset {
            dtype,
            order,
            chunks,
            shape,
            chunk_shape,
        })
    }

    /// Returns an iterator over chunk, offset and size which if joined will make up the specified slice through the
    /// variable.
    pub fn chunk_slices(
        &self,
        indices: Option<&[u64]>,
        counts: Option<&[u64]>,
    ) -> impl Iterator<Item = (usize, usize)> {
        // Go through each chunk and figure out if there is a slice in it, skip if empty. if the
        // chunk is compressed or filtered the entire chunk needs to be read, and decompressed and
        // unfiltered, before being sliced.
        //
        // Note: HDF5 uses a default chunk cache of 1MB per dataset.

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

        let indices: Vec<u64> = indices
            .unwrap_or(&vec![0; self.shape.len()])
            .iter()
            .cloned()
            .collect();
        let counts: Vec<u64> = counts.unwrap_or(&self.shape).iter().cloned().collect();

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

    /// Find chunk containing coordinate.
    fn chunk_at_coord(&self, indices: &[u64]) -> Result<&Chunk, anyhow::Error> {
        self.chunks
            .binary_search_by(|c| c.contains(indices, self.chunk_shape.as_slice()).reverse())
            .map(|i| &self.chunks[i])
            .map_err(|_| anyhow!("could not find chunk"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_at_coord() {
        let d = Dataset {
            dtype: Datatype::from_type::<f32>().unwrap(),
            order: H5T_order_t::H5T_ORDER_LE,
            shape: vec![100, 100],
            chunk_shape: vec![10, 10],
            chunks: vec![
                Chunk {
                    offset: vec![0, 0],
                    size: 400,
                    addr: 0,
                },
                Chunk {
                    offset: vec![0, 10],
                    size: 400,
                    addr: 400,
                },
                Chunk {
                    offset: vec![10, 0],
                    size: 400,
                    addr: 800,
                },
                Chunk {
                    offset: vec![10, 10],
                    size: 400,
                    addr: 1200,
                },
            ],
        };

        assert_eq!(d.chunk_at_coord(&[0, 0]).unwrap().offset, [0, 0]);
        assert_eq!(d.chunk_at_coord(&[0, 5]).unwrap().offset, [0, 0]);
        assert_eq!(d.chunk_at_coord(&[5, 5]).unwrap().offset, [0, 0]);
        assert_eq!(d.chunk_at_coord(&[0, 10]).unwrap().offset, [0, 10]);
        assert_eq!(d.chunk_at_coord(&[0, 15]).unwrap().offset, [0, 10]);
        assert_eq!(d.chunk_at_coord(&[10, 0]).unwrap().offset, [10, 0]);
        assert_eq!(d.chunk_at_coord(&[10, 1]).unwrap().offset, [10, 0]);
        assert_eq!(d.chunk_at_coord(&[15, 1]).unwrap().offset, [10, 0]);
    }
}
