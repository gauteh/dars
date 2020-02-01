use super::chunk::Chunk;

use hdf5::Datatype;
use hdf5_sys::h5t::H5T_order_t;

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

                (0..n).map(|i|
                    ds.chunk_info(i).map(|ci|
                        Chunk {
                            offset: ci.offset,
                            size: ci.size,
                            addr: ci.addr
                        }).ok_or_else(|| anyhow!("Could not get chunk info"))
                ).collect()
            },

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
}
