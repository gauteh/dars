use super::chunk::Chunk;

pub struct Dataset {
    chunks: Vec<Chunk>
}

impl Dataset {
    pub fn index(ds: hdf5::Dataset) -> Result<Dataset, anyhow::Error> {
        let chunks: Vec<Chunk> = match (ds.is_chunked(), ds.offset()) {
            // Continuous
            (false, Some(offset)) =>
                Ok::<_, anyhow::Error>(vec![Chunk {
                        size: ds.storage_size(),
                        offset
                }]),

            // Chunked
            (true, None) => {
                Ok(vec![])

            },

            _ => Err(anyhow!("Unsupported data layout"))
        }?;

        Ok(Dataset {
            chunks
        })
    }
}

