/// A HDF5 chunk. A chunk is read and written in its entirety by the HDF5 library. This is
/// usually necessary since the chunk can be compressed and filtered.
///
/// > Note: The official HDF5 library uses a 1MB dataset cache by default.
///
/// [HDF5 chunking](https://support.hdfgroup.org/HDF5/doc/Advanced/Chunking/index.html).
#[derive(Debug)]
pub struct Chunk {
    pub offset: Vec<u64>,
    pub size: u64,
    pub addr: u64,
}
