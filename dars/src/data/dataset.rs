use async_trait::async_trait;
use futures::AsyncBufRead;
use std::collections::HashMap;
use std::sync::Arc;

use dap2::das::Das;
use dap2::dds::Dds;
use dap2::dods::Dods;

use crate::hdf5;

/// A dataset provides endpoints for the metadata or data requests over the DAP2 or DAP4 protocol.
///
/// Provide stream of data and access to metadata.
#[async_trait]
pub trait Dataset: Dods {
    async fn das(&self) -> &Das;
    async fn dds(&self) -> &Dds;

    /// Returns a async reader of the raw file as well as the length of the file (if available).
    async fn raw(
        &self,
    ) -> Result<
        (
            Box<dyn Send + Sync + Unpin + AsyncBufRead + 'static>,
            Option<usize>,
        ),
        anyhow::Error,
    >;
}

/// The map of datasets.
#[derive(Default)]
pub struct Datasets {
    pub datasets: HashMap<String, Arc<DatasetType>>,
}

#[derive(Debug)]
pub enum DatasetType {
    HDF5(hdf5::Hdf5Dataset),
}
