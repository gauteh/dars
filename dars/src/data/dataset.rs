use std::collections::HashMap;
use std::sync::Arc;

use crate::hdf5;

/// The map of datasets.
#[derive(Default)]
pub struct Datasets {
    pub datasets: HashMap<String, Arc<DatasetType>>,
}

#[derive(Debug)]
pub enum DatasetType {
    HDF5(hdf5::Hdf5Dataset),
}
