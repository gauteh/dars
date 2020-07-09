use crate::hdf5::dds as hdf5dds;
use dap2::dds::{self, Variable};
use std::path::PathBuf;

pub struct NcmlDdsBuilder {
    file: hdf5::File,
    path: PathBuf,
    dimension: String,
    n: usize,
}

impl NcmlDdsBuilder {
    pub fn new(file: hdf5::File, path: PathBuf, dimension: String, n: usize) -> NcmlDdsBuilder {
        NcmlDdsBuilder {
            file,
            path,
            dimension,
            n,
        }
    }
}

impl dds::ToDds for NcmlDdsBuilder {
    fn variables(&self) -> Vec<Variable> {
        self.file
            .group("/")
            .unwrap()
            .member_names()
            .unwrap()
            .iter()
            .map(|m| self.file.dataset(m).map(|d| (m, d)))
            .filter_map(Result::ok)
            .filter(|(_, d)| d.is_chunked() || d.offset().is_some()) // skipping un-allocated datasets.
            .map(|(m, d)| {
                trace!(
                    "Variable: {} {:?}",
                    m,
                    hdf5dds::hdf5_vartype(&d.dtype().unwrap())
                );
                let dimensions = hdf5dds::hdf5_dimensions(m, &d);
                let mut shape = d.shape().clone();
                if !dimensions.is_empty() {
                    if dimensions[0] == self.dimension {
                        shape[0] = self.n;
                    }
                }
                Variable::new(
                    m.clone(),
                    hdf5dds::hdf5_vartype(&d.dtype().unwrap()),
                    dimensions,
                    shape,
                )
            })
            .collect()
    }

    fn file_name(&self) -> String {
        self.path.to_string_lossy().to_string()
    }
}
