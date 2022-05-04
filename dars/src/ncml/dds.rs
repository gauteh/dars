use crate::hdf5::dds as hdf5dds;
use dap2::dds::{self, Variable};

pub struct NcmlDdsBuilder {
    file: hdf5::File,
    key: String,
    dimension: String,
    n: usize,
}

impl NcmlDdsBuilder {
    pub fn new(file: hdf5::File, key: String, dimension: String, n: usize) -> NcmlDdsBuilder {
        NcmlDdsBuilder {
            file,
            key,
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
                let mut shape = d.shape();
                if !dimensions.is_empty() && dimensions[0] == self.dimension {
                    shape[0] = self.n;
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
        self.key.clone()
    }
}
