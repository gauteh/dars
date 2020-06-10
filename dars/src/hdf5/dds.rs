///! HDF5 files have dimensions defined through various special attributes, linking them using ID's
///! reference lists.

use dap2::dds::{self, Variable};

use super::HDF5File;

impl dds::ToDds for &HDF5File {
    fn variables(&self) -> Vec<Variable> {
        self.0
            .group("/")
            .unwrap()
            .member_names()
            .unwrap()
            .iter()
            .map(|m| self.0.dataset(m).map(|d| (m, d)))
            .filter_map(Result::ok)
            .filter(|(_, d)| d.is_chunked() || d.offset().is_some()) // skipping un-allocated datasets.
            .map(|(m, d)| {


                Variable {
                    name: m.clone(),
                    vartype: todo!(),
                    dimensions: todo!()
                }
            })
            .collect()
    }

    fn dimension_length(&self, dim: &str) -> usize {
        todo!()
    }

    fn file_name(&self) -> String {
        todo!()
    }
}

