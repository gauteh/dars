use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::dap2::dds::Dds;

pub struct NcmlDds {
    f: PathBuf,
    pub vars: HashMap<String, String>,
    varpos: HashMap<String, usize>,
    dim: String,
    dim_n: usize,
}

impl Dds for NcmlDds {
    fn default_vars(&self) -> Vec<String> {
        self.vars
            .iter()
            .filter(|(k, _)| !k.contains('.'))
            .map(|(k, _)| k.clone())
            .collect()
    }

    fn variable_position(&self, variable: &str) -> &usize {
        self.varpos
            .get(variable)
            .unwrap_or_else(|| panic!("variable not found: {}", variable))
    }

    fn get_file_name(&self) -> String {
        self.f.to_string_lossy().into_owned()
    }

    fn dim_len(&self, dim: &netcdf::Dimension) -> usize {
        match dim.name() {
            n if n == self.dim => self.dim_n,
            _ => dim.len(),
        }
    }
}

impl NcmlDds {
    pub fn build<P, S>(
        nc: &Arc<netcdf::File>,
        dataset: P,
        dim: S,
        dim_n: usize,
    ) -> Result<NcmlDds, anyhow::Error>
    where
        P: Into<PathBuf>,
        S: Into<String>,
    {
        let dataset = dataset.into();
        let dim = dim.into();

        let mut dds = NcmlDds {
            f: dataset,
            vars: HashMap::new(),
            varpos: HashMap::new(),
            dim,
            dim_n,
        };

        let (posmap, map) = dds.build_vars(nc);
        dds.vars = map;
        dds.varpos = posmap;
        Ok(dds)
    }
}
