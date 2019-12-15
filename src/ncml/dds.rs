use std::sync::Arc;
use std::path::PathBuf;
use std::collections::HashMap;

use crate::dap2::hyperslab::parse_hyberslab;
use crate::nc::dds::Dds;

pub struct NcmlDds {
    f: PathBuf,
    pub vars: Arc<HashMap<String, String>>,
    dim: String,
    dim_n: usize
}

impl Dds for NcmlDds {
    fn dds(&self, nc: &netcdf::File, vars: &Vec<String>) -> Result<String, anyhow::Error> {
        let dds: String = {
            vars.iter()
                .map(|v|
                    match v.find("[") {
                        Some(i) =>
                            match parse_hyberslab(&v[i..]) {
                                Ok(slab) => self.build_var(nc, &v[..i], slab),
                                _ => None
                            },
                        None =>
                            self.vars
                            .get(v.split("[").next().unwrap_or(v))
                            .map(|s| s.to_string())
                    }
                )
                .collect::<Option<String>>()
                .ok_or(anyhow!("variable not found"))?
        };

        Ok(format!("Dataset {{\n{}}} {};", dds, self.f.to_string_lossy()))
    }

    fn default_vars(&self) -> Vec<String> {
        self.vars.iter().filter(|(k,_)| !k.contains(".")).map(|(k,_)| k.clone()).collect()
    }

    fn dim_len(&self, dim: &netcdf::Dimension) -> usize {
        match dim.name() {
            n if n == self.dim => self.dim_n,
            _ => dim.len()
        }
    }
}

impl NcmlDds {
    pub fn build<P, S>(f: P, dataset: P, dim: S, dim_n: usize) -> Result<NcmlDds, anyhow::Error>
        where P: Into<PathBuf>,
              S: Into<String>
    {
        let dataset = dataset.into();
        let f = f.into();
        let dim = dim.into();

        debug!("Building Data Descriptor Structure (DDS) for {:?} based on {:?}", dataset, f);
        let nc = netcdf::open(f.clone())?;

        let mut dds = NcmlDds {
            f: dataset,
            vars: Arc::new(HashMap::new()),
            dim: dim,
            dim_n: dim_n
        };

        let map = dds.build_vars(&nc);
        dds.vars = Arc::new(map);
        Ok(dds)
    }
}


