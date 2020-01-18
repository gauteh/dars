use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::dap2::hyperslab::parse_hyberslab;
use crate::nc::dds::Dds;

pub struct NcmlDds {
    f: PathBuf,
    pub vars: HashMap<String, String>,
    varpos: HashMap<String, usize>,
    dim: String,
    dim_n: usize,
}

impl Dds for NcmlDds {
    fn dds(&self, nc: &netcdf::File, vars: &mut Vec<String>) -> Result<String, anyhow::Error> {
        let dds: String = {
            vars.sort_by(|a, b| {
                let a = a.find("[").map_or(&a[..], |i| &a[..i]);
                let b = b.find("[").map_or(&b[..], |i| &b[..i]);

                let a = self
                    .varpos
                    .get(a)
                    .expect(&format!("variable not found: {}", a));
                let b = self
                    .varpos
                    .get(b)
                    .expect(&format!("variable not found: {}", b));

                a.cmp(b)
            });
            vars.iter()
                .map(|v| match v.find("[") {
                    Some(i) => match parse_hyberslab(&v[i..]) {
                        Ok(slab) => self.build_var(nc, &v[..i], slab),
                        _ => None,
                    },
                    None => self
                        .vars
                        .get(v.split("[").next().unwrap_or(v))
                        .map(|s| s.to_string()),
                })
                .collect::<Option<String>>()
                .ok_or(anyhow!("variable not found"))?
        };

        Ok(format!(
            "Dataset {{\n{}}} {};",
            dds,
            self.f.to_string_lossy()
        ))
    }

    fn default_vars(&self) -> Vec<String> {
        self.vars
            .iter()
            .filter(|(k, _)| !k.contains("."))
            .map(|(k, _)| k.clone())
            .collect()
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
        nc: Arc<netcdf::File>,
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

        debug!("Building Data Descriptor Structure (DDS) for {:?}", dataset);

        let mut dds = NcmlDds {
            f: dataset,
            vars: HashMap::new(),
            varpos: HashMap::new(),
            dim: dim,
            dim_n: dim_n,
        };

        let (posmap, map) = dds.build_vars(&nc);
        dds.vars = map;
        dds.varpos = posmap;
        Ok(dds)
    }
}
