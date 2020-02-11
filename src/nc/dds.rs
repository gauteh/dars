use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::dap2::dds::Dds;

pub struct NcDds {
    f: PathBuf,
    pub vars: HashMap<String, String>,
    varpos: HashMap<String, usize>,
}

impl Dds for NcDds {
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

    fn get_var(&self, var: &str) -> Option<String> {
        self.vars.get(var).map(|s| s.clone())
    }

    fn dim_len(&self, dim: &netcdf::Dimension) -> usize {
        dim.len()
    }
}

impl NcDds {
    pub fn build<P>(f: P, nc: &Arc<netcdf::File>) -> anyhow::Result<NcDds>
    where
        P: Into<PathBuf>,
    {
        let f = f.into();

        let mut dds = NcDds {
            f,
            vars: HashMap::new(),
            varpos: HashMap::new(),
        };
        let (posmap, map) = dds.build_vars(&nc);
        dds.vars = map;
        dds.varpos = posmap;
        Ok(dds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_dds() {
        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        let dds = NcDds::build("data/coads_climatology.nc", &f).unwrap();

        assert_eq!(
            dds.dds(&f, &dds.parse_query(Some("TIME")).unwrap())
                .unwrap(),
            r#"Dataset {
    Float64 TIME[TIME = 12];
} data/coads_climatology.nc;"#
        );
    }

    #[test]
    fn time_hyperslab() {
        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        let dds = NcDds::build("data/coads_climatology.nc", &f).unwrap();

        assert_eq!(
            dds.dds(&f, &dds.parse_query(Some("TIME[0:2]")).unwrap())
                .unwrap(),
            r#"Dataset {
    Float64 TIME[TIME = 3];
} data/coads_climatology.nc;"#
        );
    }
}
