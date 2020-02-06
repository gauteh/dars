use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::dap2::hyperslab::{count_slab, parse_hyberslab};

pub struct NcDds {
    f: PathBuf,
    pub vars: HashMap<String, String>,
    pub varpos: HashMap<String, usize>,
}

pub trait Dds {
    fn dim_len(&self, dim: &netcdf::Dimension) -> usize;

    fn vartype_str(&self, t: netcdf_sys::nc_type) -> String {
        match t {
            netcdf_sys::NC_FLOAT => "Float32".to_string(),
            netcdf_sys::NC_DOUBLE => "Float64".to_string(),
            netcdf_sys::NC_SHORT => "Int16".to_string(),
            netcdf_sys::NC_INT => "Int32".to_string(),
            netcdf_sys::NC_BYTE => "Byte".to_string(),
            netcdf_sys::NC_UBYTE => "Byte".to_string(),
            // netcdf_sys::NC_CHAR => "String".to_string(),
            netcdf_sys::NC_STRING => "String".to_string(),
            e => format!("Unimplemented: {:?}", e),
        }
    }

    fn format_var(
        &self,
        indent: usize,
        var: &netcdf::Variable,
        slab: &Option<Vec<usize>>,
    ) -> String {
        if !var.dimensions().is_empty() {
            format!(
                "{}{} {}[{} = {}];",
                " ".repeat(indent),
                self.vartype_str(var.vartype()),
                var.name(),
                var.dimensions()[0].name(),
                slab.as_ref()
                    .and_then(|s| s.get(0))
                    .unwrap_or(&self.dim_len(&var.dimensions()[0]))
            )
        } else {
            format!(
                "{}{} {};",
                " ".repeat(indent),
                self.vartype_str(var.vartype()),
                var.name()
            )
        }
    }

    fn format_grid(
        &self,
        indent: usize,
        nc: &netcdf::File,
        var: &netcdf::Variable,
        slab: &Option<Vec<usize>>,
    ) -> String {
        if !var
            .dimensions()
            .iter()
            .all(|d| nc.variable(&d.name()).is_some())
        {
            return format!(
                "{}{} {}{};\n",
                " ".repeat(indent),
                self.vartype_str(var.vartype()),
                var.name(),
                var.dimensions()
                    .iter()
                    .enumerate()
                    .map(|(i, d)| format!(
                        "[{} = {}]",
                        d.name(),
                        slab.as_ref()
                            .and_then(|s| s.get(i))
                            .unwrap_or(&self.dim_len(&d))
                    ))
                    .collect::<String>()
            );
        }

        let mut grid: Vec<String> = Vec::new();

        grid.push(format!("{}Grid {{", " ".repeat(indent)));
        grid.push(format!("{} ARRAY:", " ".repeat(indent)));
        grid.push(format!(
            "{}{} {}{};",
            " ".repeat(2 * indent),
            self.vartype_str(var.vartype()),
            var.name(),
            var.dimensions()
                .iter()
                .enumerate()
                .map(|(i, d)| format!(
                    "[{} = {}]",
                    d.name(),
                    slab.as_ref()
                        .and_then(|s| s.get(i))
                        .unwrap_or(&self.dim_len(&d))
                ))
                .collect::<String>()
        ));
        grid.push(format!("{} MAPS:", " ".repeat(indent)));
        for d in var.dimensions() {
            let dvar = nc
                .variable(&d.name())
                .unwrap_or_else(|| panic!("No variable found for dimension: {}", d.name()));
            grid.push(self.format_var(2 * indent, &dvar, slab));
        }

        grid.push(format!("{}}} {};\n", " ".repeat(indent), var.name()));
        grid.join("\n")
    }

    fn format_struct(
        &self,
        indent: usize,
        _nc: &netcdf::File,
        var: &netcdf::Variable,
        dim: &netcdf::Variable,
        slab: &Option<Vec<usize>>,
    ) -> String {
        let mut grid: Vec<String> = Vec::new();

        grid.push(format!("{}Structure {{", " ".repeat(indent)));

        grid.push(format!(
            "{}{} {}{};",
            " ".repeat(2 * indent),
            self.vartype_str(dim.vartype()),
            dim.name(),
            dim.dimensions()
                .iter()
                .enumerate()
                .map(|(i, d)| format!(
                    "[{} = {}]",
                    d.name(),
                    slab.as_ref()
                        .and_then(|s| s.get(i))
                        .unwrap_or(&self.dim_len(&d))
                ))
                .collect::<String>()
        ));

        grid.push(format!("{}}} {};\n", " ".repeat(indent), var.name()));

        grid.join("\n")
    }

    fn build_vars(&self, nc: &netcdf::File) -> (HashMap<String, usize>, HashMap<String, String>) {
        let indent: usize = 4;

        let mut map = HashMap::new();
        let mut posmap = HashMap::new();

        // TODO: some types not yet supported.
        for (z, var) in nc.variables().enumerate().filter(|(_, v)| {
            v.vartype() != netcdf_sys::NC_CHAR && v.vartype() != netcdf_sys::NC_BYTE
        }) {
            if var.dimensions().len() < 2 {
                let mut v = self.format_var(indent, &var, &None);
                v.push_str("\n");
                map.insert(var.name().to_string(), v);
                posmap.insert(var.name().to_string(), z);
            } else {
                map.insert(
                    var.name().to_string(),
                    self.format_grid(indent, &nc, &var, &None),
                );
                posmap.insert(var.name().to_string(), z);

                map.insert(
                    format!("{}.{}", var.name(), var.name()),
                    self.format_struct(indent, &nc, &var, &var, &None),
                );
                posmap.insert(format!("{}.{}", var.name(), var.name()), z);

                for d in var.dimensions() {
                    match nc.variable(&d.name()) {
                        Some(dvar) => {
                            posmap.insert(format!("{}.{}", var.name(), d.name()), z);
                            map.insert(
                                format!("{}.{}", var.name(), d.name()),
                                self.format_struct(indent, &nc, &var, &dvar, &None),
                            )
                        }
                        _ => None,
                    };
                }
            }
        }

        (posmap, map)
    }

    fn build_var(&self, nc: &netcdf::File, var: &str, count: Vec<usize>) -> Option<String> {
        let indent: usize = 4;

        match var.find('.') {
            Some(i) => match nc.variable(&var[..i]) {
                Some(ivar) => match nc.variable(&var[i + 1..]) {
                    Some(dvar) => Some(self.format_struct(indent, &nc, &ivar, &dvar, &Some(count))),
                    _ => None,
                },
                _ => None,
            },

            None => match nc.variable(var) {
                Some(var) => match var.dimensions().len() {
                    l if l < 2 => Some(self.format_var(indent, &var, &Some(count))),
                    _ => Some(self.format_grid(indent, &nc, &var, &Some(count))),
                },
                _ => None,
            },
        }
    }

    fn dds(
        &self,
        nc: &netcdf::File,
        vars: &Vec<(String, Option<Vec<usize>>, Option<Vec<usize>>)>,
    ) -> Result<String, anyhow::Error>;
    fn default_vars(&self) -> Vec<String>;
}

impl Dds for NcDds {
    /// vars must be sorted
    fn dds(
        &self,
        nc: &netcdf::File,
        vars: &Vec<(String, Option<Vec<usize>>, Option<Vec<usize>>)>,
    ) -> Result<String, anyhow::Error> {
        let dds: String = vars
            .iter()
            .map(|v| match v.2 {
                Some(counts) => self.build_var(nc, v.0, counts),
                None => v.0,
            })
            .collect::<String>();

        Ok(format!(
            "Dataset {{\n{}}} {};",
            dds,
            self.f.to_string_lossy()
        ))
    }

    fn default_vars(&self) -> Vec<String> {
        self.vars
            .iter()
            .filter(|(k, _)| !k.contains('.'))
            .map(|(k, _)| k.clone())
            .collect()
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
