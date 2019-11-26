use std::sync::Arc;
use std::collections::HashMap;
use netcdf;
use netcdf_sys;
use anyhow;

use super::*;

pub struct NcDds {
    f: String,
    vars: Arc<HashMap<String, String>>
}

impl NcDds {
    fn vartype_str(t: netcdf_sys::nc_type) -> String {
        match t {
            netcdf_sys::NC_FLOAT => "Float32".to_string(),
            netcdf_sys::NC_DOUBLE => "Float64".to_string(),
            netcdf_sys::NC_STRING => "String".to_string(),
            _ => "Unimplemented".to_string()
        }
    }

    fn format_var(indent: usize, var: &netcdf::Variable) -> String {
        if var.dimensions().len() >= 1 {
            format!("{}{} {}[{} = {}]",
                    " ".repeat(indent),
                    NcDds::vartype_str(var.vartype()),
                    var.name(),
                    var.dimensions()[0].name(),
                    var.dimensions()[0].len())
        } else {
            format!("{}{} {};", " ".repeat(indent), NcDds::vartype_str(var.vartype()), var.name())
        }
    }

    fn format_grid(indent: usize, nc: &netcdf::File, var: &netcdf::Variable) -> String {
        if !var.dimensions().iter().all(|d| nc.variable(d.name()).is_some()) {
            return format!("{}{} {}{};", " ".repeat(indent),
            NcDds::vartype_str(var.vartype()),
            var.name(),
            var.dimensions().iter().map(|d|
                format!("[{} = {}]", d.name(), d.len())).collect::<String>());
        }

        let mut grid: Vec<String> = Vec::new();

        grid.push(format!("{}Grid {{", " ".repeat(indent)));
        grid.push(format!("{} ARRAY:", " ".repeat(indent)));
        grid.push(format!("{}{} {}{};", " ".repeat(2*indent),
            NcDds::vartype_str(var.vartype()),
            var.name(),
            var.dimensions().iter().map(|d|
                format!("[{} = {}]", d.name(), d.len())).collect::<String>())
            );
        grid.push(format!("{} MAPS:", " ".repeat(indent)));
        for d in var.dimensions() {
            let dvar = nc.variable(d.name()).expect(&format!("No variable found for dimension: {}", d.name()));
            grid.push(NcDds::format_var(2*indent, dvar));
        }

        grid.push(format!("{}}} {};\n", " ".repeat(indent), var.name()));
        grid.join("\n")
    }

    pub fn build(f: String) -> anyhow::Result<NcDds> {
        debug!("building Data Descriptor Structure (DDS) for {}", f);
        let nc = netcdf::open(f.clone())?;

        let indent: usize = 4;

        let mut map = HashMap::new();

        for var in nc.variables() {
            if var.dimensions().len() < 2 {
                let mut v = NcDds::format_var(indent, var);
                v.push_str("\n");
                map.insert(var.name().to_string(), v);
            } else {
                map.insert(var.name().to_string(), NcDds::format_grid(indent, &nc, var));
            }
        }

        Ok(NcDds { f: f, vars: Arc::new(map) })
    }

    pub fn dds(&self, vars: Option<Vec<String>>) -> String {
        let dds: String = {
            if let Some(vars) = vars {
                vars.iter().map(|v| self.vars[v].clone()).collect::<String>()
            } else {
                self.vars.values().map(|v| v.clone()).collect::<String>()
            }
        };

        format!("Dataset {{\n{}}} {};", dds, self.f)
    }
}

