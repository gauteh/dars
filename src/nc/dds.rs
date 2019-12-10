use std::sync::Arc;
use std::collections::HashMap;

use crate::dap2::hyperslab::{count_slab, parse_hyberslab};

pub struct NcDds {
    f: String,
    pub vars: Arc<HashMap<String, String>>
}

impl NcDds {
    fn vartype_str(t: netcdf_sys::nc_type) -> String {
        match t {
            netcdf_sys::NC_FLOAT => "Float32".to_string(),
            netcdf_sys::NC_DOUBLE => "Float64".to_string(),
            netcdf_sys::NC_INT => "Int32".to_string(),
            netcdf_sys::NC_BYTE => "Byte".to_string(),
            netcdf_sys::NC_UBYTE => "Byte".to_string(),
            // netcdf_sys::NC_CHAR => "String".to_string(),
            netcdf_sys::NC_STRING => "String".to_string(),
            e => format!("Unimplemented: {:?}", e)
        }
    }

    fn format_var(indent: usize, var: &netcdf::Variable, slab: &Option<Vec<usize>>) -> String {
        if var.dimensions().len() >= 1 {
            format!("{}{} {}[{} = {}];",
                    " ".repeat(indent),
                    NcDds::vartype_str(var.vartype()),
                    var.name(),
                    var.dimensions()[0].name(),
                    slab.as_ref().and_then(|s| s.get(0)).unwrap_or(&var.dimensions()[0].len()))
        } else {
            format!("{}{} {};", " ".repeat(indent), NcDds::vartype_str(var.vartype()), var.name())
        }
    }

    fn format_grid(indent: usize, nc: &netcdf::File, var: &netcdf::Variable, slab: &Option<Vec<usize>>) -> String {
        if !var.dimensions().iter().all(|d| nc.variable(d.name()).is_some()) {
            return format!("{}{} {}{};\n", " ".repeat(indent),
            NcDds::vartype_str(var.vartype()),
            var.name(),
            var.dimensions().iter().enumerate().map(|(i, d)|
                format!("[{} = {}]", d.name(),
                    slab.as_ref().and_then(|s| s.get(i)).unwrap_or(&d.len())
                    )).collect::<String>());
        }

        let mut grid: Vec<String> = Vec::new();

        grid.push(format!("{}Grid {{", " ".repeat(indent)));
        grid.push(format!("{} ARRAY:", " ".repeat(indent)));
        grid.push(format!("{}{} {}{};", " ".repeat(2*indent),
            NcDds::vartype_str(var.vartype()),
            var.name(),
            var.dimensions().iter().enumerate().map(|(i, d)|
                format!("[{} = {}]", d.name(),
                    slab.as_ref().and_then(|s| s.get(i)).unwrap_or(&d.len())
                    )).collect::<String>())
            );
        grid.push(format!("{} MAPS:", " ".repeat(indent)));
        for d in var.dimensions() {
            let dvar = nc.variable(d.name()).expect(&format!("No variable found for dimension: {}", d.name()));
            grid.push(NcDds::format_var(2*indent, dvar, slab));
        }

        grid.push(format!("{}}} {};\n", " ".repeat(indent), var.name()));
        grid.join("\n")
    }

    fn format_struct(indent: usize, _nc: &netcdf::File, var: &netcdf::Variable, dim: &netcdf::Variable, slab: &Option<Vec<usize>>) -> String {
        let mut grid: Vec<String> = Vec::new();

        grid.push(format!("{}Structure {{", " ".repeat(indent)));

        grid.push(format!("{}{} {}{};", " ".repeat(2*indent),
            NcDds::vartype_str(dim.vartype()),
            dim.name(),
            dim.dimensions().iter().enumerate().map(|(i, d)|
                format!("[{} = {}]", d.name(),
                    slab.as_ref().and_then(|s| s.get(i)).unwrap_or(&d.len())
                )).collect::<String>())
            );

        grid.push(format!("{}}} {};\n", " ".repeat(indent), var.name()));

        grid.join("\n")
    }

    pub fn build(f: String) -> anyhow::Result<NcDds> {
        debug!("Building Data Descriptor Structure (DDS) for {}", f);
        let nc = netcdf::open(f.clone())?;

        let indent: usize = 4;

        let mut map = HashMap::new();

        for var in nc.variables()
            .filter(|v| v.vartype() != netcdf_sys::NC_CHAR) {
            if var.dimensions().len() < 2 {
                let mut v = NcDds::format_var(indent, var, &None);
                v.push_str("\n");
                map.insert(var.name().to_string(), v);
            } else {
                map.insert(var.name().to_string(), NcDds::format_grid(indent, &nc, var, &None));

                map.insert(format!("{}.{}", var.name(), var.name()), NcDds::format_struct(indent, &nc, var, var, &None));

                for d in var.dimensions() {
                    match nc.variable(d.name()) {
                        Some(dvar) => map.insert(format!("{}.{}", var.name(), d.name()), NcDds::format_struct(indent, &nc, var, dvar, &None)),
                        _ => None
                    };
                }
            }
        }

        Ok(NcDds { f: f, vars: Arc::new(map) })
    }

    fn build_var(nc: &netcdf::File, var: &str, slab: Vec<Vec<usize>>) -> Option<String> {
        let indent: usize = 4;

        let slab: Vec<usize> = slab.iter().map(count_slab).collect();

        match var.find(".") {
            Some(i) =>
                match nc.variable(&var[..i]) {
                    Some(ivar) => match nc.variable(&var[i+1..]) {
                        Some(dvar) => Some(NcDds::format_struct(indent, &nc, ivar, dvar, &Some(slab))),
                        _ => None
                    },
                    _ => None
                },

            None => match nc.variable(var) {
                Some(var) => match var.dimensions().len() {
                            l if l < 2 => Some(NcDds::format_var(indent, var, &Some(slab))),
                            _ => Some(NcDds::format_grid(indent, &nc, var, &Some(slab)))
                    },
                _ => None
            }
        }
    }


    pub fn dds(&self, nc: &netcdf::File, vars: &Vec<String>) -> Result<String, anyhow::Error> {
        let dds: String = {
            vars.iter()
                .map(|v|
                    match v.find("[") {
                        Some(i) =>
                            match parse_hyberslab(&v[i..]) {
                                Ok(slab) => NcDds::build_var(nc, &v[..i], slab),
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

        Ok(format!("Dataset {{\n{}}} {};", dds, self.f))
    }

    pub fn default_vars(&self) -> Vec<String> {
        self.vars.iter().filter(|(k,_)| !k.contains(".")).map(|(k,_)| k.clone()).collect()
    }
}

