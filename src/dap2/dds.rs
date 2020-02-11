use super::requests::parse_query;
use std::collections::HashMap;

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

    fn format_var(&self, indent: usize, var: &netcdf::Variable, slab: Option<&[usize]>) -> String {
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
        slab: Option<&[usize]>,
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
        var: &netcdf::Variable,
        dim: &netcdf::Variable,
        slab: Option<&[usize]>,
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
                let mut v = self.format_var(indent, &var, None);
                v.push_str("\n");
                map.insert(var.name().to_string(), v);
                posmap.insert(var.name().to_string(), z);
            } else {
                map.insert(
                    var.name().to_string(),
                    self.format_grid(indent, &nc, &var, None),
                );
                posmap.insert(var.name().to_string(), z);

                map.insert(
                    format!("{}.{}", var.name(), var.name()),
                    self.format_struct(indent, &var, &var, None),
                );
                posmap.insert(format!("{}.{}", var.name(), var.name()), z);

                for d in var.dimensions() {
                    match nc.variable(&d.name()) {
                        Some(dvar) => {
                            posmap.insert(format!("{}.{}", var.name(), d.name()), z);
                            map.insert(
                                format!("{}.{}", var.name(), d.name()),
                                self.format_struct(indent, &var, &dvar, None),
                            )
                        }
                        _ => None,
                    };
                }
            }
        }

        (posmap, map)
    }

    fn build_var(&self, nc: &netcdf::File, var: &str, count: &[usize]) -> Option<String> {
        let indent: usize = 4;

        match var.find('.') {
            Some(i) => match nc.variable(&var[..i]) {
                Some(ivar) => match nc.variable(&var[i + 1..]) {
                    Some(dvar) => Some(self.format_struct(indent, &ivar, &dvar, Some(count))),
                    _ => None,
                },
                _ => None,
            },

            None => match nc.variable(var) {
                Some(var) => match var.dimensions().len() {
                    l if l < 2 => Some(self.format_var(indent, &var, Some(count))),
                    _ => Some(self.format_grid(indent, &nc, &var, Some(count))),
                },
                _ => None,
            },
        }
    }

    /// Filename of dataset.
    fn get_file_name(&self) -> String;

    /// Look up variable in cache.
    fn get_var(&self, var: &str) -> Option<String>;

    /// Get DDS for file and variables.
    fn dds(
        &self,
        nc: &netcdf::File,
        vars: &Vec<(String, Option<Vec<usize>>, Option<Vec<usize>>)>,
    ) -> Result<String, anyhow::Error> {
        trace!("dds: vars: {:?}", vars);
        let dds: String = vars
            .iter()
            .map(|v| match &v.2 {
                Some(counts) => self.build_var(nc, &v.0, &counts).map(|mut s| {
                    s.push('\n');
                    s
                }),
                None => self.get_var(&v.0),
            })
            .collect::<Option<String>>()
            .ok_or(anyhow!("could not find variable"))?;

        Ok(format!("Dataset {{\n{}}} {};", dds, self.get_file_name()))
    }

    /// The variables to be sent when none are specified in the query.
    fn default_vars(&self) -> Vec<String>;

    fn variable_position(&self, variable: &str) -> &usize;

    /// Parse a DODS or DDS query.
    fn parse_query(
        &self,
        query: Option<&str>,
    ) -> Result<Vec<(String, Option<Vec<usize>>, Option<Vec<usize>>)>, anyhow::Error> {
        match query {
            None => Ok(self
                .default_vars()
                .iter()
                .map(|v| (v.clone(), None, None))
                .collect()),
            Some(q) => parse_query(q),
        }
        .map(|mut vars| {
            vars.sort_by(|a, b| {
                self.variable_position(&a.0)
                    .cmp(self.variable_position(&b.0))
            });
            vars
        })
    }
}
