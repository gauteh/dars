use std::fmt;
use std::collections::HashMap;
use itertools::Itertools;

const indent: usize = 4;

/// Data Description Structure
///
/// TODO: Serializable.
#[derive(Default)]
pub struct Dds {
    // Probably need to use BTreeMap to get correctly ordered variables.

    /// Map of variable -> (size, type, [dimensions])
    variables: HashMap<String, Variable>,

    /// Map of dimensions -> size
    dimensions: HashMap<String, usize>,

    file_name: String,
}

pub struct Variable {
    pub name: String,
    pub vartype: VarType,
    pub dimensions: Vec<String> // TODO: use ids
}

pub enum VarType {
    Float32,
    Float64,
    Int16,
    Int32,
    Byte,
    String
}

impl fmt::Display for VarType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            match self {
                VarType::Float32 => "Float32",
                VarType::Float64 => "Float64",
                VarType::Int16   => "Int16",
                VarType::Int32   => "Int32",
                VarType::Byte    => "Byte",
                VarType::String  => "String"
            }
        )
    }
}

pub trait ToDds {
    fn variables(&self) -> Vec<Variable>;
    fn dimension_length(&self, dim: &str) -> usize;
    fn file_name(&self) -> String;
}

impl<T> From<T> for Dds
where
    T: ToDds,
{
    // make a dds struct for anything that impls ToDds
    fn from(dataset: T) -> Self {

        let variables = dataset.variables().into_iter().map(
            |var| (var.name.clone(), var) ).collect::<Vec<_>>();

        let dimensions = variables.iter()
            .map(|v| &v.1.dimensions)
            .flatten()
            .unique()
            .map(|d| (d.to_string(), dataset.dimension_length(&d)))
            .collect();

        Dds { variables: variables.into_iter().collect(), dimensions, file_name: dataset.file_name() }
    }
}

impl Dds {
    fn slab_dim_sz(&self, dim: &str, i: usize, slab: Option<&[usize]>) -> usize {
        self.dimensions.get(dim).map(|dimsz| {
            if let Some(slab) = slab.as_ref() {
                if i < slab.len() {
                    match slab[i] {
                        i if i <= *dimsz => i,
                        _ => *dimsz
                    }
                } else {
                    *dimsz
                }
            } else {
                *dimsz
            }
        }).unwrap()

    }

    /// Variables with one or zero dimensions.
    fn format_var(&self, var: &str, slab: Option<&[usize]>) -> Option<String> {
        self.variables.get(var).map(|var|
            if !var.dimensions.is_empty() {
                format!(
                    "{}{} {}[{} = {}];",
                    " ".repeat(indent),
                    var.vartype,
                    var.name,
                    var.dimensions[0],
                    self.slab_dim_sz(&var.dimensions[0], 0, slab)
                )
            } else {
                format!(
                    "{}{} {};",
                    " ".repeat(indent),
                    var.vartype,
                    var.name
                )
            })
    }

    /// Structs or variable selected in grid.
    fn format_struct(
        &self,
        var: &str,
        dim: &str,
        slab: Option<&[usize]>,
    ) -> String {
        let mut grid: Vec<String> = Vec::new();

        let var = self.variables.get(var).unwrap();
        let dim = self.variables.get(dim).unwrap();

        grid.push(format!("{}Structure {{", " ".repeat(indent)));

        grid.push(format!(
            "{}{} {}{};",
            " ".repeat(2 * indent),
            dim.vartype,
            dim.name,
            dim.dimensions
                .iter()
                .enumerate()
                .map(|(i, d)| format!(
                    "[{} = {}]",
                    d,
                    self.slab_dim_sz(&d, i, slab)
                ))
                .collect::<String>()
        ));

        grid.push(format!("{}}} {};\n", " ".repeat(indent), var.name));

        grid.join("\n")
    }

    /// Variable with more than one dimension.
    fn format_grid(
        &self,
        var: &str,
        slab: Option<&[usize]>,
    ) -> String {
        let var = self.variables.get(var).unwrap();

        if !var
            .dimensions
            .iter()
            .all(|d| self.dimensions.get(d).is_some())
        {
            return format!(
                "{}{} {}{};\n",
                " ".repeat(indent),
                var.vartype,
                var.name,
                var.dimensions
                    .iter()
                    .enumerate()
                    .map(|(i, d)| format!(
                        "[{} = {}]",
                        d,
                        self.slab_dim_sz(&d, i, slab)
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
            var.vartype,
            var.name,
            var.dimensions
                .iter()
                .enumerate()
                .map(|(i, d)| format!(
                    "[{} = {}]",
                    d,
                    self.slab_dim_sz(&d, i, slab)
                ))
                .collect::<String>()
        ));
        grid.push(format!("{} MAPS:", " ".repeat(indent)));
        for d in &var.dimensions {
            grid.push(self.format_var(&d, slab).unwrap());
        }

        grid.push(format!("{}}} {};\n", " ".repeat(indent), var.name));
        grid.join("\n")
    }

    pub fn all(&self) -> String {
        self.variables.iter().map(|(name, var)| {
            if var.dimensions.len() > 1 {
                self.format_grid(name, None)
            } else {
                self.format_var(name, None).unwrap()
            }
        }).collect()
    }
}
