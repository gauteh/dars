use itertools::Itertools;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

use super::constraint::{Constraint, ConstraintVariable};
use super::hyperslab;

const INDENT: usize = 4;

/// Data Description Structure
///
/// TODO: Serializable.
#[derive(Default)]
pub struct Dds {
    /// Variables, needs to be ordered for libnetcdf clients to work correctly.
    variables: BTreeMap<String, Variable>,

    /// Dimensions and size.
    dimensions: HashMap<String, usize>,

    file_name: String,
}

pub struct Variable {
    pub name: String,
    pub vartype: VarType,
    pub dimensions: Vec<String>,
}

pub enum VarType {
    Float32,
    Float64,
    UInt16,
    UInt32,
    UInt64,
    Int16,
    Int32,
    Int64,
    Byte,
    String,
}

impl fmt::Display for VarType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            VarType::Float32 => "Float32",
            VarType::Float64 => "Float64",
            VarType::UInt16 => "UInt16",
            VarType::UInt32 => "UInt32",
            VarType::UInt64 => "UInt64",
            VarType::Int16 => "Int16",
            VarType::Int32 => "Int32",
            VarType::Int64 => "Int64",
            VarType::Byte => "Byte",
            VarType::String => "String",
        })
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
        let variables = dataset
            .variables()
            .into_iter()
            .map(|var| (var.name.clone(), var))
            .collect::<Vec<_>>();

        let dimensions = variables
            .iter()
            .map(|v| &v.1.dimensions)
            .flatten()
            .unique()
            .map(|d| (d.to_string(), dataset.dimension_length(&d)))
            .collect();

        Dds {
            variables: variables.into_iter().collect(),
            dimensions,
            file_name: dataset.file_name(),
        }
    }
}

impl Dds {
    fn slab_dim_sz(&self, dim: &str, i: usize, slab: Option<impl AsRef<[usize]>>) -> usize {
        self.dimensions
            .get(dim)
            .map(|dimsz| {
                if let Some(slab) = slab.as_ref() {
                    if i < slab.as_ref().len() {
                        match slab.as_ref()[i] {
                            i if i <= *dimsz => i,
                            _ => *dimsz,
                        }
                    } else {
                        *dimsz
                    }
                } else {
                    *dimsz
                }
            })
            .unwrap()
    }

    /// Variables with one or zero dimensions.
    fn format_var(&self, var: &str, slab: Option<impl AsRef<[usize]>>) -> Option<String> {
        self.variables.get(var).map(|var| {
            if !var.dimensions.is_empty() {
                format!(
                    "{}{} {}[{} = {}];",
                    " ".repeat(INDENT),
                    var.vartype,
                    var.name,
                    var.dimensions[0],
                    self.slab_dim_sz(&var.dimensions[0], 0, slab)
                )
            } else {
                format!("{}{} {};", " ".repeat(INDENT), var.vartype, var.name)
            }
        })
    }

    /// Structs or variable selected in grid.
    fn format_struct(&self, var: &str, dim: &str, slab: Option<Vec<usize>>) -> String {
        let mut grid: Vec<String> = Vec::new();

        let var = self.variables.get(var).unwrap();
        let dim = self.variables.get(dim).unwrap();

        grid.push(format!("{}Structure {{", " ".repeat(INDENT)));

        grid.push(format!(
            "{}{} {}{};",
            " ".repeat(2 * INDENT),
            dim.vartype,
            dim.name,
            dim.dimensions
                .iter()
                .enumerate()
                .map(|(i, d)| format!("[{} = {}]", d, self.slab_dim_sz(&d, i, slab.as_ref())))
                .collect::<String>()
        ));

        grid.push(format!("{}}} {};", " ".repeat(INDENT), var.name));

        grid.join("\n")
    }

    /// Variable with more than one dimension.
    fn format_grid(&self, var: &str, slab: Option<Vec<usize>>) -> String {
        let var = self.variables.get(var).unwrap();

        if !var
            .dimensions
            .iter()
            .all(|d| self.dimensions.get(d).is_some())
        {
            return format!(
                "{}{} {}{};\n",
                " ".repeat(INDENT),
                var.vartype,
                var.name,
                var.dimensions
                    .iter()
                    .enumerate()
                    .map(|(i, d)| format!("[{} = {}]", d, self.slab_dim_sz(&d, i, slab.as_ref())))
                    .collect::<String>()
            );
        }

        let mut grid: Vec<String> = Vec::new();

        grid.push(format!("{}Grid {{", " ".repeat(INDENT)));
        grid.push(format!("{} ARRAY:", " ".repeat(INDENT)));
        grid.push(format!(
            "{}{} {}{};",
            " ".repeat(2 * INDENT),
            var.vartype,
            var.name,
            var.dimensions
                .iter()
                .enumerate()
                .map(|(i, d)| format!("[{} = {}]", d, self.slab_dim_sz(&d, i, slab.as_ref())))
                .collect::<String>()
        ));
        grid.push(format!("{} MAPS:", " ".repeat(INDENT)));
        for d in &var.dimensions {
            grid.push(format!(
                "{}{}",
                " ".repeat(INDENT),
                self.format_var(&d, slab.as_ref()).unwrap()
            ));
        }

        grid.push(format!("{}}} {};", " ".repeat(INDENT), var.name));
        grid.join("\n")
    }

    /// DDS response for all variables.
    pub fn all(&self) -> String {
        std::iter::once("Dataset {".to_string())
            .chain(self.variables.iter().map(|(name, var)| {
                if var.dimensions.len() > 1 {
                    self.format_grid(name, None)
                } else {
                    self.format_var(name, Option::<Vec<usize>>::None).unwrap()
                }
            }))
            .chain(std::iter::once(format!("}} {};", self.file_name)))
            .join("\n")
    }

    /// Counts the number of elements a hyperslab slice results in.
    fn counts(slab: &Option<Vec<Vec<usize>>>) -> Option<Vec<usize>> {
        slab.as_ref().map(|slab| {
            slab.iter()
                .map(|v| hyperslab::count_slab(&v))
                .collect::<Vec<usize>>()
        })
    }

    /// DDS string given the constraint.
    pub fn dds(&self, constraint: &Constraint) -> Result<String, anyhow::Error> {
        use ConstraintVariable::*;

        if constraint.len() == 0 {
            Ok(self.all())
        } else {
            std::iter::once(Ok("Dataset {".to_string()))
                .chain(constraint.iter().map(|c| {
                    match c {
                        Variable((var, slab)) => self
                            .variables
                            .get(var.as_str())
                            .and_then(|dvar| {
                                if dvar.dimensions.len() > 1 {
                                    Some(self.format_grid(var, Dds::counts(slab)))
                                } else {
                                    self.format_var(var, Dds::counts(slab))
                                }
                            })
                            .ok_or_else(|| anyhow!("Variable not found")),
                        Structure((v1, v2, slab)) => self
                            .variables
                            .get(v1.as_str())
                            .and(self.variables.get(v2.as_str()))
                            .and_then(|_| Some(self.format_struct(v1, v2, Dds::counts(slab))))
                            .ok_or_else(|| anyhow!("Variable or dimension not found")),
                    }
                }))
                .chain(std::iter::once(Ok(format!("}} {};", self.file_name))))
                .collect::<Result<Vec<String>, anyhow::Error>>()
                .map(|v| v.join("\n"))
        }
    }

    // TODO: Return a DdsResponse with Variables, Grids, Structures with indices and counts and
    // sizes (with datatype size). All verified against size.
}
