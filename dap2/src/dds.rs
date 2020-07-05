use itertools::{izip, Itertools};
use std::collections::BTreeMap;
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

    file_name: String,
}

// TODO: Use Rc for String's?
pub struct Variable {
    pub name: String,
    pub vartype: VarType,
    pub dimensions: Vec<String>,
    pub shape: Vec<usize>,
}

#[derive(Copy, Clone)]
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

impl VarType {
    pub fn size(&self) -> usize {
        use VarType::*;

        match self {
            Byte | String => 1,
            UInt16 | Int16 => 2,
            Float32 | UInt32 | Int32 => 4,
            Float64 | UInt64 | Int64 => 8,
        }
    }
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

        Dds {
            variables: variables.into_iter().collect(),
            file_name: dataset.file_name(),
        }
    }
}

impl Dds {
    /// Counts the number of elements a hyperslab slice results in.
    fn counts(slab: &Option<Vec<Vec<usize>>>) -> Option<Vec<usize>> {
        slab.as_ref().map(|slab| {
            slab.iter()
                .map(|v| hyperslab::count_slab(&v))
                .collect::<Vec<usize>>()
        })
    }

    /// Get an array of starting index of hyperslabs.
    fn indices(slab: &Option<Vec<Vec<usize>>>) -> Option<Vec<usize>> {
        slab.as_ref()
            .map(|slab| slab.iter().map(|v| v[0]).collect::<Vec<usize>>())
    }

    /// Counts number of elements in hyperslap slice and extends with shape of variable if missing
    /// dimensions.
    fn extend_counts(
        &self,
        var: &Variable,
        indices: &[usize],
        slab: &Option<Vec<Vec<usize>>>,
    ) -> Result<Vec<usize>, anyhow::Error> {
        use itertools::EitherOrBoth::*;

        Dds::counts(slab)
            .map(|counts| {
                counts
                    .iter()
                    .zip_longest(var.shape.iter().cloned())
                    .zip(indices)
                    .map(|(e, i)| match e {
                        Left(_) => Err(anyhow!("More counts than dimensions")),
                        Both(c, s) => {
                            if *c <= (s - i) {
                                Ok(*c)
                            } else {
                                Err(anyhow!("Count greater than dimension shape"))
                            }
                        }
                        Right(c) => {
                            if *i <= c {
                                Err(anyhow!("Indices greater than dimension shape"))
                            } else {
                                Ok(c - i)
                            }
                        }
                    })
                    .collect()
            })
            .unwrap_or_else(|| {
                var.shape
                    .iter()
                    .zip(indices)
                    .map(|(s, i)| Ok(s - i))
                    .collect()
            })
    }

    /// Get array of indices from hyperslab, extending with 0 if missing dimensions.
    fn extend_indices(
        &self,
        var: &Variable,
        slab: &Option<Vec<Vec<usize>>>,
    ) -> Result<Vec<usize>, anyhow::Error> {
        Dds::indices(slab)
            .map(|mut indices| {
                if indices.len() > var.shape.len() {
                    return Err(anyhow!("More indices than dimensions"));
                }

                indices.extend((0..(var.shape.len() - indices.len())).map(|_| 0));
                if indices.iter().zip(&var.shape).any(|(i, s)| *i >= *s) {
                    Err(anyhow!("Indices out of range"))
                } else {
                    Ok(indices)
                }
            })
            .unwrap_or_else(|| Ok(vec![0; var.shape.len()]))
    }

    pub fn all(&self) -> DdsResponse {
        DdsResponse {
            file_name: self.file_name.clone(),
            variables: self
                .variables
                .iter()
                .map(|(_, var)| {
                    // If not all dimensions have corresponding variables, return as variable and
                    // not a gridded variable.
                    if var.dimensions.len() > 1
                        && var
                            .dimensions
                            .iter()
                            .all(|d| self.variables.get(d).is_some())
                    {
                        ConstrainedVariable::Grid {
                            variable: DdsVariableDetails {
                                name: var.name.clone(),
                                vartype: var.vartype,
                                dimensions: var
                                    .dimensions
                                    .iter()
                                    .cloned()
                                    .zip(var.shape.iter().cloned())
                                    .collect(),
                                size: var.shape.iter().product(),
                                indices: vec![0; var.shape.len()],
                                counts: var.shape.clone(),
                            },
                            dimensions: var
                                .dimensions
                                .iter()
                                .filter_map(|dim| {
                                    self.variables.get(dim).map(|dim| DdsVariableDetails {
                                        name: dim.name.clone(),
                                        vartype: dim.vartype,
                                        dimensions: dim
                                            .dimensions
                                            .iter()
                                            .cloned()
                                            .zip(dim.shape.iter().cloned())
                                            .collect(),
                                        size: dim.shape.iter().product(),
                                        indices: vec![0; dim.shape.len()],
                                        counts: dim.shape.clone(),
                                    })
                                })
                                .collect(),
                        }
                    } else {
                        ConstrainedVariable::Variable(DdsVariableDetails {
                            name: var.name.clone(),
                            vartype: var.vartype,
                            dimensions: var
                                .dimensions
                                .iter()
                                .cloned()
                                .zip(var.shape.iter().cloned())
                                .collect(),
                            size: var.shape.iter().product(),
                            indices: vec![0; var.shape.len()],
                            counts: var.shape.clone(),
                        })
                    }
                })
                .collect(),
        }
    }

    pub fn dds(&self, constraint: &Constraint) -> Result<DdsResponse, anyhow::Error> {
        use ConstraintVariable::*;

        if constraint.len() == 0 {
            Ok(self.all())
        } else {
            Ok(DdsResponse {
                file_name: self.file_name.clone(),
                variables: constraint
                    .iter()
                    .map(|c| {
                        match c {
                            Variable((var, slab)) => {
                                self.variables
                                    .get(var.as_str())
                                    .map(|var| {
                                        let indices = self.extend_indices(var, slab)?;
                                        let counts = self.extend_counts(var, &indices, slab)?;

                                        if var.dimensions.len() > 1
                                            && var
                                                .dimensions
                                                .iter()
                                                .all(|d| self.variables.get(d).is_some())
                                        {
                                            Ok(ConstrainedVariable::Grid {
                                                variable: DdsVariableDetails {
                                                    name: var.name.clone(),
                                                    vartype: var.vartype,
                                                    dimensions: var
                                                        .dimensions
                                                        .iter()
                                                        .cloned()
                                                        .zip(counts.iter().cloned())
                                                        .collect(),
                                                    size: counts.iter().product(),
                                                    indices: indices.clone(),
                                                    counts: counts.clone(),
                                                },

                                                // XXX: More deeply nested dimensions are not
                                                // supported.
                                                dimensions: izip!(
                                                    &var.dimensions,
                                                    &indices,
                                                    &counts
                                                )
                                                .map(|(dim, i, c)| {
                                                    self.variables
                                                        .get(dim)
                                                        .map(|dim| DdsVariableDetails {
                                                            name: dim.name.clone(),
                                                            vartype: dim.vartype,
                                                            dimensions: vec![(
                                                                dim.name.clone(),
                                                                *c,
                                                            )],
                                                            size: *c,
                                                            indices: vec![*i],
                                                            counts: vec![*c],
                                                        })
                                                        .ok_or_else(|| {
                                                            anyhow!(
                                                                "Variable not found: {}",
                                                                var.name
                                                            )
                                                        })
                                                })
                                                .collect::<Result<Vec<_>, _>>()?,
                                            })
                                        } else {
                                            Ok(ConstrainedVariable::Variable(DdsVariableDetails {
                                                name: var.name.clone(),
                                                vartype: var.vartype,
                                                dimensions: var
                                                    .dimensions
                                                    .iter()
                                                    .cloned()
                                                    .zip(counts.iter().cloned())
                                                    .collect(),
                                                size: counts.iter().product(),
                                                indices: indices,
                                                counts: counts,
                                            }))
                                        }
                                    })
                                    .ok_or_else(|| anyhow!("Variable not found: {}", var))?
                            }

                            Structure((v1, v2, slab)) => self
                                .variables
                                .get(v1.as_str())
                                .and_then(|var1| {
                                    self.variables.get(v2.as_str()).map(|var2| (var1, var2))
                                })
                                .ok_or_else(|| anyhow!("Variable not found: {}.{}", v1, v2))
                                .and_then(|(var1, var2)| {
                                    let indices = self.extend_indices(var2, slab)?;
                                    let counts = self.extend_counts(var2, &indices, slab)?;

                                    Ok(ConstrainedVariable::Structure {
                                        variable: var1.name.clone(),
                                        member: DdsVariableDetails {
                                            name: var2.name.clone(),
                                            vartype: var2.vartype,
                                            dimensions: var2
                                                .dimensions
                                                .iter()
                                                .cloned()
                                                .zip(counts.iter().cloned())
                                                .collect(),
                                            size: counts.iter().product(),
                                            indices: indices,
                                            counts: counts,
                                        },
                                    })
                                }),
                        }
                    })
                    .collect::<Result<Vec<ConstrainedVariable>, anyhow::Error>>()?,
            })
        }
    }
}

pub struct DdsVariableDetails {
    pub name: String,
    pub vartype: VarType,

    /// Dimensions and their _constrained_ size
    pub dimensions: Vec<(String, usize)>,

    /// The _constrained_ length of array or variable in elements (constructed from `counts.prod()`)
    size: usize,

    /// Slice in the variable
    pub indices: Vec<usize>,
    pub counts: Vec<usize>,
}

impl DdsVariableDetails {
    pub fn is_scalar(&self) -> bool {
        self.dimensions.is_empty()
    }

    /// Number of elements in array.
    pub fn len(&self) -> usize {
        self.size
    }

    /// Size of variable in bytes.
    pub fn size(&self) -> usize {
        self.size * self.vartype.size()
    }

    /// Size of variable with XDR header in bytes.
    pub fn dods_size(&self) -> usize {
        self.size() + if self.is_scalar() { 0 } else { 8 }
    }
}

pub enum ConstrainedVariable {
    Variable(DdsVariableDetails),
    Grid {
        variable: DdsVariableDetails,
        dimensions: Vec<DdsVariableDetails>,
    },
    Structure {
        variable: String,
        member: DdsVariableDetails,
    },
}

impl ConstrainedVariable {
    /// Total size of variable in bytes.
    pub fn size(&self) -> usize {
        use ConstrainedVariable::*;

        match self {
            Variable(v)
            | Structure {
                variable: _,
                member: v,
            } => v.size(),
            Grid {
                variable,
                dimensions,
            } => variable.size() + dimensions.iter().map(|d| d.size()).sum::<usize>(),
        }
    }

    /// Total size of variable in bytes serialized as XDR.
    pub fn dods_size(&self) -> usize {
        use ConstrainedVariable::*;

        match self {
            Variable(v)
            | Structure {
                variable: _,
                member: v,
            } => v.dods_size(),
            Grid {
                variable,
                dimensions,
            } => variable.dods_size() + dimensions.iter().map(|d| d.dods_size()).sum::<usize>(),
        }
    }
}

impl fmt::Display for DdsVariableDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.vartype, self.name)?;

        for (d, sz) in &self.dimensions {
            write!(f, "[{} = {}]", d, sz)?;
        }

        write!(f, ";")
    }
}

impl fmt::Display for ConstrainedVariable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ConstrainedVariable::*;

        match self {
            Variable(v) => {
                write!(f, "{}", " ".repeat(INDENT))?;
                v.fmt(f)
            }

            Grid {
                variable,
                dimensions,
            } => {
                write!(f, "{}Grid {{\n", " ".repeat(INDENT))?;
                write!(f, "{} ARRAY:\n", " ".repeat(INDENT))?;

                write!(f, "{}", " ".repeat(2 * INDENT))?;
                variable.fmt(f)?;
                write!(f, "\n")?;

                write!(f, "{} MAPS:\n", " ".repeat(INDENT))?;
                for d in dimensions {
                    write!(f, "{}", " ".repeat(2 * INDENT))?;
                    d.fmt(f)?;
                    write!(f, "\n")?;
                }

                write!(f, "{}}} {};", " ".repeat(INDENT), variable.name)
            }

            Structure { variable, member } => {
                write!(f, "{}Structure {{\n", " ".repeat(INDENT))?;
                write!(f, "{}", " ".repeat(2 * INDENT))?;
                member.fmt(f)?;
                write!(f, "\n")?;
                write!(f, "{}}} {};", " ".repeat(INDENT), variable)
            }
        }
    }
}

pub struct DdsResponse {
    pub variables: Vec<ConstrainedVariable>,
    pub file_name: String,
}

impl DdsResponse {
    /// Total size of variables in bytes.
    pub fn size(&self) -> usize {
        self.variables.iter().map(|v| v.size()).sum()
    }

    /// Total XDR size of variables in bytes.
    pub fn dods_size(&self) -> usize {
        self.variables.iter().map(|v| v.dods_size()).sum()
    }
}

impl fmt::Display for DdsResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Dataset {{\n")?;
        for c in &self.variables {
            c.fmt(f)?;
            write!(f, "\n")?;
        }
        write!(f, "}} {};", self.file_name)
    }
}
