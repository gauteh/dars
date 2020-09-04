//! # Constraint
//!
//! DAP constraints consist of variable list and slices [hyperslabs](crate::hyperslab) of those variables.
//!
//! See [Constraint::parse] on how to parse a query into variable constraints. A `Constraint` can
//! be passed to [crate::dds::Dds::dds] of a data-source to constrain the actual variables of a
//! dataset, returning a [crate::dds::DdsResponse] with [crate::dds::ConstrainedVariable]s that can
//! be used to stream the variables of a data-source.
//!
//! * Strides are not supported.
//! * Constraints based on variable value is not supported.
use crate::hyperslab;
use percent_encoding::percent_decode_str;
use std::ops::{Deref, DerefMut};

/// Variables with constraints.
#[derive(Debug, Clone)]
pub struct Constraint {
    variables: Vec<ConstraintVariable>,
}

impl Deref for Constraint {
    type Target = Vec<ConstraintVariable>;

    fn deref(&self) -> &Self::Target {
        &self.variables
    }
}

impl DerefMut for Constraint {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.variables
    }
}

#[derive(Debug, Clone)]
pub enum ConstraintVariable {
    Variable((String, Option<Vec<Vec<usize>>>)),

    // TODO: This is more like StructureMember
    Structure((String, String, Option<Vec<Vec<usize>>>)),
}

impl Constraint {
    /// Parse a query into variable constraints.
    pub fn parse(query: &str) -> anyhow::Result<Constraint> {
        let query = percent_decode_str(query).decode_utf8()?;
        debug!("query: {}", query);

        query
            .split(",")
            .map(|var| {
                if let Some(s) = var.find(".") {
                    let v1 = &var[..s];
                    let v2 = &var[s + 1..];

                    match v2.find("[") {
                        Some(i) => hyperslab::parse_hyperslab(&v2[i..])
                            .and_then(|slab| Ok((&v2[..i], Some(slab)))),
                        None => Ok((v2, None)),
                    }
                    .and_then(|(v2, slab)| {
                        Ok(ConstraintVariable::Structure((
                            v1.to_string(),
                            v2.to_string(),
                            slab,
                        )))
                    })
                } else {
                    match var.find("[") {
                        Some(i) => hyperslab::parse_hyperslab(&var[i..])
                            .and_then(|slab| Ok((&var[..i], Some(slab)))),
                        None => Ok((var, None)),
                    }
                    .and_then(|(var, slab)| {
                        Ok(ConstraintVariable::Variable((var.to_string(), slab)))
                    })
                }
            })
            .collect::<anyhow::Result<_>>()
            .and_then(|variables| Ok(Constraint { variables }))
    }

    /// An empty constraints, meaning all variables.
    pub fn empty() -> Constraint {
        Constraint {
            variables: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        assert_eq!(Constraint::empty().len(), 0);
    }

    #[test]
    fn single_variable() {
        let c = Constraint::parse("SST").unwrap();

        assert_eq!(c.len(), 1);

        if let ConstraintVariable::Variable((var, slab)) = &c[0] {
            assert_eq!(var, "SST");
            assert!(slab.is_none());
        }
    }

    #[test]
    fn single_variable_slab() {
        let c = Constraint::parse("SST[0:5]").unwrap();

        assert_eq!(c.len(), 1);

        if let ConstraintVariable::Variable((var, slab)) = &c[0] {
            assert_eq!(var, "SST");
            assert_eq!(*slab.as_ref().unwrap(), vec!(vec![0usize, 5]));
        } else {
            panic!("wrong enum");
        }
    }

    #[test]
    fn single_struct_slab() {
        let c = Constraint::parse("SST.TIME[0:5]").unwrap();

        assert_eq!(c.len(), 1);

        if let ConstraintVariable::Structure((v1, v2, slab)) = &c[0] {
            assert_eq!(v1, "SST");
            assert_eq!(v2, "TIME");
            assert_eq!(*slab.as_ref().unwrap(), vec!(vec![0usize, 5]));
        } else {
            panic!("wrong enum");
        }
    }

    #[test]
    fn single_struct_slab_indexes() {
        let c = Constraint::parse("SST.TIME[5][4]").unwrap();

        assert_eq!(c.len(), 1);

        if let ConstraintVariable::Structure((v1, v2, slab)) = &c[0] {
            assert_eq!(v1, "SST");
            assert_eq!(v2, "TIME");
            assert_eq!(*slab.as_ref().unwrap(), vec!(vec![5usize], vec![4usize]));
        } else {
            panic!("wrong enum");
        }
    }

    #[test]
    fn multi_struct_slab_indexes() {
        let c = Constraint::parse("SST.TIME[5][4],SST,TIME[4:5]").unwrap();

        assert_eq!(c.len(), 3);

        if let ConstraintVariable::Structure((v1, v2, slab)) = &c[0] {
            assert_eq!(v1, "SST");
            assert_eq!(v2, "TIME");
            assert_eq!(*slab.as_ref().unwrap(), vec!(vec![5usize], vec![4usize]));
        } else {
            panic!("wrong enum");
        }

        if let ConstraintVariable::Variable((var, slab)) = &c[1] {
            assert_eq!(var, "SST");
            assert!(slab.is_none());
        } else {
            panic!("wrong enum");
        }

        if let ConstraintVariable::Variable((var, slab)) = &c[2] {
            assert_eq!(var, "TIME");
            assert_eq!(*slab.as_ref().unwrap(), vec!(vec![4usize, 5]));
        } else {
            panic!("wrong enum");
        }
    }

    #[test]
    fn erroneous_queries() {
        assert!(Constraint::parse("SST[a]").is_err());
        assert!(Constraint::parse("SST[1").is_err());
        assert!(Constraint::parse("SST.SST[1:3:4:5]").is_err());
        // assert!(Constraint::parse("SST.SST[1],a]").is_err());
    }
}
