#![feature(trait_alias)]

#[macro_use]
extern crate anyhow;

pub mod constraint;
pub mod das;
pub mod dds;
pub mod dods;
pub mod hyperslab;

pub use constraint::Constraint;
pub use das::Das;
pub use dds::Dds;
pub use dods::Dods;
