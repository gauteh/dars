#![recursion_limit = "512"]
#![feature(async_closure)]
#![feature(test)]
extern crate test;

#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

pub mod config;
pub mod data;
pub mod hdf5;
pub mod ncml;


