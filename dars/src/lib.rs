#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

pub mod config;
pub mod data;
pub mod hdf5;
pub mod ncml;

fn make_extents<E>(e: E) -> anyhow::Result<hidefix::extent::Extents>
where
    E: TryInto<hidefix::extent::Extents>,
    E::Error: Into<anyhow::Error>,
{
    e.try_into().map_err(|e| e.into())
}
