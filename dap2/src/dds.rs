/// Data Description Structure
pub struct Dds { }

pub trait ToDds {
    // define some generics that can be used to build a dds
}

impl<T> From<T> for Dds where T: ToDds {
    // make a dds struct for anything that impls ToDds
    fn from(_: T) -> Self { todo!() }
}

impl Dds {
    // get dds for a given query
}
