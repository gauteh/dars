use super::dds::ToDds;

/// DAS (Data Attribute Structure)
pub struct Das { }

pub trait ToDas { }

impl<T> From<T> for Das where T: ToDas {
    fn from(_: T) -> Self { todo!() }
}
// impl<T> From<T> for Das where T: ToDds { } // maybe enough?

impl Das {
}
