/// DAS (Data Attribute Structure)
pub struct Das { }

pub trait ToDas { }

impl From<ToDas> for Das { }
impl From<ToDds> for Das { } // maybe enough?

impl Das {
}
