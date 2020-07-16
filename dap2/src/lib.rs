//! # DAP/2
//!
//! An implementation of the serverside DAP/2 protocol.
//!
//! ## Resources
//!
//! * [OPeNDAP design documentation](https://www.opendap.org/support/design-documentation)
//! * [The DAP/2 protocol specification](https://www.opendap.org/pdf/ESE-RFC-004v1.2.pdf)
//! * [libdap](https://opendap.github.io/libdap4/html/)
//!
//! ## Other implementations
//!
//! * [Hyrax](https://www.opendap.org/software/hyrax-data-server) ( [guide](https://opendap.github.io/hyrax_guide/Master_Hyrax_Guide.html) | [documentation](https://docs.opendap.org/index.php/Hyrax) )
//!   - [BES](https://opendap.github.io/bes/html/)
//!   - [Developer information](https://docs.opendap.org/index.php/Developer_Info)
//! * [Thredds](https://www.unidata.ucar.edu/software/tds/current/)
#[macro_use]
extern crate log;

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
