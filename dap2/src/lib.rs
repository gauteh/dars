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

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;

pub mod constraint;
pub mod das;
pub mod dds;
pub mod dods;
pub mod hyperslab;

pub use constraint::Constraint;
pub use das::Das;
pub use dds::Dds;
pub use dods::Dods;

/// The `Dap2` trait defines the necessary methods for serving a data-source (containing many
/// variables, or `dataset`s in `HDF5` terms) over the `DAP2` protocol. Additionally the
/// [dods::Dods] trait which is implemented for sources implementing this trait handles the
/// streaming a DODS response of several constrained variables.
#[async_trait]
pub trait Dap2 {
    /// Return a reference to a DAS structure for a data-source.
    async fn das(&self) -> &Das;

    /// Return a reference to a DDS structure for a data-source.
    async fn dds(&self) -> &Dds;

    /// Stream the bytes of the variable in `XDR` (_big-endian_) format.
    async fn variable(
        &self,
        variable: &dds::DdsVariableDetails,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Send + 'static>>,
        anyhow::Error,
    >;

    /// Stream the raw file (if supported). Should return a tuple with the content-length and a
    /// stream of [Bytes].
    async fn raw(
        &self,
    ) -> Result<
        (
            u64,
            Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static>>,
        ),
        std::io::Error,
    >;
}

#[async_trait]
impl<T: Send + Sync + Dap2> Dap2 for std::sync::Arc<T> {
    async fn das(&self) -> &Das {
        T::das(self).await
    }

    async fn dds(&self) -> &Dds {
        T::dds(self).await
    }

    async fn variable(
        &self,
        variable: &dds::DdsVariableDetails,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Send + 'static>>,
        anyhow::Error,
    > {
        T::variable(self, variable).await
    }

    async fn raw(
        &self,
    ) -> Result<
        (
            u64,
            Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static>>,
        ),
        std::io::Error,
    > {
        T::raw(self).await
    }
}
