//! # Data response
//!
//! The DODS response consists of a [DDS](crate::dds) response followed by the string: `Data:\n\n` and the
//! `XDR` encoded data.
//!
//! ## XDR encoding
//!
//! [XDR encoded](https://en.wikipedia.org/wiki/External_Data_Representation) data are always
//! big-endian. The smallest size is 4 bytes, so data must be padded to this. Strings or opaque types
//! are padded to be divisible by 4 bytes.
//!
//! ### Length
//!
//! Arrays are prepended with their XDR encoded length as `u32` _twice_. While scalars do not. A
//! Structure or Grid is sent as each member sequentially.
//!
//!
//! ### XDR types
//!
//! * Byte   -> cast to ??
//! * Int16  -> cast to Int32
//! * UInt16 -> cast to UInt32
//! * Int32
//! * UInt32
//! * Float32
//! * Float64
//! * String
//! * URL
//!
//! See the [OPeNDAP documentation](https://docs.opendap.org/index.php?title=UserGuideDataModel#External_Data_Representation). This seems to deviate from the other XDR specification where each
//! type must be minimum 4 bytes.
//!
//! ### Strings
//!
//! Strings seem to be XDR encoded by first sending the length (as u32 big endian) of the number of
//! elements. Then each string is prepended with the string length of that element, then the string
//! is sent null-terminated.
use bytes::Bytes;
use futures::{pin_mut, Stream, StreamExt};
use std::pin::Pin;

use async_stream::stream;
use async_trait::async_trait;

pub mod xdr;
use xdr::xdr_length;

use crate::{dds::ConstrainedVariable, Constraint};

/// A `DODS` response streaming the [DDS](crate::dds::Dds) header and the (possibly) constrained
/// variable data.
#[async_trait]
pub trait Dods: crate::Dap2 + Send + Sync + Clone + 'static {
    /// A streamed DODS response based on [crate::Constraint] for a data source
    /// implementing [crate::Dap2].
    ///
    /// Returns a tuple with the content length (in bytes) and a stream of [Bytes].
    async fn dods(
        &self,
        constraint: Constraint,
    ) -> Result<
        (
            u64,
            Pin<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Send + 'static>>,
        ),
        anyhow::Error,
    > {
        let dds = self.dds().await.dds(&constraint)?;
        let dds_bytes = Bytes::from(dds.to_string());
        let content_length = (dds.dods_size() + dds_bytes.len() + 8) as u64;
        debug!(
            "dods length: {} b / {} mb",
            content_length,
            content_length / 1024 / 1024
        );

        let slf = self.clone();

        Ok((content_length, stream! {
            yield Ok::<_, anyhow::Error>(dds_bytes);
            yield Ok(Bytes::from_static(b"\n\nData:\n"));

            for c in dds.variables {
                match c {
                    ConstrainedVariable::Variable(v) |
                        ConstrainedVariable::Structure { variable: _, member: v }
                    => {
                        if !v.is_scalar() {
                            yield Ok(Bytes::from(Vec::from(xdr_length(v.len() as u32))));
                        }

                        let reader = slf.variable_xdr(&v).await?;

                        pin_mut!(reader);

                        while let Some(b) = reader.next().await {
                            yield b;
                        }
                    },
                    ConstrainedVariable::Grid {
                        variable,
                        dimensions,
                    } => {
                        for variable in std::iter::once(variable).chain(dimensions) {
                            if !variable.is_scalar() {
                                yield Ok(Bytes::from(Vec::from(xdr_length(variable.len() as u32))));
                            }

                            let reader = slf.variable_xdr(&variable).await?;

                            pin_mut!(reader);

                            while let Some(b) = reader.next().await {
                                yield b;
                            }
                        }
                    }
                }
            }
        }.boxed()))
    }
}

impl<T: crate::Dap2 + Send + Sync + Clone + 'static> Dods for T {}
