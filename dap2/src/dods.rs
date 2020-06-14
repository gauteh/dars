///! The DAP/2 data response.
///
/// The DODS response consists of a DDS response, possibly constrained. Then followed by XDR
/// encoded arrays or the value.
///
/// Arrays are preceeded by the XDR-encoded length as an u32 _repeated twice_, while single values
/// are sent without header.
use async_trait::async_trait;
use futures::stream::TryStreamExt;
use futures::{AsyncBufRead, AsyncReadExt};

use byte_slice_cast::IntoByteVec;
use byteorder::{BigEndian, ByteOrder};

pub trait Reader = Send + Sync + Unpin + AsyncBufRead + 'static;

pub enum DodsVariable {
    Value(Box<dyn Reader>),
    Array(usize, Box<dyn Reader>),
}

#[async_trait]
pub trait Dods {
    /// The XDR bytes of a variable. Big-endian (network-endian) IEEE encoded.
    async fn variable(&self, variable: &str, slab: Option<&[usize]>) -> DodsVariable;
}

impl DodsVariable {
    /// Consumes variable and returns a reader with the XDR header and the XDR data.
    pub fn reader(self) -> Box<dyn Reader> {
        match self {
            DodsVariable::Value(reader) => reader,
            DodsVariable::Array(len, reader) => {
                let mut length = vec![len as u32, len as u32];
                length.pack();
                let length = length.into_byte_vec();

                // All this stuff to store the length value in the async task.
                Box::new(
                    Box::pin(futures::stream::once(async move { Ok(length) }))
                        .into_async_read()
                        .chain(reader),
                )
            }
        }
    }
}

/// The XdrPack trait defines how a type can be serialized to
/// XDR.
pub trait XdrPack {
    fn pack(&mut self);
}

impl XdrPack for [u8] {
    fn pack(&mut self) {}
}

impl XdrPack for [i32] {
    fn pack(&mut self) {
        BigEndian::from_slice_i32(self);
    }
}

impl XdrPack for [f32] {
    fn pack(&mut self) {
        BigEndian::from_slice_f32(self);
    }
}

impl XdrPack for [f64] {
    fn pack(&mut self) {
        BigEndian::from_slice_f64(self);
    }
}

impl XdrPack for [u32] {
    fn pack(&mut self) {
        BigEndian::from_slice_u32(self);
    }
}
