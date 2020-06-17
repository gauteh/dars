///! The DAP/2 data response.
///
/// The DODS response consists of a DDS response, possibly constrained. Then followed by XDR
/// encoded arrays or the value.
///
/// Arrays are preceeded by the XDR-encoded length as an u32 _repeated twice_, while single values
/// are sent without header.
use futures::stream::{self, TryStreamExt};
use futures::{AsyncBufRead, AsyncReadExt};
use std::pin::Pin;

use async_trait::async_trait;
use byte_slice_cast::IntoByteVec;

use crate::dds::DdsVariableDetails;
use super::xdr::*;

pub trait Reader = Send + Sync + Unpin + AsyncBufRead + 'static;

pub enum DodsVariable {
    Value(Pin<Box<dyn Reader>>),

    /// size and reader. size is number of elements reader will output.
    Array(usize, Pin<Box<dyn Reader>>),
}

#[async_trait]
pub trait Dods {
    /// The XDR bytes of a variable. Big-endian (network-endian) IEEE encoded.
    async fn variable(&self, variable: &DdsVariableDetails) -> Result<DodsVariable, anyhow::Error>;
}

impl DodsVariable {
    /// Consumes variable and returns a reader with the XDR header and the XDR data.
    pub fn as_reader(self) -> Pin<Box<dyn Reader>> {
        match self {
            DodsVariable::Value(reader) => reader,
            DodsVariable::Array(len, reader) => {
                let mut length = vec![len as u32, len as u32];
                length.pack();
                let length = length.into_byte_vec();

                // All this stuff to store the length value in the async task.
                Box::pin(
                    Box::pin(stream::once(async move { Ok(length) }))
                        .into_async_read()
                        .chain(reader),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::io::{AsyncReadExt, Cursor};

    #[test]
    fn read_array() {
        block_on(async {
            let reader =
                DodsVariable::Array(8, Box::pin(Cursor::new(vec![1u8, 2, 3, 4, 5, 6, 7, 8])));
            let mut output = Vec::new();
            reader.as_reader().read_to_end(&mut output).await.unwrap();
            assert_eq!(output, vec![0, 0, 0, 8, 0, 0, 0, 8, 1, 2, 3, 4, 5, 6, 7, 8]);
        });
    }

    #[test]
    fn read_value() {
        block_on(async {
            let reader = DodsVariable::Value(Box::pin(Cursor::new(vec![1u8, 2, 3, 4, 5, 6, 7, 8])));
            let mut output = Vec::new();
            reader.as_reader().read_to_end(&mut output).await.unwrap();
            assert_eq!(output, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        });
    }
}
