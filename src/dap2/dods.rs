use async_stream::stream;
use byte_slice_cast::IntoByteVec;
use byteorder::{BigEndian, ByteOrder};
use futures::pin_mut;
use futures::stream::{Stream, StreamExt};

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

/// Pack a single value.
pub fn encode_value<T>(mut v: Vec<T>) -> Result<Vec<u8>, anyhow::Error>
where
    [T]: XdrPack,
    Vec<T>: IntoByteVec,
{
    ensure!(v.len() == 1, "value with more than one element");

    v.pack();
    Ok(v.into_byte_vec()) // TODO: if possible to return reference, use as_byte_slice() to avoid copy
}

/// Encodes a chunked stream of Vec<T> as XDR array into a new chunked
/// stream of Vec<u8>'s.
///
/// Use if variable has dimensions.
pub fn encode_array<S, T>(
    v: S,
    len: Option<usize>,
) -> impl Stream<Item = Result<Vec<u8>, anyhow::Error>>
where
    S: Stream<Item = Result<Vec<T>, anyhow::Error>>,
    [T]: XdrPack,
    Vec<T>: IntoByteVec,
{
    stream! {
        pin_mut!(v);

        if let Some(sz) = len {
            if sz > std::u32::MAX as usize {
                yield Err(anyhow!("XDR cannot send slices larger than {}", std::u32::MAX));
            }

            let mut val = vec![sz as u32, sz as u32];
            val.pack();
            yield Ok(val.into_byte_vec());
        }

        while let Some(mut val) = v.next().await {
            match val {
                Ok(mut val) => {
                    val.pack();
                    yield Ok(val.into_byte_vec())
                },
                Err(e) => yield Err(e)
            };
        }
    }
}
