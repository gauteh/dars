use byte_slice_cast::IntoByteVec;
use byteorder::{BigEndian, ByteOrder};
use futures::stream::{self, Stream, StreamExt};
use std::pin::Pin;

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

pub trait StreamingDataset {
    /// Stream variable as chunks of values.
    fn stream_variable<T>(
        &self,
        variable: &str,
        indices: Option<&[usize]>,
        counts: Option<&[usize]>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<T>, anyhow::Error>> + Send + Sync + 'static>>
    where
        T: netcdf::Numeric + Unpin + Clone + std::default::Default + Send + Sync + 'static;

    /// Return size of variable (in elements), required by default implementation of
    /// `stream_encoded_variable`.
    fn get_var_size(&self, var: &str) -> Result<usize, anyhow::Error>;

    /// Return true if the variable does not have any dimensions and should be streamed
    /// without length.
    fn get_var_single_value(&self, var: &str) -> Result<bool, anyhow::Error>;

    /// This encodes a variable of the given type `T`. Call this from `stream_encoded_variable`
    /// after resolving the type.
    fn stream_encoded_variable_impl<T>(
        &self,
        variable: &str,
        indices: Option<&[usize]>,
        counts: Option<&[usize]>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, anyhow::Error>> + Send + Sync + 'static>>
    where
        T: netcdf::Numeric + Unpin + Clone + std::default::Default + Send + Sync + 'static,
        [T]: XdrPack,
        Vec<T>: IntoByteVec,
    {
        // TODO: if possible to return reference, use as_byte_slice() to avoid copy
        if self.get_var_single_value(variable).unwrap() {
            Box::pin(
                self.stream_variable::<T>(variable, indices, counts)
                    .map(|value| {
                        value.map(|mut value| {
                            ensure!(value.len() == 1, "value with more than one element");
                            value.pack();
                            Ok(value.into_byte_vec())
                        })?
                    }),
            )
        } else {
            let sz = counts
                .map(|c| c.iter().product::<usize>())
                .unwrap_or_else(|| self.get_var_size(variable).unwrap());

            Box::pin(
                stream::once(async move {
                    let mut sz = vec![sz as u32, sz as u32];
                    sz.pack();
                    Ok(sz.into_byte_vec())
                })
                .chain(self.stream_variable::<T>(variable, indices, counts).map(
                    |values| {
                        values.map(|mut values| {
                            values.pack();
                            values.into_byte_vec()
                        })
                    },
                )),
            )
        }
    }

    /// Stream variable as chunks of bytes encoded as XDR. Some datasets can return this directly,
    /// rather than first reading the variable.
    ///
    /// Use `stream_encoded_variable_impl` to implement this once the type of the variable
    /// is resolved.
    fn stream_encoded_variable(
        &self,
        variable: &str,
        indices: Option<&[usize]>,
        counts: Option<&[usize]>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, anyhow::Error>> + Send + Sync + 'static>>;
}
