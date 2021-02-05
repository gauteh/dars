use bytes::Bytes;
use std::mem;

use crate::dds::{DdsVariableDetails, VarType};

/// XDR encoded length.
pub fn xdr_length(len: u32) -> [u8; 8] {
    let len = len.to_be();
    let x: [u32; 2] = [len, len];

    unsafe { mem::transmute(x) }
}

/// Upcast 16-bit datatypes to 32-bit datatypes. Non 16-bit variables are passed through as-is.
///
/// The input bytes are assumed to be in native endianness.
pub fn xdr_serialize(v: &DdsVariableDetails, b: Bytes) -> Bytes {
    use VarType::*;

    // TODO: * Check performance of casting.
    //       * Move common code to templated function over Pod's
    //       * Use either byte-slice-cast or bytemuck.

    match v.vartype {
        UInt16 => {
            let b: &[u8] = &*b;
            let u: &[u16] = bytemuck::cast_slice(b);

            let mut n: Vec<u8> = Vec::with_capacity(u.len() * 4);
            unsafe {
                n.set_len(u.len() * 4);
            }
            let nn: &mut [u32] = bytemuck::cast_slice_mut(&mut n);

            for (s, d) in u.iter().zip(nn.iter_mut()) {
                if cfg!(target_endian = "big") {
                    *d = *s as u32;
                } else {
                    *d = (s.swap_bytes() as u32).swap_bytes();
                }
            }

            Bytes::from(n)
        }
        Int16 => {
            let b: &[u8] = &*b;
            let u: &[i16] = bytemuck::cast_slice(b);

            let mut n: Vec<u8> = Vec::with_capacity(u.len() * 4);
            unsafe {
                n.set_len(u.len() * 4);
            }
            let nn: &mut [i32] = bytemuck::cast_slice_mut(&mut n);

            for (s, d) in u.iter().zip(nn.iter_mut()) {
                if cfg!(target_endian = "big") {
                    *d = *s as i32;
                } else {
                    *d = (s.swap_bytes() as i32).swap_bytes();
                }
            }

            Bytes::from(n)
        },

        Float32 | UInt32 | Int32 |
        Float64 | UInt64 | Int64 => {
            unimplemented!("need to swap to big endinaness");
        },

        _ => {
            b
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn length() {
        let x: u32 = 2;
        let b = xdr_length(x);

        assert_eq!(b, [0u8, 0, 0, 2, 0, 0, 0, 2]);
    }
}
