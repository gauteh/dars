//! # Data response
//!
//! The DODS response consists of a [DDS](crate::dds) response followed by the string: `Data:\n\n` and the
//! `XDR` encoded data.
//!
//! ## XDR encoding
//!
//! [XDR encoded](https://en.wikipedia.org/wiki/External_Data_Representation) data are always
//! big-endian. The smallest size is 4 bytes, so data is padded to this. Strings or opaque types
//! are padded to be divisible by 4 bytes.
//!
//! ### Length
//!
//! Arrays are prepended with their XDR encoded length as `u32` _twice_. While scalars do not have
//! length prepended. A Structure or Grid is sent as each member sequentially.
//!
//!
//! ### XDR types
//!
//! * Byte
//! * Int16
//! * UInt16
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
use std::mem;

/// XDR encoded length.
pub fn xdr_length(len: u32) -> [u8; 8] {
    let len = len.to_be();
    let x: [u32; 2] = [len, len];

    unsafe { mem::transmute(x) }
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
