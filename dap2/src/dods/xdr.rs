use byteorder::{BigEndian, ByteOrder};

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


