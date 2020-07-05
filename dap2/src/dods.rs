use std::mem;

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
