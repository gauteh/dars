/// Hyperslabs
///
/// OPeNDAP constraint expressions for ranges can consist of:
/// - single values:            [1]     -> [1]
/// - a range:                  [1:5]   -> [1, 2, 3, 4, 5]
/// - a range with strides:     [1:2:7] -> [1, 3, 5, 7]
///                             [1:2:8] -> [1, 3, 5, 7]
pub mod hyperslab {
    pub fn count_slab(slab: &Vec<usize>) -> usize {
        if slab.len() == 1 {
            1
        } else if slab.len() == 2 {
            slab[1] - slab[0] + 1
        } else if slab.len() == 3 {
            (slab[2] - slab[0] + 1) / slab[1]
        } else {
            panic!("too much slabs");
        }
    }

    fn parse_slice(s: &str) -> anyhow::Result<Vec<usize>> {
        match s.split(":").map(|h| h.parse::<usize>())
            .collect::<Result<Vec<usize>,_>>()
            .map_err(|_| anyhow!("Failed to parse index")) {
                Ok(v) => match v.len() {
                    l if l <= 3 => Ok(v),
                    _ => Err(anyhow!("Too many values to unpack."))
                },
                e => e
            }
    }

    pub fn parse_hyberslab(s: &str) -> anyhow::Result<Vec<Vec<usize>>> {
        if s.len() < 3 || !s.starts_with("[") || !s.ends_with("]") {
            return Err(anyhow!("Hyberslab missing brackets"));
        }

        s.split("]")
            .filter(|slab| slab.len() != 0)
            .map(|slab| {
                if slab.starts_with("[") {
                    parse_slice(&slab[1..])
                } else {
                    return Err(anyhow!("Missing start bracket"));
                }
            }).collect()
    }
}

pub mod xdr {
    pub trait XdrSize {
        fn size() -> usize;
    }

    impl XdrSize for i8 {
        fn size() -> usize { 1 }
    }

    impl XdrSize for u8 {
        fn size() -> usize { 1 }
    }

    impl XdrSize for u32 {
        fn size() -> usize { 4 }
    }

    impl XdrSize for i32 {
        fn size() -> usize { 4 }
    }

    impl XdrSize for f32 {
        fn size() -> usize { 4 }
    }

    impl XdrSize for f64 {
        fn size() -> usize { 8 }
    }

    pub fn pack_xdr_val<T>(v: Vec<T>) -> Result<Vec<u8>, anyhow::Error>
        where T: xdr_codec::Pack<std::io::Cursor<Vec<u8>>> + Sized + XdrSize
    {
        use std::io::Cursor;

        ensure!(v.len() == 1, "value with more than one element");

        let sz: usize = <T as XdrSize>::size();
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::with_capacity(sz));
        v[0].pack(&mut buf)?;
        Ok(buf.into_inner())
    }

    pub fn pack_xdr_arr<T>(v: Vec<T>, start: bool, len: Option<usize>) -> Result<Vec<u8>, anyhow::Error>
        where T: xdr_codec::Pack<std::io::Cursor<Vec<u8>>> + Sized + XdrSize
    {
        use std::io::Cursor;
        use xdr_codec::Pack;


        let sz: usize = 2*4 + v.len()*<T as XdrSize>::size();
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::with_capacity(sz));

        debug!("packarr: start: {:?}, len: {:?}, xdrsz: {}, sz: {}", start, len, <T as XdrSize>::size(), sz);

        if start {
            if let Some(len) = len {
                len.pack(&mut buf)?;
                len.pack(&mut buf)?;
            } else {
                v.len().pack(&mut buf)?;
                v.len().pack(&mut buf)?;
            }
        }

        for val in v {
            val.pack(&mut buf)?;
        }

        Ok(buf.into_inner())
    }
}

#[cfg(test)]
mod tests {
    mod hyperslab {
        use super::super::hyperslab::*;

        #[test]
        fn test_hyperslab() {
            assert_eq!(parse_hyberslab("[0:30]").unwrap(), [[0, 30]]);
        }

        #[test]
        fn test_stride() {
            assert_eq!(parse_hyberslab("[0:2:30]").unwrap(), [[0, 2, 30]]);
        }

        #[test]
        fn too_many_values() {
            assert!(parse_hyberslab("[0:3:4:40]").is_err());
        }

        #[test]
        fn too_wrong_indx() {
            assert!(parse_hyberslab("[0:a:40]").is_err());
        }

        #[test]
        fn test_multidim() {
            assert_eq!(parse_hyberslab("[0][1]").unwrap(), [[0], [1]]);
        }

        #[test]
        fn test_multidim_slice() {
            assert_eq!(parse_hyberslab("[0:30][1][0:1200]").unwrap(), vec!(vec![0, 30], vec![1], vec![0, 1200]));
        }
    }
}
