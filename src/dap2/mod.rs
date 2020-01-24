/// Hyperslabs
///
/// OPeNDAP constraint expressions for ranges can consist of:
/// - single values:            [1]     -> [1]
/// - a range:                  [1:5]   -> [1, 2, 3, 4, 5]
/// - a range with strides:     [1:2:7] -> [1, 3, 5, 7]
///                             [1:2:8] -> [1, 3, 5, 7]
pub mod hyperslab {
    pub fn count_slab(slab: &[usize]) -> usize {
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
        match s
            .split(':')
            .map(|h| h.parse::<usize>())
            .collect::<Result<Vec<usize>, _>>()
            .map_err(|_| anyhow!("Failed to parse index"))
        {
            Ok(v) => match v.len() {
                l if l <= 3 => Ok(v),
                _ => Err(anyhow!("Too many values to unpack.")),
            },
            e => e,
        }
    }

    pub fn parse_hyberslab(s: &str) -> anyhow::Result<Vec<Vec<usize>>> {
        if s.len() < 3 || !s.starts_with('[') || !s.ends_with(']') {
            return Err(anyhow!("Hyberslab missing brackets"));
        }

        s.split(']')
            .filter(|slab| !slab.is_empty())
            .map(|slab| {
                if slab.starts_with('[') {
                    parse_slice(&slab[1..])
                } else {
                    Err(anyhow!("Missing start bracket"))
                }
            })
            .collect()
    }
}

pub mod xdr {
    use async_stream::stream;
    use byte_slice_cast::IntoByteVec;
    use byteorder::{BigEndian, ByteOrder};
    use futures::pin_mut;
    use futures::stream::{Stream, StreamExt};

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

    pub fn pack_xdr_val<T>(mut v: Vec<T>) -> Result<Vec<u8>, anyhow::Error>
    where
        [T]: XdrPack,
        Vec<T>: IntoByteVec,
    {
        ensure!(v.len() == 1, "value with more than one element");

        v.pack();
        Ok(v.into_byte_vec())
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

                        // if cfg!(not(test)) {
                        //     tokio::task::block_in_place(|| {
                        //         for v in val {
                        //             v.pack(&mut buf).unwrap();
                        //         }
                        //     });
                        // } else {
                        // use rayon::prelude::*;
                        // val.par_iter()
                        //     .map(|v|
                        //         let mut buf: Cursor<Vec< = Cursor::new(Vec::w
                        //         v.pack(&mut buf).unwrap());
                        // for v in (&val).into_par_iter() {
                        //     v.pack(&mut buf).unwrap();
                        // }
                        // }

                        // yield Ok(buf.into_inner())
                    },
                    Err(e) => yield Err(e)
                };
            }
        }
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
            assert_eq!(
                parse_hyberslab("[0:30][1][0:1200]").unwrap(),
                vec!(vec![0, 30], vec![1], vec![0, 1200])
            );
        }
    }
}
