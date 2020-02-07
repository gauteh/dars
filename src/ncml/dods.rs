use futures::stream::{self, Stream, StreamExt};
use itertools::izip;
use std::cmp::{max, min};
use std::pin::Pin;
use std::sync::Arc;

use super::NcmlDataset;
use crate::dap2::dods::StreamingDataset;

impl StreamingDataset for NcmlDataset {
    fn get_var_size(&self, var: &str) -> Result<usize, anyhow::Error> {
        self.members[0]
            .f
            .variable(var)
            .map(|v| {
                v.dimensions()
                    .iter()
                    .map(|d| {
                        if d.name() == self.aggregation_dim {
                            self.dim_n
                        } else {
                            d.len()
                        }
                    })
                    .product::<usize>()
            })
            .ok_or_else(|| anyhow!("could not find variable"))
    }

    fn get_var_single_value(&self, var: &str) -> Result<bool, anyhow::Error> {
        self.members[0]
            .f
            .variable(var)
            .map(|v| v.dimensions().is_empty())
            .ok_or_else(|| anyhow!("could not find variable"))
    }

    fn stream_encoded_variable(
        &self,
        v: &str,
        indices: Option<&[usize]>,
        counts: Option<&[usize]>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, anyhow::Error>> + Send + Sync + 'static>> {
        let vv = self.members[0].f.variable(&v).unwrap();
        match vv.vartype() {
            netcdf_sys::NC_FLOAT => self.stream_encoded_variable_impl::<f32>(v, indices, counts),
            netcdf_sys::NC_DOUBLE => self.stream_encoded_variable_impl::<f64>(v, indices, counts),
            netcdf_sys::NC_INT => self.stream_encoded_variable_impl::<i32>(v, indices, counts),
            netcdf_sys::NC_SHORT => self.stream_encoded_variable_impl::<i32>(v, indices, counts),
            netcdf_sys::NC_BYTE => self.stream_encoded_variable_impl::<u8>(v, indices, counts),
            _ => unimplemented!(),
        }
    }

    /// Stream a variable with a predefined chunk size. Chunk size is not guaranteed to be
    /// kept, and may be at worst half of specified size in order to fill up slabs.
    fn stream_variable<T>(
        &self,
        vn: &str,
        indices: Option<&[usize]>,
        counts: Option<&[usize]>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<T>, anyhow::Error>> + Send + Sync + 'static>>
    where
        T: netcdf::Numeric + Unpin + Clone + std::default::Default + Send + Sync + 'static,
    {
        let fnc = self.members[0].f.clone();

        // lengths of coordinate axis
        let ns = self.members.iter().map(|m| m.n).collect::<Vec<usize>>();

        // start index of each member
        let ss = ns
            .iter()
            .scan(0, |acc, &n| {
                let c = *acc;
                *acc += n;
                Some(c)
            })
            .collect::<Vec<usize>>();

        // clones of file descriptors
        let fs = self
            .members
            .iter()
            .map(|m| m.f.clone())
            .collect::<Vec<Arc<netcdf::File>>>();

        let vv = fnc.variable(vn).unwrap();

        let indices: Vec<usize> = indices
            .map(|i| i.to_vec())
            .unwrap_or_else(|| vec![0usize; max(vv.dimensions().len(), 1)]);

        let counts: Vec<usize> = counts.map(|c| c.to_vec()).unwrap_or_else(|| {
            vv.dimensions()
                .iter()
                .map(|d| {
                    if d.name() == self.aggregation_dim {
                        self.dim_n
                    } else {
                        d.len()
                    }
                })
                .collect()
        });

        if vv.dimensions().len() > 0 && vv.dimensions()[0].name() == self.aggregation_dim {
            if indices[0] + counts[0] > self.dim_n {
                panic!("slab too great");
                // stream::once(async { Err::<Vec<T>, _>(anyhow!("slab too great")) });
            }

            let agg_sz = counts.iter().product::<usize>();

            trace!(
                "Indices: {:?}, counts: {:?}, agg_sz = {}",
                indices,
                counts,
                agg_sz
            );

            enum S<TT> {
                Segment(
                    Pin<
                        Box<
                            dyn Stream<Item = Result<Vec<TT>, anyhow::Error>>
                                + Send
                                + Sync
                                + 'static,
                        >,
                    >,
                ),
                Empty,
                Stop,
            };

            let vn = String::from(vn);
            let streams: Vec<
                Pin<Box<dyn Stream<Item = Result<Vec<T>, anyhow::Error>> + Send + Sync + 'static>>,
            > = izip!(&ss, &ns, &fs)
                .map(|(s, n, f)| {
                    trace!("testing: {}, {} against {} {}", s, n, indices[0], counts[0]);
                    if indices[0] >= *s && indices[0] < (s + n) {
                        // pack start (incl len x 2)
                        let mut mindices = indices.clone();
                        mindices[0] = indices[0] - s;

                        let mut mcounts = counts.clone();
                        mcounts[0] = min(counts[0], *n - mindices[0]);

                        trace!(
                            "First file at {} to {} (i = {:?}, c = {:?})",
                            s,
                            s + n,
                            mindices,
                            mcounts
                        );

                        S::Segment(f.stream_variable(&vn, Some(&mindices), Some(&mcounts)))
                    } else if indices[0] < *s && (*s < indices[0] + counts[0]) {
                        let mut mcounts = counts.clone();
                        mcounts[0] = min(indices[0] + counts[0] - *s, *n);

                        let mut mindices = indices.clone();
                        mindices[0] = 0;

                        trace!(
                            "Consecutive file at {} to {} (i = {:?}, c = {:?})",
                            s,
                            s + n,
                            mindices,
                            mcounts
                        );

                        S::Segment(f.stream_variable(&vn, Some(&mindices), Some(&mcounts)))
                    } else if indices[0] + counts[0] < *s {
                        S::Stop
                    } else {
                        S::Empty
                    }
                })
                .filter(|s| if let S::<T>::Empty = s { false } else { true })
                .take_while(|s| if let S::<T>::Stop = s { false } else { true })
                .map(|s| {
                    if let S::Segment(s) = s {
                        s
                    } else {
                        panic!("weird..")
                    }
                })
                .collect();

            Box::pin(stream::iter(streams).flatten())
        } else {
            trace!(
                "Non aggregated variable, i = {:?}, c = {:?}",
                indices,
                counts
            );
            fnc.stream_variable(vn, Some(&indices), Some(&counts))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on_stream;
    use std::io::Cursor;

    #[test]
    fn ncml_xdr_time_dim() {
        let nm = NcmlDataset::open("data/ncml/aggExisting.ncml", false).unwrap();
        let t = nm.stream_encoded_variable("time", None, None);
        let bs: Vec<u8> = block_on_stream(t)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .flatten()
            .skip(4)
            .map(|b| b.clone())
            .collect();

        println!("len: {}", bs.len() / 4);
        let n: usize = (bs.len() / 4) - 1;

        println!("transmitted length: {:?}", &bs[0..4]);
        assert_eq!(n, 31 + 28);

        let mut time = Cursor::new(&bs[4..]);

        let mut buf: Vec<i32> = vec![0; n];
        let sz = xdr_codec::unpack_array(&mut time, &mut buf, n, None).unwrap();
        println!("deserialized time (sz: {}): {:?}", sz, buf);

        assert_eq!(sz, (31 + 28) * 4);

        let jan = netcdf::open("data/ncml/jan.nc").unwrap();
        let jt = jan
            .variable("time")
            .unwrap()
            .values::<i32>(None, None)
            .unwrap();

        assert!(&buf[0..31] == jt.as_slice().unwrap());

        let feb = netcdf::open("data/ncml/feb.nc").unwrap();
        let ft = feb
            .variable("time")
            .unwrap()
            .values::<i32>(None, None)
            .unwrap();

        assert!(&buf[31..] == ft.as_slice().unwrap());
    }

    #[test]
    fn ncml_xdr_temp() {
        let nm = NcmlDataset::open("data/ncml/aggExisting.ncml", false).unwrap();
        let t = nm.stream_encoded_variable("T", None, None);
        let bs: Vec<u8> = block_on_stream(t)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .flatten()
            .skip(4)
            .map(|b| b.clone())
            .collect();

        println!("len: {}", bs.len() / 8);
        let n: usize = bs.len() / 8;

        println!("transmitted length: {:?}", &bs[0..4]);
        assert_eq!(n, 3 * 4 * (31 + 28));

        let mut temp = Cursor::new(&bs[4..]);

        let mut buf: Vec<f64> = vec![0.0; n];
        let sz = xdr_codec::unpack_array(&mut temp, &mut buf, n, None).unwrap();

        assert_eq!(sz, (31 + 28) * 3 * 4 * 8);

        let jan = netcdf::open("data/ncml/jan.nc").unwrap();
        let jt = jan
            .variable("T")
            .unwrap()
            .values::<f64>(None, None)
            .unwrap();

        assert!(&buf[0..(31 * 3 * 4)] == jt.as_slice().unwrap());

        let feb = netcdf::open("data/ncml/feb.nc").unwrap();
        let ft = feb
            .variable("T")
            .unwrap()
            .values::<f64>(None, None)
            .unwrap();

        assert!(&buf[(31 * 3 * 4)..] == ft.as_slice().unwrap());
    }

    #[test]
    fn span_time() {
        crate::testcommon::init();
        let nm = NcmlDataset::open("data/ncml/scan.ncml", false).unwrap();

        let t = nm.stream_encoded_variable("T", Some(&[0, 0, 0]), Some(&[51, 1, 1]));
        let bs: Vec<u8> = block_on_stream(t)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .flatten()
            .skip(4)
            .map(|b| b.clone())
            .collect();
        assert!(bs.len() == 4 + 51 * 8);

        let t = nm.stream_encoded_variable("T", Some(&[20, 0, 0]), Some(&[31, 1, 1]));
        let bs: Vec<u8> = block_on_stream(t)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .flatten()
            .skip(4)
            .map(|b| b.clone())
            .collect();
        assert!(bs.len() == 4 + 31 * 8);

        // files are spliced at 31:32
        let t = nm.stream_encoded_variable("T", Some(&[31, 0, 0]), Some(&[2, 1, 1]));
        let bs: Vec<u8> = block_on_stream(t)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .flatten()
            .skip(4)
            .map(|b| b.clone())
            .collect();
        assert!(bs.len() == 4 + 2 * 8);
    }
}
