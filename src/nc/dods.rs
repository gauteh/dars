use async_stream::stream;
use futures::stream::Stream;
use itertools::izip;
use std::cmp::{max, min};
use std::pin::Pin;
use std::sync::Arc;

use crate::dap2::dods::StreamingDataset;

impl StreamingDataset for Arc<netcdf::File> {
    fn get_var_size(&self, var: &str) -> Result<usize, anyhow::Error> {
        self.variable(var)
            .map(|v| v.dimensions().iter().map(|d| d.len()).product::<usize>())
            .ok_or_else(|| anyhow!("could not find variable"))
    }

    fn get_var_single_value(&self, var: &str) -> Result<bool, anyhow::Error> {
        self.variable(var)
            .map(|v| v.dimensions().is_empty())
            .ok_or_else(|| anyhow!("could not find variable: {}", var))
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
        trace!("streaming variable: {} ({:?}) ({:?})", vn, indices, counts);

        let vn = if let Some(i) = vn.find(".") {
            String::from(&vn[i + 1..])
        } else {
            String::from(vn)
        };

        const CHUNK_SZ: usize = 10 * 1024 * 1024;

        let f = self.clone();
        let v = f
            .variable(&vn)
            .expect(&format!("could not find variable: {}", vn));
        let counts: Vec<usize> = counts
            .map(|c| c.to_vec())
            .unwrap_or_else(|| v.dimensions().iter().map(|d| d.len()).collect());
        let indices: Vec<usize> = indices
            .map(|i| i.to_vec())
            .unwrap_or_else(|| vec![0usize; max(v.dimensions().len(), 1)]);

        Box::pin(stream! {
            let v = f.variable(&vn).expect(&format!("could not find variable: {}", vn));
            let mut jump: Vec<usize> = counts.iter().rev().scan(1, |n, &c| {
                if *n >= CHUNK_SZ {
                    Some(1)
                } else {
                    let p = min(CHUNK_SZ / *n, c);
                    *n *= p;

                    Some(p)
                }
            }).collect::<Vec<usize>>();
            jump.reverse();

            // size of count dimensions
            let mut dim_sz: Vec<usize> = counts.iter().rev().scan(1, |p, &c| {
                let sz = *p;
                *p *= c;
                Some(sz)
            }).collect();
            dim_sz.reverse();

            let mut offset = vec![0usize; counts.len()];

            loop {
                let mjump: Vec<usize> = izip!(&offset, &jump, &counts)
                    .map(|(o, j, c)| if o + j > *c { *c - *o } else { *j }).collect();
                let jump_sz: usize = mjump.iter().product();

                let mind: Vec<usize> = indices.iter().zip(&offset).map(|(a,b)| a + b).collect();

                let mut buf: Vec<T> = vec![T::default(); jump_sz];
                v.values_to(&mut buf, Some(&mind), Some(&mjump))?;

                yield Ok(buf);

                // let f = f.clone();
                // let mvn = vn.clone();
                // let cache = tokio::task::block_in_place(|| {
                //     let mut cache: Vec<T> = vec![T::default(); jump_sz];
                //     let v = f.variable(&mvn).ok_or(anyhow!("Could not find variable"))?;

                //     v.values_to(&mut cache, Some(&mind), Some(&mjump))?;
                //     Ok::<_,anyhow::Error>(cache)
                // })?;

                // yield Ok(cache);

                let mut carry = offset.iter().zip(&dim_sz).map(|(a,b)| a * b).sum::<usize>() + jump_sz;
                for (o, c) in izip!(offset.iter_mut().rev(), counts.iter().rev()) {
                    *o = carry % *c;
                    carry /= c;
                }

                if carry > 0 {
                    break;
                }
            }
        })
    }

    fn stream_encoded_variable(
        &self,
        v: &str,
        indices: Option<&[usize]>,
        counts: Option<&[usize]>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, anyhow::Error>> + Send + Sync + 'static>> {
        let vn = if let Some(i) = v.find(".") {
            String::from(&v[i + 1..])
        } else {
            String::from(v)
        };
        let vv = self
            .variable(&vn)
            .expect(&format!("could not find variable: {}", vn));
        match vv.vartype() {
            netcdf_sys::NC_FLOAT => self.stream_encoded_variable_impl::<f32>(&vn, indices, counts),
            netcdf_sys::NC_DOUBLE => self.stream_encoded_variable_impl::<f64>(&vn, indices, counts),
            netcdf_sys::NC_INT => self.stream_encoded_variable_impl::<i32>(&vn, indices, counts),
            netcdf_sys::NC_SHORT => self.stream_encoded_variable_impl::<i32>(&vn, indices, counts),
            netcdf_sys::NC_BYTE => self.stream_encoded_variable_impl::<u8>(&vn, indices, counts),
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[bench]
    fn open_nc(b: &mut Bencher) {
        b.iter(|| netcdf::open("data/coads_climatology.nc").unwrap());
    }

    #[bench]
    fn open_nc_native(b: &mut Bencher) {
        use std::fs::File;

        b.iter(|| {
            let f = File::open("data/coads_climatology.nc").unwrap();

            f
        });
    }

    #[bench]
    fn read_native_all(b: &mut Bencher) {
        b.iter(|| std::fs::read("data/coads_climatology.nc").unwrap());
    }

    #[bench]
    fn read_variable_preopen(b: &mut Bencher) {
        let f = netcdf::open("data/coads_climatology.nc").unwrap();
        b.iter(|| {
            let v = f.variable("SST").unwrap();

            let mut vbuf: Vec<f32> = vec![0.0; v.len()];
            v.values_to(&mut vbuf, None, None)
                .expect("could not read values");

            vbuf
        });
    }

    #[bench]
    fn read_variable(b: &mut Bencher) {
        b.iter(|| {
            let f = netcdf::open("data/coads_climatology.nc").unwrap();
            let v = f.variable("SST").unwrap();

            let mut vbuf: Vec<f32> = vec![0.0; v.len()];
            v.values_to(&mut vbuf, None, None).unwrap();

            vbuf
        });
    }

    #[bench]
    fn encoded_streaming_variable(b: &mut Bencher) {
        use futures::executor::block_on_stream;

        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        b.iter(|| {
            let f = f.clone();
            let v = f.stream_encoded_variable("SST", None, None);
            block_on_stream(v).collect::<Vec<_>>()
        });
    }

    #[bench]
    fn streaming_variable(b: &mut Bencher) {
        use futures::executor::block_on_stream;

        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        b.iter(|| {
            let f = f.clone();
            let v = f.stream_variable::<f32>("SST", None, None);
            block_on_stream(v).collect::<Vec<_>>()
        });
    }

    #[test]
    fn stream_variable_offset() {
        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        let counts = vec![10usize, 30, 80];

        let dir = {
            let v = f.variable("SST").unwrap();

            println!("{}", v.vartype() == netcdf_sys::NC_FLOAT);

            let mut vbuf: Vec<f32> = vec![0.0; counts.iter().product()];
            v.values_to(&mut vbuf, Some(&[1, 10, 10]), Some(&counts))
                .expect("could not read values");

            vbuf
        };

        let v = f.stream_variable::<f32>("SST", Some(&[1, 10, 10]), Some(&counts));

        let s: Vec<f32> = futures::executor::block_on_stream(v)
            .flatten()
            .flatten()
            .collect();

        assert_eq!(dir, s);
    }

    #[test]
    fn stream_variable_start_zero() {
        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        let counts = vec![10usize, 30, 80];

        let dir = {
            let v = f.variable("SST").unwrap();

            let mut vbuf: Vec<f32> = vec![0.0; counts.iter().product()];
            v.values_to(&mut vbuf, Some(&[0, 0, 0]), Some(&counts))
                .expect("could not read values");

            vbuf
        };

        let v = f.stream_variable::<f32>("SST", Some(&[0, 0, 0]), Some(&counts));

        let s: Vec<f32> = futures::executor::block_on_stream(v)
            .flatten()
            .flatten()
            .collect();
        assert_eq!(dir, s);
    }

    #[test]
    fn stream_variable_group_member() {
        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        let counts = vec![10usize, 30, 80];

        let dir = {
            let v = f.variable("SST").unwrap();

            let mut vbuf: Vec<f32> = vec![0.0; counts.iter().product()];
            v.values_to(&mut vbuf, Some(&[0, 0, 0]), Some(&counts))
                .expect("could not read values");

            vbuf
        };

        let v = f.stream_variable::<f32>("SST.SST", Some(&[0, 0, 0]), Some(&counts));

        let s: Vec<f32> = futures::executor::block_on_stream(v)
            .flatten()
            .flatten()
            .collect();
        assert_eq!(dir, s);
    }

    #[test]
    fn stream_encoded_variable_group_member() {
        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        let counts = vec![10usize, 30, 80];

        let v = f.stream_encoded_variable("SST.SST", Some(&[0, 0, 0]), Some(&counts));

        futures::executor::block_on_stream(v).for_each(drop);
    }

    #[test]
    fn stream_variable_read_all() {
        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        let counts: Vec<usize> = f
            .variable("SST")
            .unwrap()
            .dimensions()
            .iter()
            .map(|d| d.len())
            .collect();

        let dir = {
            let v = f.variable("SST").unwrap();

            let mut vbuf: Vec<f32> = vec![0.0; counts.iter().product()];
            v.values_to(&mut vbuf, Some(&[0, 0, 0]), Some(&counts))
                .expect("could not read values");

            vbuf
        };

        let v = f.stream_variable("SST", Some(&[0, 0, 0]), Some(&counts));

        let s: Vec<f32> = futures::executor::block_on_stream(v)
            .flatten()
            .flatten()
            .collect();

        assert_eq!(dir, s);
    }
}
