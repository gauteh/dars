use std::sync::Arc;
use itertools::izip;
use std::cmp::min;
use futures::stream::Stream;
use async_stream::stream;

use crate::dap2::{xdr, hyperslab::{count_slab, parse_hyberslab}};

pub fn stream_variable<T>(f: Arc<netcdf::File>, vn: String, indices: Vec<usize>, counts: Vec<usize>) -> impl Stream<Item=Result<Vec<T>, anyhow::Error>>
    where T: netcdf::Numeric + Unpin + Clone + Default + std::fmt::Debug
{
    const CHUNK_SZ: usize = 1024;

    stream! {
        let v = f.variable(&vn).ok_or(anyhow!("Could not find variable"))?;

        let mut jump: Vec<usize> = counts.iter().rev().scan(1, |n, &c| {
            if *n >= CHUNK_SZ {
                Some(0)
            } else {
                let p = min(CHUNK_SZ / *n, c);
                *n = *n * p;

                Some(p)
            }
        }).collect::<Vec<usize>>();
        jump.reverse();

        // size of count dimensions
        let mut dim_sz: Vec<usize> = counts.iter().rev().scan(1, |p, &c| {
            let sz = *p;
            *p = *p * c;
            Some(sz)
        }).collect();
        dim_sz.reverse();

        let mut offset = vec![0usize; counts.len()];

        loop {
            let mjump: Vec<usize> = izip!(&offset, &jump, &counts)
                .map(|(o, j, c)| if o + j > *c { *c - *o } else { *j }).collect();
            let jump_sz: usize = mjump.iter().product();

            let mind: Vec<usize> = indices.iter().zip(&offset).map(|(a,b)| a + b).collect();

            let mut cache: Vec<T> = vec![T::default(); jump_sz];
            v.values_to(&mut cache, Some(&mind), Some(&mjump))?;


            yield Ok(cache);


            let mut carry = offset.iter().zip(&dim_sz).map(|(a,b)| a * b).sum::<usize>() + jump_sz;
            for (o, c) in izip!(offset.iter_mut().rev(), counts.iter().rev()) {
                *o = carry % *c;
                carry = carry / c;
            }

            if carry > 0 {
                break;
            }
        }
    }
}


// TODO: Try tokio::codec::FramedRead with Read impl on dods?

pub fn pack_var(v: &netcdf::Variable, start: bool, len: Option<usize>, slab: Option<(Vec<usize>, Vec<usize>)>) -> Result<Vec<u8>, anyhow::Error> {
    match v.vartype() {
        netcdf_sys::NC_FLOAT => xdr_chunk::<f32>(v, start, len, slab),
        netcdf_sys::NC_DOUBLE => xdr_chunk::<f64>(v, start, len, slab),
        netcdf_sys::NC_INT => xdr_chunk::<i32>(v, start, len, slab),
        netcdf_sys::NC_SHORT => xdr_chunk::<i32>(v, start, len, slab),
        netcdf_sys::NC_BYTE => xdr_chunk::<u8>(v, start, len, slab),
        // netcdf_sys::NC_UBYTE => xdr_bytes(vv),
        // netcdf_sys::NC_CHAR => xdr_bytes(vv),
        _ => unimplemented!()
    }
}

pub fn xdr_chunk<T>(v: &netcdf::Variable, start: bool, len: Option<usize>, slab: Option<(Vec<usize>, Vec<usize>)>) -> Result<Vec<u8>, anyhow::Error>
    where T: netcdf::variable::Numeric +
                xdr_codec::Pack<std::io::Cursor<Vec<u8>>> +
                Sized +
                xdr::XdrSize +
                std::default::Default +
                std::clone::Clone
{
    let n = match &slab {
        Some((_, c)) => c.iter().product::<usize>(),
        None => v.len()
    };

    if n > v.len() {
        warn!("slab too great");
        Err(anyhow!("slab too great {} > {}", n, v.len()))?;
    }

    let mut vbuf: Vec<T> = vec![T::default(); n];

    match slab {
        Some((indices, counts)) => v.values_to(&mut vbuf, Some(&indices), Some(&counts)),
        None => v.values_to(&mut vbuf, None, None)
    }?;

    if v.dimensions().len() > 0 {
        xdr::pack_xdr_arr(vbuf, start, len)
    } else {
        xdr::pack_xdr_val(vbuf)
    }
}

pub fn xdr(nc: Arc<netcdf::File>, vs: Vec<String>) -> impl Stream<Item = Result<Vec<u8>, anyhow::Error>> {
    stream! {
        for v in vs {
            // TODO: Structures not supported, only single variables.

            let mut mv = match v.find(".") {
                Some(i) => &v[i+1..],
                None => &v
            };

            let slab = match mv.find("[") {
                Some(i) => {
                    let slab = parse_hyberslab(&mv[i..])?;
                    mv = &mv[..i];

                    let counts = slab.iter().map(count_slab).collect::<Vec<usize>>();
                    let indices = slab.iter().map(|slab| slab[0]).collect::<Vec<usize>>();

                    if slab.iter().any(|s| s.len() > 2) {
                        yield Err(anyhow!("Strides not implemented yet"));
                    }

                    Some((indices, counts))
                },

                None => None
            };

            let vv = nc.variable(&mv).ok_or(anyhow!("variable not found"))?;

            // TODO, IMPORTANT: loop over chunks of max. size. It is possible to generate a request
            // with a very large slab. Causing a large amount of memory to be allocated. The
            // variable should be chunked and streamed in e.g. 1MB sizes.
            yield pack_var(vv, true, None, slab)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[bench]
    fn open_nc(b: &mut Bencher) {
        b.iter(|| { netcdf::open("data/coads_climatology.nc").unwrap() });
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
        b.iter(|| {
            std::fs::read("data/coads_climatology.nc").unwrap()
        });
    }

    #[bench]
    fn read_var_preopen(b: &mut Bencher) {
        let f = netcdf::open("data/coads_climatology.nc").unwrap();
        b.iter(|| {
            let v = f.variable("SST").unwrap();

            let mut vbuf: Vec<f32> = vec![0.0; v.len()];
            v.values_to(&mut vbuf, None, None).expect("could not read values");

            vbuf
        });
    }

    #[bench]
    fn read_var(b: &mut Bencher) {
        b.iter(|| {
            let f = netcdf::open("data/coads_climatology.nc").unwrap();
            let v = f.variable("SST").unwrap();

            let mut vbuf: Vec<f32> = vec![0.0; v.len()];
            v.values_to(&mut vbuf, None, None).unwrap();

            vbuf
        });
    }

    #[bench]
    fn xdr_stream(b: &mut Bencher) {
        use futures::pin_mut;
        use futures::executor::block_on_stream;

        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        b.iter(|| {
            let f = f.clone();
            let v = xdr(
                f,
                vec![ "SST".to_string() ]);

            pin_mut!(v);
            block_on_stream(v).collect::<Vec<_>>()
        });
    }

    #[test]
    fn test_async_read_start_offset() {
        use futures::pin_mut;
        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        let counts = vec![10usize, 30, 80];

        let dir = {
            let v = f.variable("SST").unwrap();

            println!("{}", v.vartype() == netcdf_sys::NC_FLOAT);

            let mut vbuf: Vec<f32> = vec![0.0; counts.iter().product()];
            v.values_to(&mut vbuf, Some(&[1, 10, 10]), Some(&counts)).expect("could not read values");

            vbuf
        };

        let v = stream_variable::<f32>(f, "SST".to_string(), vec![1,10,10], counts.clone());
        pin_mut!(v);

        let s = futures::executor::block_on_stream(v).flatten().collect::<Vec<f32>>();

        assert_eq!(dir, s);
    }

    #[test]
    fn test_async_read_start_zero() {
        use futures::pin_mut;
        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        let counts = vec![10usize, 30, 80];

        let dir = {
            let v = f.variable("SST").unwrap();

            let mut vbuf: Vec<f32> = vec![0.0; counts.iter().product()];
            v.values_to(&mut vbuf, Some(&[0, 0, 0]), Some(&counts)).expect("could not read values");

            vbuf
        };

        let v = stream_variable::<f32>(f, "SST".to_string(), vec![0, 0, 0], counts.clone());
        pin_mut!(v);

        let s = futures::executor::block_on_stream(v).flatten().collect::<Vec<f32>>();
        assert_eq!(dir, s);
    }

    #[test]
    fn test_async_read_all() {
        use futures::pin_mut;
        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());

        let counts: Vec<usize> = f.variable("SST").unwrap().dimensions().iter().map(|d| d.len()).collect();

        let dir = {
            let v = f.variable("SST").unwrap();

            let mut vbuf: Vec<f32> = vec![0.0; counts.iter().product()];
            v.values_to(&mut vbuf, Some(&[0, 0, 0]), Some(&counts)).expect("could not read values");

            vbuf
        };

        let v = stream_variable::<f32>(f, "SST".to_string(), vec![0, 0, 0], counts.clone());
        pin_mut!(v);

        let s = futures::executor::block_on_stream(v).flatten().collect::<Vec<f32>>();

        assert_eq!(dir, s);
    }
}
