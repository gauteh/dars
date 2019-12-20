use std::sync::Arc;
use std::pin::Pin;
use std::cmp::min;
use itertools::izip;
use futures::stream::Stream;
use futures::io::AsyncRead;
use futures::task::{Poll, Context};
use async_stream::stream;

use crate::dap2::{xdr, hyperslab::{count_slab, parse_hyberslab}};

pub fn stream_variable<T>(f: Arc<netcdf::File>, vn: String, indices: Vec<usize>, counts: Vec<usize>) -> impl Stream<Item=T> {
    const CHUNK_SZ: usize = 1024;

    stream! {
        let n = counts.iter().product();

        let cur  = indices.clone();

        let mut jump = counts.iter().rev().scan(0, |n, &c| {
            if n >= CHUNK_SZ {


        let mut jump = Vec::new();
        for c in counts.iter().rev() {
            let p = jump.iter().product();
            if p >= CACHE_SZ {
                jump.push(1);
            } else {
                jump.push(min(CACHE_SZ / p, *c));
            }
        }

        jump.reverse();
        debug!("jump: {:?}", jump);

        let cache: Vec<T> = Vec::with_capacity(CHUNK_SZ);

        while cur.iter().product() < n {
            let mjump = cur.iter().zip(jump).map(|(s,j)| min(

            Some((indices, counts)) => v.values_to(&mut vbuf, Some(&indices), Some(&counts)),

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

            let mut vbuf: Vec<f64> = vec![0.0; v.len()];
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
    fn test_async_read() {
        let f = Arc::new(netcdf::open("data/coads_climatology.nc").unwrap());
        let v = stream_variable(f, "SST", vec![0,0,0], vec![12,90,180]);

        futures::executor::block_on_stream(v);
    }
}
