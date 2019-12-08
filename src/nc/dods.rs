use std::sync::Arc;
use futures::stream::Stream;
use async_stream::stream;

use crate::dap2::{xdr, hyperslab::{count_slab, parse_hyberslab}};

// TODO: Try tokio::codec::FramedRead with Read impl on dods?

fn xdr_chunk<T>(v: &netcdf::Variable, slab: Option<(Vec<usize>, Vec<usize>)>) -> Result<Vec<u8>, anyhow::Error>
    where T:    netcdf::variable::Numeric +
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
        Err(anyhow!("slab too great"))?;
    }

    let mut vbuf: Vec<T> = vec![T::default(); n];

    match slab {
        Some((indices, counts)) => v.values_to(&mut vbuf, Some(&indices), Some(&counts)),
        None => v.values_to(&mut vbuf, None, None)
    }?;

    if v.dimensions().len() > 0 {
        xdr::pack_xdr_arr(vbuf)
    } else {
        xdr::pack_xdr_val(vbuf)
    }
}

pub fn xdr(nc: Arc<netcdf::File>, vs: Vec<String>) -> impl Stream<Item = Result<Vec<u8>, anyhow::Error>> {
    stream! {
        for v in vs {
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

                    Some((indices, counts))
                },

                None => None
            };

            let vv = nc.variable(mv).ok_or(anyhow!("variable not found"))?;

            // TODO: loop over chunks

            yield match vv.vartype() {
                netcdf_sys::NC_FLOAT => xdr_chunk::<f32>(vv, slab),
                netcdf_sys::NC_DOUBLE => xdr_chunk::<f64>(vv, slab),
                netcdf_sys::NC_INT => xdr_chunk::<i32>(vv, slab),
                netcdf_sys::NC_BYTE => xdr_chunk::<u32>(vv, slab),
                netcdf_sys::NC_UBYTE => xdr_chunk::<u32>(vv, slab),
                netcdf_sys::NC_CHAR => xdr_chunk::<u32>(vv, slab),
                _ => unimplemented!()
            };
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
                vec![ "COADSX".to_string() ]);

            pin_mut!(v);
            block_on_stream(v).collect::<Vec<_>>()
        });
    }
}