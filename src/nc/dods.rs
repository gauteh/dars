use async_stream::stream;
use std::sync::Arc;

use futures::stream::Stream;

use crate::dap2;

pub fn xdr(nc: Arc<netcdf::File>, vs: Vec<String>) -> impl Stream<Item = Result<Vec<u8>, anyhow::Error>> {
    stream! {
        for v in vs {
            let mv = match v.find(".") {
                Some(i) => &v[i+1..],
                None => &v
            };

            let vbuf = if let Some(i) = mv.find("[") {
                let vv = nc.variable(&mv[..i]).ok_or(anyhow!("variable not found"))?;
                let slab = dap2::parse_hyberslab(&mv[i..])?;

                let counts = slab.iter().map(dap2::count_slab).collect::<Vec<usize>>();
                let n = counts.iter().product::<usize>();

                if n > vv.len() {
                    Err(anyhow!("slab too great"))?;
                }

                let indices = slab.iter().map(|slab| slab[0]).collect::<Vec<usize>>();

                match vv.vartype() {
                    netcdf_sys::NC_FLOAT => {
                        let mut vbuf: Vec<f32> = vec![0.0; n];
                        vv.values_to(&mut vbuf, Some(&indices), Some(&counts))?;
                        dap2::xdr::pack_xdr(vbuf)
                    },
                    netcdf_sys::NC_DOUBLE => {
                        let mut vbuf: Vec<f64> = vec![0.0; n];
                        vv.values_to(&mut vbuf, Some(&indices), Some(&counts))?;
                        dap2::xdr::pack_xdr(vbuf)
                    },
                    _ => unimplemented!()
                }
            } else {
                let vv = nc.variable(mv).ok_or(anyhow!("variable not found"))?;
                match vv.vartype() {
                    netcdf_sys::NC_FLOAT => {
                        let mut vbuf: Vec<f32> = vec![0.0; vv.len()];
                        vv.values_to(&mut vbuf, None, None)?;
                        dap2::xdr::pack_xdr(vbuf)
                    },
                    netcdf_sys::NC_DOUBLE => {
                        let mut vbuf: Vec<f64> = vec![0.0; vv.len()];
                        vv.values_to(&mut vbuf, None, None)?;
                        dap2::xdr::pack_xdr(vbuf)
                    },
                    _ => unimplemented!()
                }
            };

            yield vbuf;
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

    #[test]
    fn read_var() {
        // b.iter(|| {
            let f = netcdf::open("data/coads_climatology.nc").unwrap();
            let v = f.variable("SST").unwrap();

            let mut vbuf: Vec<f32> = vec![0.0; 1];
            v.values_to(&mut vbuf, Some(&[1, 1, 1]), Some(&[1,1,1])).unwrap();

            // let vbuf = v.values::<f32>(Some(&[0, 0, 0]), Some(&[1,5,1])).unwrap();


            println!("vbuf: {:?}", vbuf);

            // vbuf
        // });
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
