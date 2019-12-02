use async_stream::stream;
use async_std::task;
use std::sync::Arc;

use futures_util::pin_mut;
use futures_util::stream::{Stream, StreamExt};
use std::io::Cursor;

pub fn xdr(nc: Arc<netcdf::File>, vs: Vec<String>) -> impl Stream<Item = Vec<u8>> {
    stream! {
        for v in vs {
            let vv = nc.variable(&v).expect("could not open variable");
            let mut vbuf: Vec<f64> = vec![0.0; vv.len()];
            vv.values_to(&mut vbuf, None, None).expect("could not read values");

            let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::with_capacity(1024*1024*3));

            use xdr_codec::pack;

            pack(&vbuf.len(), &mut buf).expect("could not pack length of array");
            pack(&vbuf, &mut buf).expect("could not pack XDR");

            yield buf.into_inner();
        }
    }
}

struct NcDods {
    f: Arc<netcdf::File>,
    v: Arc<Vec<String>>,
    i: usize
}

impl NcDods {
    pub fn make(f: &str, v: Vec<String>) -> NcDods {
        NcDods {
            f: Arc::new(netcdf::open(format!("data/{}", f)).expect("could not open file")),
            v: Arc::new(v),
            i: 0
        }
    }
}

impl Iterator for NcDods {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i < self.v.len() {

            let vv = self.f.variable(&self.v[self.i]).expect("could not open variable");
            let mut vbuf: Vec<f32> = vec![0.0; vv.len()];
            vv.values_to(&mut vbuf, None, None).expect("could not read values");


            let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
            use xdr_codec::pack;

            pack(&vbuf.len(), &mut buf).unwrap();
            xdr_codec::pack(&vbuf, &mut buf).expect("could not pack XDR");

            self.i += 1;

            Some(buf.into_inner())

        } else {
            self.i = 0;
            None
        }
    }
}

pub fn var_xdr(f: &str, v: &str) -> Vec<u8> {
    debug!("XDR: {}:{}", f, v);

    let f = netcdf::open(format!("data/{}", f)).expect("could not open file");

    let v = f.variable(v).expect("could not open variable");

    let mut vbuf: Vec<f64> = vec![0.0; v.len()];
    v.values_to(&mut vbuf, None, None).expect("could not read values");

    let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
    use xdr_codec::pack;

    pack(&vbuf.len(), &mut buf).unwrap();
    xdr_codec::pack(&vbuf, &mut buf).expect("could not pack XDR");

    buf.into_inner()
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[bench]
    fn open_nc(b: &mut Bencher) {
        b.iter(|| {
            let f = netcdf::open("data/coads_climatology.nc").unwrap();
        });
    }

    #[bench]
    fn open_nc_native(b: &mut Bencher) {
        use std::fs::File;
        use std::io::prelude::*;

        b.iter(|| {
            let f = File::open("data/coads_climatology.nc").unwrap();

            f
        });
    }

    #[bench]
    fn read_native_all(b: &mut Bencher) {
        use std::fs::File;
        use std::io::prelude::*;

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
            v.values_to(&mut vbuf, None, None).expect("could not read values");

            vbuf
        });
    }

    #[bench]
    fn read_var_hdf5(b: &mut Bencher) {
        let f = hdf5::File::open("data/coads.hdf5.nc", "r").unwrap();

        b.iter(|| {
            let d = f.dataset("SST").unwrap();
            let v: Vec<f32> = d.read_raw().unwrap();
        });
    }

    #[bench]
    fn xdr_var(b: &mut Bencher) {
        b.iter(|| {
            let v = var_xdr(
                "coads_climatology.nc",
                "SST");
        });
    }

    #[bench]
    fn xdr_stream(b: &mut Bencher) {
        use futures::pin_mut;
        use futures::executor::block_on_stream;

        b.iter(|| {
            let v = xdr(
                "coads_climatology.nc".to_string(),
                vec![ "SST".to_string() ]);

            pin_mut!(v);
            block_on_stream(v).collect::<Vec<Vec<u8>>>()
        });
    }

    #[bench]
    fn xdr_iter(b: &mut Bencher) {
        b.iter(|| {
            let v = NcDods::make("coads_climatology.nc", vec!["SST".to_string()]);
            v.into_iter().collect::<Vec<Vec<u8>>>()
        });
    }

    #[bench]
    fn pack_xdr(b: &mut Bencher) {
        let f = netcdf::open("data/coads_climatology.nc").unwrap();
        let v = f.variable("SST").unwrap();

        let mut vbuf: Vec<f32> = vec![0.0; v.len()];
        v.values_to(&mut vbuf, None, None).expect("could not read values");

        b.iter(|| {
            let b = serde_xdr::to_bytes(&vbuf);
        });
    }
}
