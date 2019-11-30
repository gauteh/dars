// struct NcDods;

// constraints
//
// * variables
// * hyperslabs

// impl NcDods {
    // pub fn parse_hyberslab(q: &str) -> Vec<usize> {
    //     // [0:10][1:30]

    // }

    // Stream {
    //     async read; poll ready
    // }
// }
pub fn var_xdr(f: &str, v: &str) -> Vec<u8> {
    // XXX: Float32 is apparently f64 in xdr world.
    debug!("XDR: {}:{}", f, v);
    use std::io::Cursor;

    let f = netcdf::open(format!("data/{}", f)).expect("could not open file");

    let v = f.variable(v).expect("could not open variable");

    let mut vbuf: Vec<f64> = vec![0.0; v.len()];
    v.values_to(&mut vbuf, None, None).expect("could not read values");

    let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
    use xdr_codec::pack;

    pack(&vbuf.len(), &mut buf).unwrap();
    pack(&vbuf.len(), &mut buf).unwrap();
    vbuf.iter().for_each(|f: &f64| pack::<_,f64>(&(*f as f64), &mut buf).unwrap());
    // xdr_codec::pack(&vbuf, &mut buf).expect("could not pack XDR");

    buf.into_inner()
}

