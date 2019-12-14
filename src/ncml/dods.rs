use std::sync::Arc;
use futures::stream::Stream;
use async_stream::stream;
use itertools::izip;
use std::iter::once;
use std::cmp::min;

use crate::dap2::hyperslab::{count_slab, parse_hyberslab};
use super::NcmlDataset;
use super::nc::dods::pack_var;

pub fn xdr(ncml: &NcmlDataset, vs: Vec<String>) -> impl Stream<Item = Result<Vec<u8>, anyhow::Error>> {
    let fnc = ncml.members[0].f.clone();
    let dim = ncml.aggregation_dim.clone();
    let dim_len = ncml.dds.dim_n;

    let ns = ncml.members.iter().map(|m| m.n).collect::<Vec<usize>>();

    // start index of each member
    let ss = ns.iter().scan(0, |acc, &n| {
        let c = *acc;
        *acc = *acc + n;
        Some(c)
    }).collect::<Vec<usize>>();

    let fs = ncml.members.iter().map(|m| m.f.clone()).collect::<Vec<Arc<netcdf::File>>>();

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

                    Some((indices, counts))
                },

                None => None
            };

            let vv = fnc.variable(mv).ok_or(anyhow!("variable not found"))?;

            // TODO: loop over chunks
            if vv.dimensions().len() > 0 && vv.dimensions()[0].name() == dim {
                // single values are cannot have a dimension so we only need to handle arrays here.
                // arrays have their length sent first, but single values do not.
                let (ind, cnt) = match slab {
                    Some((i,c)) => (i, c),
                    None => (vec![0; vv.dimensions().len()],
                        once(dim_len).chain(
                            vv.dimensions().iter().skip(1).map(|d| d.len())).collect::<Vec<usize>>())

                };

                if ind[0] + cnt[0] > dim_len {
                    yield Err(anyhow!("slab too great"));
                }

                let agg_sz = cnt.iter().product::<usize>();

                // loop through files untill slab has been exhausted
                for (s, n, f) in izip!(&ss, &ns, &fs) {
                    if ind[0] >= *s && ind[0] < (s + n) {
                        // pack start (incl len x 2)
                        let mut mcnt = cnt.clone();
                        mcnt[0] = min(cnt[0], *n);

                        let mut mind = ind.clone();
                        mind[0] = ind[0] - s;

                        let mvv = f.variable(mv).ok_or(anyhow!("variable not found"))?;
                        yield pack_var(mvv, true, Some(agg_sz), Some((mind, mcnt)));

                    } else if ind[0] < *s && (*s < ind[0] + cnt[0]) {
                        let mut mcnt = cnt.clone();
                        mcnt[0] = min((cnt[0] - *s), *n);

                        let mut mind = ind.clone();
                        mind[0] = 0;

                        let mvv = f.variable(mv).ok_or(anyhow!("variable not found"))?;
                        yield pack_var(mvv, false, None, Some((mind, mcnt)));
                    } else {
                        break;
                    }
                }
            } else {
                // take first member
                yield pack_var(vv, true, None, slab);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::pin_mut;
    use futures::executor::block_on_stream;
    use std::io::Cursor;

    #[test]
    fn ncml_xdr_time_dim() {
        let nm = NcmlDataset::open("data/ncml/aggExisting.ncml").unwrap();
        let t = xdr(&nm, vec!["time".to_string()]);
        pin_mut!(t);
        let bs: Vec<u8> = block_on_stream(t).collect::<Result<Vec<_>,_>>().unwrap().iter().flatten().skip(4).map(|b| b.clone()).collect();

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
        let jt = jan.variable("time").unwrap().values::<i32>(None, None).unwrap();

        assert!(&buf[0..31] == jt.as_slice().unwrap());

        let feb = netcdf::open("data/ncml/feb.nc").unwrap();
        let ft = feb.variable("time").unwrap().values::<i32>(None, None).unwrap();

        assert!(&buf[31..] == ft.as_slice().unwrap());
    }

    #[test]
    fn ncml_xdr_temp() {
        let nm = NcmlDataset::open("data/ncml/aggExisting.ncml").unwrap();
        let t = xdr(&nm, vec!["T".to_string()]);
        pin_mut!(t);
        let bs: Vec<u8> = block_on_stream(t).collect::<Result<Vec<_>,_>>().unwrap().iter().flatten().skip(4).map(|b| b.clone()).collect();

        println!("len: {}", bs.len() / 8);
        let n: usize = (bs.len() / 8);

        println!("transmitted length: {:?}", &bs[0..4]);
        assert_eq!(n, 3*4*(31+28));

        let mut T = Cursor::new(&bs[4..]);

        let mut buf: Vec<f64> = vec![0.0; n];
        let sz = xdr_codec::unpack_array(&mut T, &mut buf, n, None).unwrap();

        assert_eq!(sz, (31 + 28)*3*4 * 8);

        let jan = netcdf::open("data/ncml/jan.nc").unwrap();
        let jt = jan.variable("T").unwrap().values::<f64>(None, None).unwrap();

        assert!(&buf[0..(31*3*4)] == jt.as_slice().unwrap());

        let feb = netcdf::open("data/ncml/feb.nc").unwrap();
        let ft = feb.variable("T").unwrap().values::<f64>(None, None).unwrap();

        assert!(&buf[(31*3*4)..] == ft.as_slice().unwrap());
    }
}
