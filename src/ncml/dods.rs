use async_stream::stream;
use futures::pin_mut;
use futures::stream::{Stream, StreamExt};
use itertools::izip;
use std::cmp::min;
use std::iter::once;
use std::sync::Arc;

use super::nc::dods::pack_var;
use super::NcmlDataset;
use crate::dap2::hyperslab::{count_slab, parse_hyberslab};

pub fn xdr(
    ncml: &NcmlDataset,
    vs: Vec<String>,
) -> impl Stream<Item = Result<Vec<u8>, anyhow::Error>> {
    let fnc = ncml.members[0].f.clone();
    let dim = ncml.aggregation_dim.clone();
    let dim_len = ncml.dim_n;

    let ns = ncml.members.iter().map(|m| m.n).collect::<Vec<usize>>();

    // start index of each member
    let ss = ns
        .iter()
        .scan(0, |acc, &n| {
            let c = *acc;
            *acc = *acc + n;
            Some(c)
        })
        .collect::<Vec<usize>>();

    let fs = ncml
        .members
        .iter()
        .map(|m| m.f.clone())
        .collect::<Vec<Arc<netcdf::File>>>();

    stream! {
        for v in vs {
            trace!("streaming variable: {}", v);
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

            let vv = fnc.variable(mv).ok_or(anyhow!("variable not found"))?;

            if vv.dimensions().len() > 0 && vv.dimensions()[0].name() == dim {
                // single values cannot have a dimension so we only need to handle arrays here.
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

                trace!("Indices: {:?}, counts: {:?}, agg_sz = {}", ind, cnt, agg_sz);

                // loop through files until slab has been exhausted
                for (s, n, f) in izip!(&ss, &ns, &fs) {
                    trace!("testing: {}, {} against {} {}", s, n, ind[0], cnt[0]);
                    if ind[0] >= *s && ind[0] < (s + n) {
                        // pack start (incl len x 2)
                        let mut mind = ind.clone();
                        mind[0] = ind[0] - s;

                        let mut mcnt = cnt.clone();
                        mcnt[0] = min(cnt[0], *n - mind[0]);

                        let mvv = f.variable(mv).ok_or(anyhow!("variable not found"))?;
                        trace!("First file at {} to {} (i = {:?}, c = {:?})", s, s + n, mind, mcnt);

                        let pack = pack_var(f.clone(), String::from(mv), Some(agg_sz), (mind, mcnt));
                        pin_mut!(pack);

                        while let Some(p) = pack.next().await {
                            yield p;
                        }
                    } else if ind[0] < *s && (*s < ind[0] + cnt[0]) {
                        let mut mcnt = cnt.clone();
                        mcnt[0] = min((ind[0] + cnt[0] - *s), *n);

                        let mut mind = ind.clone();
                        mind[0] = 0;

                        let mvv = f.variable(mv).ok_or(anyhow!("variable not found"))?;

                        trace!("Consecutive file at {} to {} (i = {:?}, c = {:?})", s, s + n, mind, mcnt);

                        let pack = pack_var(f.clone(), String::from(mv), None, (mind, mcnt));
                        pin_mut!(pack);

                        while let Some(p) = pack.next().await {
                            yield p;
                        }
                    } else if ind[0] + cnt[0] < *s {
                        break;
                    } else {
                        continue;
                    }
                }
            } else {
                // variable without joining dimension, using values from first member
                let slab = match slab {
                    Some(t) => t,
                    None => (vec![0usize; vv.dimensions().len()], vv.dimensions().iter().map(|d| d.len()).collect::<Vec<usize>>())
                };

                trace!("Non aggregated variable, i = {:?}, c = {:?}", slab.0, slab.1);

                let pack = pack_var(fnc.clone(), String::from(mv), Some(slab.1.iter().product::<usize>()), slab);
                pin_mut!(pack);

                while let Some(p) = pack.next().await {
                    yield p;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on_stream;
    use futures_util::pin_mut;
    use std::io::Cursor;

    #[test]
    fn ncml_xdr_time_dim() {
        let nm = NcmlDataset::open("data/ncml/aggExisting.ncml").unwrap();
        let t = xdr(&nm, vec!["time".to_string()]);
        pin_mut!(t);
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
        let nm = NcmlDataset::open("data/ncml/aggExisting.ncml").unwrap();
        let t = xdr(&nm, vec!["T".to_string()]);
        pin_mut!(t);
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
        let nm = NcmlDataset::open("data/ncml/scan.ncml").unwrap();

        let t = xdr(&nm, vec!["T.T[0:50][0][0]".to_string()]);
        pin_mut!(t);
        let bs: Vec<u8> = block_on_stream(t)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .flatten()
            .skip(4)
            .map(|b| b.clone())
            .collect();
        assert!(bs.len() == 4 + 51 * 8);

        let t = xdr(&nm, vec!["T.T[20:50][0][0]".to_string()]);
        pin_mut!(t);
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
        let t = xdr(&nm, vec!["T.T[31:32][0][0]".to_string()]);
        pin_mut!(t);
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
