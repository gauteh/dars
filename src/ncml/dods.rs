use std::sync::Arc;
use futures::stream::Stream;
use async_stream::stream;
use itertools::izip;
use std::iter::once;

use crate::dap2::{xdr, hyperslab::{count_slab, parse_hyberslab}};
use super::NcmlDataset;
use super::nc;

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

pub fn xdr(ncml: &NcmlDataset, vs: Vec<String>) -> impl Stream<Item = Result<Vec<u8>, anyhow::Error>> {
    let fnc = ncml.members[0].f.clone();
    let dim = ncml.aggregation_dim.clone();
    let dim_len = ncml.dds.dim_n;

    let ns = ncml.members.iter().map(|m| m.n).collect::<Vec<usize>>();
    let ss = ns.iter().scan(0, |acc, &n| {
        let c = *acc;
        *acc = *acc + n;
        Some(c)
    }).collect::<Vec<usize>>();
    let fs = ncml.members.iter().map(|m| m.f.clone()).collect::<Vec<Arc<netcdf::File>>>();

    println!("ns: {:#?}", ns);
    println!("ss: {:#?}", ss);

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
                let (ind, cnt) = match slab {
                    Some((i,c)) => (i, c),
                    None => (vec![0; vv.dimensions().len()],
                        once(dim_len).chain(
                            vv.dimensions().iter().skip(1).map(|d| d.len())).collect::<Vec<usize>>())

                };

                if ind[0] + cnt[0] <= dim_len {
                    yield Err(anyhow!("slab too great"));
                }

                // loop through files untill slab has been exhausted
                for (s, n, f) in izip!(&ss, &ns, &fs) {
                    if ind[0] >= s && ind[0] < (s + n) {
                        // pack start (incl len x 2)
                    } else if (ind[0] + cnt[0]) > s {
                        // can be joined with a max(..)
                        if (ind[0] + cnt[0]) > (s + n) {
                            // pack whole
                        } else {
                            // pack some
                        }
                    }
                }
            } else {
                // take first member
                yield match vv.vartype() {
                    netcdf_sys::NC_FLOAT => nc::dods::xdr_chunk::<f32>(vv, slab),
                    netcdf_sys::NC_DOUBLE => nc::dods::xdr_chunk::<f64>(vv, slab),
                    netcdf_sys::NC_INT => nc::dods::xdr_chunk::<i32>(vv, slab),
                    netcdf_sys::NC_BYTE => nc::dods::xdr_chunk::<u8>(vv, slab),
                    // netcdf_sys::NC_UBYTE => xdr_bytes(vv),
                    // netcdf_sys::NC_CHAR => xdr_bytes(vv),
                    _ => unimplemented!()
                };

            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use futures_util::pin_mut;

    #[test]
    fn ncml_xdr_time_dim() {
        let nm = NcmlDataset::open("data/ncml/aggExisting.ncml").unwrap();
        let t = xdr(&nm, vec!["TIME".to_string()]);
        pin_mut!(t);
        t.next();
    }
}
