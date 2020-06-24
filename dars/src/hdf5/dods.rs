use async_trait::async_trait;
use futures::stream::TryStreamExt;

use dap2::dds::DdsVariableDetails;
use dap2::dods::{Dods, DodsVariable};

use hidefix::filters::byteorder::*;

use super::Hdf5Dataset;

#[async_trait]
impl Dods for Hdf5Dataset {
    async fn variable(&self, variable: &DdsVariableDetails) -> Result<DodsVariable, anyhow::Error> {
        let reader = self.idx.streamer(&variable.name)?;

        let indices: Vec<u64> = variable.indices.iter().map(|c| *c as u64).collect();
        let counts: Vec<u64> = variable.counts.iter().map(|c| *c as u64).collect();

        let dsz = variable.vartype.size();
        let order = reader.order();

        let r = Box::pin(
            reader
                .stream(Some(indices.as_slice()), Some(counts.as_slice()))
                .and_then(move |mut v| {
                    let dsz = dsz;
                    async move { to_big_e_sized(&mut v, order, dsz).map(|_| v) }
                })
                .map_err(|_| std::io::ErrorKind::UnexpectedEof.into()),
        )
        .into_async_read();

        if variable.is_scalar() {
            Ok(DodsVariable::Value(Box::pin(r)))
        } else {
            Ok(DodsVariable::Array(variable.len(), Box::pin(r)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dap2::constraint::Constraint;
    use dap2::dds::ConstrainedVariable;
    use futures::executor::block_on;
    use futures::io::AsyncReadExt;
    use test::Bencher;

    #[bench]
    fn coads_sst_struct(b: &mut Bencher) {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();

        let c = Constraint::parse("SST.SST").unwrap();
        let dds = hd.dds.dds(&c).unwrap();

        assert_eq!(dds.variables.len(), 1);
        if let ConstrainedVariable::Structure {
            variable: _,
            member,
        } = &dds.variables[0]
        {
            b.iter(|| {
                block_on(async {
                    let reader = hd.variable(&member).await.unwrap();
                    if let DodsVariable::Array(sz, mut reader) = reader {
                        assert_eq!(sz, 12 * 90 * 180);
                        let mut buf = Vec::with_capacity(8 * 1024);
                        reader.read_to_end(&mut buf).await.unwrap();
                        assert_eq!(buf.len(), 12 * 90 * 180 * 4);
                    } else {
                        panic!("not array variable");
                    }
                })
            });
        } else {
            panic!("wrong constrained variable");
        }
    }
}
