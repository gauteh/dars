use std::sync::Arc;
use std::fmt;
use std::path::PathBuf;

/// # Implementation of a member in NCML dataset.
///
pub struct NcmlMember
{
    pub filename: PathBuf,
    pub f: Arc<netcdf::File>,
    pub n: usize,
    pub rank: f64
}

impl fmt::Debug for NcmlMember {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} ([{}])", self.filename.clone(), self.n)
    }
}

impl NcmlMember
{
    pub fn open<P, S>(f: P, dim_name: S) -> Result<NcmlMember, anyhow::Error>
        where P: Into<PathBuf>,
              S: Into<String>
    {
        let f = f.into();
        debug!("Loading member: {:?}", f);
        let dim_name = dim_name.into();
        let nc = Arc::new(netcdf::open(f.clone())?);

        let v = nc.variable(&dim_name).expect(&format!("could not find coordinate variable in {:?}", f.clone()));
        let co = v.len();
        let r = v.value(Some(&[0])).expect(&format!("could not read first value of coordinate variable in {:?}", f.clone()));


        Ok(NcmlMember {
            filename: f,
            f: nc.clone(),
            n: co,
            rank: r
        })
    }
}

