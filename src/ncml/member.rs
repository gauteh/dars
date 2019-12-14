use std::sync::Arc;
use std::fmt;
use std::path::PathBuf;

/// # Implementation of a member in NCML dataset.
///
pub struct NcmlMember
{
    pub filename: PathBuf,
    pub f: Arc<netcdf::File>,
    dim: String,
    pub n: usize
}

impl fmt::Debug for NcmlMember {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} ({}[{}])", self.filename.clone(), self.dim, self.n)
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
        let co = nc.variable(&dim_name)
            .expect(&format!("could not find coordinate variable in {:?}", f.clone()));


        Ok(NcmlMember {
            filename: f,
            f: nc.clone(),
            dim: dim_name,
            n: co.len()
        })
    }
}

