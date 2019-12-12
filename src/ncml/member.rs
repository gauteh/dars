use std::sync::Arc;
use std::path::PathBuf;

/// `T` is type of coordinate dimension
struct NcmlMemberI<T> {
    pub filename: PathBuf,
    f: Arc<netcdf::File>,

    // lower and upper bounds on coordinate dimensions.
    lower: T,
    upper: T,
}

pub trait NcmlMember {

}

impl NcmlMember for NcmlMemberI<f64> { }

pub fn open<P, S>(f: P, dim_name: S) -> Result<Box<dyn NcmlMember>, anyhow::Error>
    where P: Into<PathBuf>,
          S: Into<String>
{
    let f = f.into();
    let dim_name = dim_name.into();
    info!("Loading ncml member: {:?}", f);

    let nc = netcdf::open(f.clone())?;
    let co = nc.variable(&dim_name)
        .expect(&format!("could not find coordinate variable in {:?}", f.clone()));

    Ok(Box::new(
        match co.vartype() {
            netcdf_sys::NC_DOUBLE => NcmlMemberI::<f64>::open(f, dim_name)?,
            _ => unimplemented!()
        }
    ))
}

impl<T> NcmlMemberI<T>
where T: netcdf::Numeric
{
    pub fn open<P, S>(f: P, dim_name: S) -> Result<NcmlMemberI<T>, anyhow::Error>
        where P: Into<PathBuf>,
              S: Into<String>
    {
        let f = f.into();
        let dim_name = dim_name.into();
        let nc = Arc::new(netcdf::open(f.clone())?);
        let co = nc.variable(&dim_name)
            .expect(&format!("could not find coordinate variable in {:?}", f.clone()));


        Ok(NcmlMemberI::<T> {
            filename: f,
            f: nc.clone(),
            lower: co.value(None)?,
            upper: co.value(None)?
        })
    }
}

