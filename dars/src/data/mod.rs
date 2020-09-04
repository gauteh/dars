use colored::Colorize;
use log::debug;
use std::fmt;
use std::sync::Arc;

mod dataset;
pub mod filters;
mod handlers;

pub use dataset::{DatasetType, Datasets};
pub type State = Arc<Datasets>;

/// Ripped off from warp::filters::log to get to debug!
pub fn request_log(info: warp::filters::log::Info) {
    debug!(
        target: "dars::data",
        "{} \"{} {} {:?}\" {} \"{}\" \"{}\" {}",
        OptFmt(info.remote_addr()).to_string().yellow(),
        info.method().to_string().blue(),
        info.path().bold(),
        info.version(),
        info.status().as_u16().to_string().white(),
        OptFmt(info.referer()),
        OptFmt(info.user_agent()),
        format!("{:?}", info.elapsed()).italic(),
    );
}

struct OptFmt<T>(Option<T>);

impl<T: fmt::Display> fmt::Display for OptFmt<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref t) = self.0 {
            fmt::Display::fmt(t, f)
        } else {
            f.write_str("-")
        }
    }
}

#[cfg(test)]
pub fn test_state() -> State {
    use crate::hdf5;

    let mut data = Datasets::temporary();
    data.datasets.insert(
        "coads_climatology.nc4".to_string(),
        Arc::new(DatasetType::HDF5(
            hdf5::Hdf5Dataset::open(
                "../data/coads_climatology.nc4",
                "nested/coads_climatology.nc4".into(),
                &data.db,
            )
            .unwrap(),
        )),
    );
    data.datasets.insert(
        "nested/coads_climatology.nc4".to_string(),
        Arc::new(DatasetType::HDF5(
            hdf5::Hdf5Dataset::open(
                "../data/coads_climatology.nc4",
                "nested/coads_climatology.nc4".into(),
                &data.db,
            )
            .unwrap(),
        )),
    );
    Arc::new(data)
}

#[cfg(test)]
pub fn test_db() -> sled::Db {
    sled::Config::default()
        .temporary(true)
        .print_profile_on_drop(true)
        .open()
        .unwrap()
}
