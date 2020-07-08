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
