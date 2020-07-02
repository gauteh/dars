//! Ripped off from warp::filters::log to get to debug!
use log::debug;
use std::fmt;

pub fn mlog(info: warp::filters::log::Info) {
    debug!(
        target: "dars::data",
        "{} \"{} {} {:?}\" {} \"{}\" \"{}\" {:?}",
        OptFmt(info.remote_addr()),
        info.method(),
        info.path(),
        info.version(),
        info.status().as_u16(),
        OptFmt(info.referer()),
        OptFmt(info.user_agent()),
        info.elapsed(),
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
