use std::convert::TryInto;
use std::sync::Arc;
use warp::Filter;
use dars::{config, data};

pub const TDS_UNI: &'static str = "https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/";
pub const TDS_MET: &'static str = "https://thredds.met.no/thredds/dodsC/";
pub const TDS_LCL: &'static str = "http://localhost:8002/thredds/dodsC/test/data/";

pub fn test_log() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("dap2=debug,dars=debug")).is_test(true).try_init();
}

/// Set up a test-server.
pub async fn dars_test() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let config = config::Config::default();
    let db = sled::Config::default()
        .temporary(true)
        .open()
        .unwrap();
    let data =
        Arc::new(data::Datasets::new_with_datadir(config.root_url.clone(), "../data/".into(), db).await.unwrap());
    data::filters::datasets(data.clone()).with(warp::log::custom(data::request_log))
}

// https://stackoverflow.com/questions/35901547/how-can-i-find-a-subsequence-in-a-u8-slice
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

/// Split DODS response in DDS and data.
pub fn split_dods(b: &[u8]) -> (&[u8], &[u8]) {
    let i = find_subsequence(&b, b"Data:\n").expect("Could not find 'Data:' marker");
    b.split_at(i + 6)
}

/// Print and split DODS using hexyl.
pub fn print_split_dods(b: &[u8]) -> (&[u8], &[u8]) {
    use hexyl::Printer;

    let (dds, dods) = split_dods(b);
    println!("DDS: {}", String::from_utf8_lossy(dds));

    let l = u32::from_be_bytes(dods[..4].try_into().unwrap());
    let l2 = u32::from_be_bytes(dods[4..8].try_into().unwrap());
    assert_eq!(l, l2);

    println!("DODS, length = {}:", l);

    let s = std::io::stdout();
    let mut s = s.lock();
    let mut p = Printer::new(&mut s, true, hexyl::BorderStyle::Unicode, true);
    p.print_all(dods).expect("Could not write DODS hex to stdout");

    (dds, dods)
}
