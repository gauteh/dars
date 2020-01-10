#[cfg(test)]
pub fn init () {
    std::env::set_var("RUST_LOG", "dars=trace");
    let _ = env_logger::builder().is_test(true).try_init ();
}
