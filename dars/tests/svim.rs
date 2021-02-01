mod common;
pub use common::*;

#[tokio::test(flavor = "multi_thread")]
async fn ocean_time() {
    test_log();

    let dars = dars_test().await;
    let dods = warp::test::request()
        .path("/data/met/ocean_avg_19600101.nc4.dods?ocean_time")
        .reply(&dars)
        .await;

    assert_eq!(dods.status(), 200);
    let (_, d1) = split_dods(dods.body());

    println!("getting remote..");
    let tds = reqwest::get(&format!("{}/nansen-legacy-ocean/SVIM/1960/ocean_avg_19600101.nc4.dods?ocean_time", TDS_MET)).await.unwrap();
    assert_eq!(tds.status(), 200);
    let tdods = tds.bytes().await.unwrap();
    let (_, d2) = split_dods(&tdods);

    assert_eq!(d1, d2);
}

#[tokio::test(flavor = "multi_thread")]
async fn temp_int16() {
    test_log();

    let dars = dars_test().await;
    let dods = warp::test::request()
        .path("/data/met/ocean_avg_19600101.nc4.dods?temp.temp[0][0][0][0]")
        .reply(&dars)
        .await;

    assert_eq!(dods.status(), 200);
    let (_, d1) = split_dods(dods.body());

    println!("getting remote..");
    let tds = reqwest::get(&format!("{}/nansen-legacy-ocean/SVIM/1960/ocean_avg_19600101.nc4.dods?temp[0][0][0][0]", TDS_MET)).await.unwrap();
    assert_eq!(tds.status(), 200);
    let tdods = tds.bytes().await.unwrap();
    let (_, d2) = split_dods(&tdods);

    assert_eq!(d1, d2);
}
