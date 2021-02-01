mod common;
pub use common::*;

#[tokio::test(flavor = "multi_thread")]
async fn coads_subset() {
    test_log();

    let dars = dars_test().await;
    let dods = warp::test::request()
        .path("/data/coads_climatology.nc4.dods?SST.SST[0][0:80][7]")
        .reply(&dars)
        .await;

    assert_eq!(dods.status(), 200);
    let (_, d1) = split_dods(dods.body());

    let tds = reqwest::get(&format!("{}/coads_climatology.nc.dods?SST.SST[0][0:80][7]", TDS_UNI)).await.unwrap();
    assert_eq!(tds.status(), 200);
    let tdods = tds.bytes().await.unwrap();
    let (_, d2) = split_dods(&tdods);

    assert_eq!(d1, d2);
}

#[tokio::test(flavor = "multi_thread")]
async fn coads_full_sst() {
    test_log();

    let dars = dars_test().await;
    let dods = warp::test::request()
        .path("/data/coads_climatology.nc4.dods?SST")
        .reply(&dars)
        .await;

    assert_eq!(dods.status(), 200);
    let (_, d1) = split_dods(dods.body());

    let tds = reqwest::get(&format!("{}/coads_climatology.nc.dods?SST", TDS_UNI)).await.unwrap();
    assert_eq!(tds.status(), 200);
    let tdods = tds.bytes().await.unwrap();
    let (_, d2) = split_dods(&tdods);

    assert_eq!(d1, d2);
}
