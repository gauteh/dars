use futures::executor::{block_on, block_on_stream};
use std::collections::HashMap;
use std::sync::Arc;

use divan::Bencher;
use warp::reply::Reply;

use dap2::constraint::Constraint;
use dars::data::State;
use dars::data::{DatasetType, Datasets};
use dars::hdf5::Hdf5Dataset;

fn test_db() -> sled::Db {
    sled::Config::default()
        .temporary(true)
        .print_profile_on_drop(true)
        .open()
        .unwrap()
}

fn temporary() -> Datasets {
    Datasets {
        datasets: HashMap::default(),
        url: None,
        db: test_db(),
    }
}

fn test_state() -> State {
    let mut data = temporary();
    data.datasets.insert(
        "coads_climatology.nc4".to_string(),
        Arc::new(DatasetType::HDF5(
            Hdf5Dataset::open(
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
            Hdf5Dataset::open(
                "../data/coads_climatology.nc4",
                "nested/coads_climatology.nc4".into(),
                &data.db,
            )
            .unwrap(),
        )),
    );
    Arc::new(data)
}

#[divan::bench]
fn coads_das(b: Bencher) {
    let state = test_state();
    let das = dars::data::filters::das(state.clone());

    b.bench_local(|| {
        let res = block_on(
            warp::test::request()
                .path("/data/coads_climatology.nc4.das")
                .reply(&das),
        );

        assert_eq!(res.status(), 200);
        divan::black_box(res.body());
    });
}

#[divan::bench]
fn dds_constraint(b: Bencher) {
    let filter = dars::data::filters::constraint();

    b.bench_local(|| {
        block_on(
            warp::test::request()
                .path("/data/coads_climatology.nc4.dds?SST[0:5][0:70][0:70],TIME,COADSX,COADSY")
                .filter(&filter),
        )
        .unwrap()
    })
}

#[divan::bench]
fn coads_dds_constrained(b: Bencher) {
    let state = test_state();
    let dds = dars::data::filters::dds(state.clone());

    b.bench_local(|| {
        let res = block_on(
            warp::test::request()
                .path("/data/coads_climatology.nc4.dds?SST[0:5][0:70][0:70],TIME,COADSX,COADSY")
                .reply(&dds),
        );

        assert_eq!(res.status(), 200);
        divan::black_box(res.body());
    })
}

#[divan::bench]
fn coads_dds_unconstrained(b: Bencher) {
    let state = test_state();
    let dds = dars::data::filters::dds(state.clone());

    b.bench_local(|| {
        let res = block_on(
            warp::test::request()
                .path("/data/coads_climatology.nc4.dds")
                .reply(&dds),
        );

        assert_eq!(res.status(), 200);
        divan::black_box(res.body());
    })
}

#[divan::bench]
fn coads_build_sst_struct(b: Bencher) {
    let db = test_db();
    let hd = Arc::new(DatasetType::HDF5(
        Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap(),
    ));

    let c = Constraint::parse("SST.SST").unwrap();

    b.bench_local(|| {
        let hd = hd.clone();
        let c = c.clone();
        block_on(dars::data::handlers::dods(hd, c))
    })
}

#[divan::bench]
fn coads_stream_sst_struct(b: Bencher) {
    let db = test_db();
    let hd = Arc::new(DatasetType::HDF5(
        Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap(),
    ));

    let c = Constraint::parse("SST.SST").unwrap();

    b.bench_local(|| {
        let hd = hd.clone();
        let c = c.clone();
        let response = block_on(dars::data::handlers::dods(hd, c))
            .unwrap()
            .into_response();
        block_on_stream(response.into_body()).for_each(drop);
    })
}

fn main() {
    divan::main();
}
