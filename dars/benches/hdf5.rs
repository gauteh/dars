use divan::Bencher;
use futures::executor::{block_on, block_on_stream};
use futures::pin_mut;

use dap2::constraint::Constraint;
use dap2::dds::ConstrainedVariable;
use dap2::DodsXdr;
use dars::hdf5::Hdf5Dataset;

fn test_db() -> sled::Db {
    sled::Config::default()
        .temporary(true)
        .print_profile_on_drop(true)
        .open()
        .unwrap()
}

#[divan::bench]
fn coads_stream_sst_struct(b: Bencher) {
    let db = test_db();
    let hd = Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap();

    let c = Constraint::parse("SST.SST").unwrap();
    let dds = hd.dds.dds(&c).unwrap();

    assert_eq!(dds.variables.len(), 1);
    if let ConstrainedVariable::Structure {
        variable: _,
        member,
    } = &dds.variables[0]
    {
        b.bench_local(|| {
            let reader = block_on(hd.variable_xdr(&member)).unwrap();
            pin_mut!(reader);
            block_on_stream(reader).for_each(drop);
        });
    } else {
        panic!("wrong constrained variable");
    }
}

#[divan::bench]
fn coads_get_modified_time(b: Bencher) {
    b.bench_local(|| {
        let m = std::fs::metadata("../data/coads_climatology.nc4").unwrap();
        divan::black_box(m.modified().unwrap());
    })
}

#[divan::bench]
fn coads_das(b: Bencher) {
    let db = test_db();
    let hd = Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap();

    b.bench_local(|| hd.das.to_string());
}

#[divan::bench]
fn coads_dds(b: Bencher) {
    let db = test_db();
    let hd = Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap();

    b.bench_local(|| hd.dds.all().to_string());

    let dds = hd.dds.all().to_string();

    // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds
    //
    // filename updated
    // keys sorted by name

    let tds = r#"Dataset {
    Grid {
     ARRAY:
        Float32 AIRT[TIME = 12][COADSY = 90][COADSX = 180];
     MAPS:
        Float64 TIME[TIME = 12];
        Float64 COADSY[COADSY = 90];
        Float64 COADSX[COADSX = 180];
    } AIRT;
    Float64 COADSX[COADSX = 180];
    Float64 COADSY[COADSY = 90];
    Grid {
     ARRAY:
        Float32 SST[TIME = 12][COADSY = 90][COADSX = 180];
     MAPS:
        Float64 TIME[TIME = 12];
        Float64 COADSY[COADSY = 90];
        Float64 COADSX[COADSX = 180];
    } SST;
    Float64 TIME[TIME = 12];
    Grid {
     ARRAY:
        Float32 UWND[TIME = 12][COADSY = 90][COADSX = 180];
     MAPS:
        Float64 TIME[TIME = 12];
        Float64 COADSY[COADSY = 90];
        Float64 COADSX[COADSX = 180];
    } UWND;
    Grid {
     ARRAY:
        Float32 VWND[TIME = 12][COADSY = 90][COADSX = 180];
     MAPS:
        Float64 TIME[TIME = 12];
        Float64 COADSY[COADSY = 90];
        Float64 COADSX[COADSX = 180];
    } VWND;
} coads;"#;

    assert_eq!(dds, tds);
}

#[divan::bench]
fn coads_sst_grid(b: Bencher) {
    let db = test_db();
    let hd = Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap();

    let c = Constraint::parse("SST").unwrap();
    b.bench_local(|| hd.dds.dds(&c).unwrap().to_string());
    let dds = hd.dds.dds(&c).unwrap();

    // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds?SST
    let tds = r#"Dataset {
    Grid {
     ARRAY:
        Float32 SST[TIME = 12][COADSY = 90][COADSX = 180];
     MAPS:
        Float64 TIME[TIME = 12];
        Float64 COADSY[COADSY = 90];
        Float64 COADSX[COADSX = 180];
    } SST;
} coads;"#;

    assert_eq!(dds.to_string(), tds);
}

fn main() {
    divan::main();
}
