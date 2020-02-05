#![feature(test)]
extern crate test;
use test::Bencher;

use hdfidx::{idx::Index, reader::DatasetReader};

#[bench]
fn read_2d_chunked_idx(b: &mut Bencher) {
    let i = Index::index("tests/data/chunked_oneD.h5").unwrap();
    let r = DatasetReader::with_dataset(i.dataset("d_4_chunks").unwrap(), i.path()).unwrap();

    b.iter(|| r.values::<f32>(None, None).unwrap())
}

#[bench]
fn read_2d_chunked_nat(b: &mut Bencher) {
    let h = hdf5::File::open("tests/data/chunked_oneD.h5").unwrap();
    let d = h.dataset("d_4_chunks").unwrap();

    b.iter(|| d.read_raw::<f32>().unwrap())
}

