//! HDF5 files have dimensions defined through various special attributes, linking them using ID's
//! reference lists.
//!
//! String and char type datasets are not supported.
//!
//! There are some types of datasets that apparently should be ignored.
use hdf5_sys as hs;
use hdf5_sys::h5t::hvl_t;
use std::convert::TryInto;

use dap2::dds::{self, Variable};

use super::HDF5File;

pub(crate) fn hdf5_vartype(dtype: &hdf5::Datatype) -> dds::VarType {
    use dds::VarType;
    use hdf5::types::TypeDescriptor;

    match dtype {
        _ if dtype.is::<u8>() => VarType::Byte,
        _ if dtype.is::<u16>() => VarType::UInt16,
        _ if dtype.is::<u32>() => VarType::UInt32,
        _ if dtype.is::<u64>() => VarType::UInt64,
        _ if dtype.is::<i16>() => VarType::Int16,
        _ if dtype.is::<i32>() => VarType::Int32,
        _ if dtype.is::<i64>() => VarType::Int64,
        _ if dtype.is::<f32>() => VarType::Float32,
        _ if dtype.is::<f64>() => VarType::Float64,
        _ => match dtype.to_descriptor() {
            Ok(desc) => match desc {
                TypeDescriptor::FixedAscii(_) => VarType::Unimplemented,
                TypeDescriptor::FixedUnicode(_) => VarType::Unimplemented,
                _ => {
                    trace!("Unimplemented type: {:?}", dtype);
                    VarType::Unimplemented
                }
            },
            _ => {
                trace!("Unimplemented type: {:?}", dtype);
                VarType::Unimplemented
            }
        },
    }
}

pub(crate) fn hdf5_dimensions(m: &str, dataset: &hdf5::Dataset) -> Vec<String> {
    if let Ok(dim_list) = dataset.attr("DIMENSION_LIST") {
        // HDF5 references not yet supported in hdf5-rust:
        // https://github.com/aldanor/hdf5-rust/issues/98
        //
        // relevant examples:
        // - https://bitbucket.hdfgroup.org/projects/HDFFV/repos/hdf5-examples/browse/1_10/C/H5T/h5ex_t_vlenatt.c
        // - https://bitbucket.hdfgroup.org/projects/HDFFV/repos/hdf5-examples/browse/1_10/C/H5T/h5ex_t_objrefatt.c

        hdf5::sync::sync(|| {
            let id = dim_list.id();
            let len = dim_list.size();
            let mut dims = Vec::with_capacity(len);
            unsafe {
                let tid = hs::h5a::H5Aget_type(id);
                let mut rdata = vec![0_u8; std::mem::size_of::<hvl_t>() * len];
                hs::h5a::H5Aread(id, tid, rdata.as_mut_ptr().cast());
                let refs: &[hvl_t] = std::slice::from_raw_parts(rdata.as_ptr().cast(), len);

                for &r in refs {
                    #[cfg(feature = "fast-index")]
                    let dset = hs::h5r::H5Rdereference2(
                        id,
                        hs::h5p::H5P_DEFAULT,
                        hs::h5r::H5R_OBJECT1,
                        r.p,
                    );

                    #[cfg(not(feature = "fast-index"))]
                    let dset = hs::h5r::H5Rdereference2(
                        id,
                        hs::h5p::H5P_DEFAULT,
                        hs::h5r::H5R_OBJECT,
                        r.p,
                    );

                    let sz = 1 + hs::h5i::H5Iget_name(dset, std::ptr::null_mut(), 0);
                    let sz: usize = sz.try_into().unwrap();
                    let mut name = vec![0_u8; sz + 1];
                    hs::h5i::H5Iget_name(dset, name.as_mut_ptr().cast(), sz);

                    let name = std::str::from_utf8(&name[..name.len() - 2]).unwrap();

                    dims.push(name[1..].to_owned()); // remove leading '/'

                    hs::h5d::H5Dclose(dset);
                }

                let space = dim_list.space().unwrap();
                hs::h5t::H5Treclaim(
                    tid,
                    space.id(),
                    hs::h5p::H5P_DEFAULT,
                    rdata.as_mut_ptr().cast(),
                );
                hs::h5t::H5Tclose(tid);

                dims
            }
        })
    } else {
        vec![m.to_string()]
    }
}

impl dds::ToDds for &HDF5File {
    fn variables(&self) -> Vec<Variable> {
        self.0
            .group("/")
            .unwrap()
            .member_names()
            .unwrap()
            .iter()
            .map(|m| self.0.dataset(m).map(|d| (m, d)))
            .filter_map(Result::ok)
            .filter(|(_, d)| d.is_chunked() || d.offset().is_some()) // skipping un-allocated datasets.
            .map(|(m, d)| {
                trace!("Variable: {} {:?}", m, hdf5_vartype(&d.dtype().unwrap()));
                Variable::new(
                    m.clone(),
                    hdf5_vartype(&d.dtype().unwrap()),
                    hdf5_dimensions(m, &d),
                    d.shape(),
                )
            })
            .collect()
    }

    fn file_name(&self) -> String {
        self.1.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::super::Hdf5Dataset;
    use crate::data::test_db;
    use dap2::constraint::Constraint;

    #[test]
    fn coads_time() {
        let db = test_db();
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap();

        let c = Constraint::parse("TIME").unwrap();
        let dds = hd.dds.dds(&c).unwrap();
        println!("{}", dds);

        // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds?TIME
        let tds = r#"Dataset {
    Float64 TIME[TIME = 12];
} coads;"#;

        assert_eq!(dds.to_string(), tds);
    }

    #[test]
    fn coads_time_slab() {
        let db = test_db();
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap();

        let c = Constraint::parse("TIME[0:5]").unwrap();
        let dds = hd.dds.dds(&c).unwrap();
        println!("{}", dds);

        // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds?TIME[0:5]
        let tds = r#"Dataset {
    Float64 TIME[TIME = 6];
} coads;"#;

        assert_eq!(dds.to_string(), tds);
    }

    #[test]
    fn coads_sst_struct() {
        let db = test_db();
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap();

        let c = Constraint::parse("SST.SST").unwrap();
        let dds = hd.dds.dds(&c).unwrap();
        println!("{}", dds);

        // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds?SST.SST
        let tds = r#"Dataset {
    Structure {
        Float32 SST[TIME = 12][COADSY = 90][COADSX = 180];
    } SST;
} coads;"#;

        assert_eq!(dds.to_string(), tds);
        assert_eq!(dds.size(), 4 * 12 * 90 * 180);
        assert_eq!(dds.dods_size(), 8 + 4 * 12 * 90 * 180);
    }

    #[test]
    fn coads_sst_struct_span() {
        let db = test_db();
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap();

        let c = Constraint::parse("SST.SST[0:5][0:10][10:20]").unwrap();
        let dds = hd.dds.dds(&c).unwrap();
        println!("{}", dds);

        // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds?SST.SST[0:5][0:10][10:20]
        let tds = r#"Dataset {
    Structure {
        Float32 SST[TIME = 6][COADSY = 11][COADSX = 11];
    } SST;
} coads;"#;

        assert_eq!(dds.to_string(), tds);
        assert_eq!(dds.size(), 4 * 6 * 11 * 11);
        assert_eq!(dds.dods_size(), 8 + 4 * 6 * 11 * 11);
    }

    #[test]
    fn coads_sst_time_struct_span() {
        let db = test_db();
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap();

        let c = Constraint::parse("SST.TIME[0:5]").unwrap();
        let dds = hd.dds.dds(&c).unwrap();
        println!("{}", dds);

        // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds?SST.TIME[0:5]
        let tds = r#"Dataset {
    Structure {
        Float64 TIME[TIME = 6];
    } SST;
} coads;"#;

        assert_eq!(dds.to_string(), tds);
        assert_eq!(dds.size(), 8 * 6);
    }

    #[test]
    fn dimensions_1d() {
        let db = test_db();
        let hd = Hdf5Dataset::open("../data/h5/dims_1d.h5", "1d".into(), &db).unwrap();
        println!("DDS:\n{}", hd.dds.all());

        let res = r#"Dataset {
    Float32 data[x1 = 2];
    Int64 x1[x1 = 2];
} 1d;"#;

        assert_eq!(hd.dds.all().to_string(), res);
    }

    #[test]
    fn dimensions_2d() {
        let db = test_db();
        let hd = Hdf5Dataset::open("../data/h5/dims_2d.h5", "2d".into(), &db).unwrap();
        println!("DDS:\n{}", hd.dds.all());

        let res = r#"Dataset {
    Grid {
     ARRAY:
        Float32 data[x1 = 2][y1 = 3];
     MAPS:
        Int64 x1[x1 = 2];
        Int64 y1[y1 = 3];
    } data;
    Int64 x1[x1 = 2];
    Int64 y1[y1 = 3];
} 2d;"#;

        assert_eq!(hd.dds.all().to_string(), res);
    }
}
