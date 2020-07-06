///! HDF5 files have dimensions defined through various special attributes, linking them using ID's
///! reference lists.
///!
///! String and char type datasets are not supported.
///!
///! There are some types of datasets that apparently should be ignored.
use hdf5_sys as hs;
use libc;
use std::convert::TryInto;
use std::ptr;
use std::slice;

use dap2::dds::{self, Variable};

use super::HDF5File;

fn hdf5_vartype(dtype: &hdf5::Datatype) -> dds::VarType {
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
                _ => panic!("Unimplemented type: {:?}", dtype),
            },
            _ => panic!("Unimplemented type: {:?}", dtype),
        },
    }
}

#[repr(C)]
pub struct VarLenRef {
    ptr: *mut u8,
    len: usize,
    space: hs::h5i::hid_t,
}

impl Drop for VarLenRef {
    #[inline]
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                let memtype = hs::h5t::H5Tvlen_create(*hs::h5t::H5T_STD_REF_OBJ);
                hs::h5d::H5Dvlen_reclaim(
                    memtype,
                    self.space,
                    hs::h5p::H5P_DEFAULT,
                    self.ptr as *mut _,
                );
                libc::free(self.ptr as *mut _)
            };
        }
    }
}

impl Clone for VarLenRef {
    #[inline]
    fn clone(&self) -> Self {
        unsafe { Self::from_bytes(self.space, self.as_bytes()) }
    }
}

impl VarLenRef {
    #[inline]
    unsafe fn from_bytes(space: hs::h5i::hid_t, bytes: &[u8]) -> Self {
        let ptr = libc::malloc(bytes.len()) as *mut _;
        let len = bytes.len() / std::mem::size_of::<hs::h5t::hvl_t>();
        ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
        VarLenRef { ptr, len, space }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[allow(dead_code)]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[allow(dead_code)]
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(
                self.ptr as *const _,
                self.len() * std::mem::size_of::<hs::h5t::hvl_t>(),
            )
        }
    }

    pub fn as_slice(&self) -> &[hs::h5t::hvl_t] {
        unsafe { slice::from_raw_parts(self.ptr as *mut _, self.len()) }
    }
}

fn hdf5_dimensions(m: &str, dataset: &hdf5::Dataset) -> Vec<String> {
    if let Ok(dim_list) = dataset.attribute("DIMENSION_LIST") {
        let id = dim_list.id();

        // HDF5 references not yet supported in hdf5-rust:
        // https://github.com/aldanor/hdf5-rust/issues/98
        //
        // relevant examples:
        // - https://bitbucket.hdfgroup.org/projects/HDFFV/repos/hdf5-examples/browse/1_10/C/H5T/h5ex_t_vlenatt.c
        // - https://bitbucket.hdfgroup.org/projects/HDFFV/repos/hdf5-examples/browse/1_10/C/H5T/h5ex_t_objrefatt.c
        let space = dim_list.space().unwrap();

        hdf5::sync::sync(|| {
            let refs = unsafe {
                let memtype = hs::h5t::H5Tvlen_create(*hs::h5t::H5T_STD_REF_OBJ);
                let rdata = libc::malloc(std::mem::size_of::<hs::h5t::hvl_t>() * dim_list.size());
                hs::h5a::H5Aread(id, memtype, rdata);
                VarLenRef {
                    ptr: rdata as *mut _,
                    len: dim_list.size(),
                    space: space.id(),
                }
            };

            refs.as_slice()
                .iter()
                .map(|r| {
                    let name = unsafe {
                        let obj = hs::h5r::H5Rdereference2(
                            id,
                            hs::h5p::H5P_DEFAULT,
                            hs::h5r::H5R_OBJECT,
                            r.p,
                        );
                        let sz = 1 + hs::h5i::H5Iget_name(obj, std::ptr::null_mut(), 0);
                        let sz: usize = sz.try_into().unwrap();
                        let name = libc::malloc(sz + 1);
                        hs::h5i::H5Iget_name(obj, name as *mut _, sz);

                        let name_s = slice::from_raw_parts(name as *const u8, sz);
                        let name_s = String::from_utf8((&name_s[..name_s.len() - 1]).to_vec());

                        hs::h5o::H5Oclose(obj);
                        libc::free(name);

                        name_s
                    }
                    .unwrap();

                    (&name[1..]).to_string() // remove leading '/'
                })
                .collect()
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
                debug!("Variable: {} {:?}", m, hdf5_vartype(&d.dtype().unwrap()));
                Variable {
                    name: m.clone(),
                    vartype: hdf5_vartype(&d.dtype().unwrap()),
                    dimensions: hdf5_dimensions(m, &d),
                    shape: d.shape().clone(),
                }
            })
            .collect()
    }

    fn file_name(&self) -> String {
        self.1.to_string_lossy().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::super::Hdf5Dataset;
    use dap2::constraint::Constraint;
    use test::Bencher;

    #[bench]
    fn coads(b: &mut Bencher) {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();

        b.iter(|| hd.dds.all().to_string());

        let dds = hd.dds.all().to_string();
        println!("dds: {}", dds);

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
} ../data/coads_climatology.nc4;"#;

        assert_eq!(hd.dds.all().to_string(), tds);
    }

    #[test]
    fn coads_time() {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();

        let c = Constraint::parse("TIME").unwrap();
        let dds = hd.dds.dds(&c).unwrap();
        println!("{}", dds);

        // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds?TIME
        let tds = r#"Dataset {
    Float64 TIME[TIME = 12];
} ../data/coads_climatology.nc4;"#;

        assert_eq!(dds.to_string(), tds);
    }

    #[test]
    fn coads_time_slab() {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();

        let c = Constraint::parse("TIME[0:5]").unwrap();
        let dds = hd.dds.dds(&c).unwrap();
        println!("{}", dds);

        // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds?TIME[0:5]
        let tds = r#"Dataset {
    Float64 TIME[TIME = 6];
} ../data/coads_climatology.nc4;"#;

        assert_eq!(dds.to_string(), tds);
    }

    #[bench]
    fn coads_sst_grid(b: &mut Bencher) {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();

        let c = Constraint::parse("SST").unwrap();
        b.iter(|| hd.dds.dds(&c).unwrap().to_string());
        let dds = hd.dds.dds(&c).unwrap();
        println!("{}", dds);

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
} ../data/coads_climatology.nc4;"#;

        assert_eq!(dds.to_string(), tds);
    }

    #[test]
    fn coads_sst_struct() {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();

        let c = Constraint::parse("SST.SST").unwrap();
        let dds = hd.dds.dds(&c).unwrap();
        println!("{}", dds);

        // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds?SST.SST
        let tds = r#"Dataset {
    Structure {
        Float32 SST[TIME = 12][COADSY = 90][COADSX = 180];
    } SST;
} ../data/coads_climatology.nc4;"#;

        assert_eq!(dds.to_string(), tds);
        assert_eq!(dds.size(), 4 * 12 * 90 * 180);
        assert_eq!(dds.dods_size(), 8 + 4 * 12 * 90 * 180);
    }

    #[test]
    fn coads_sst_struct_span() {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();

        let c = Constraint::parse("SST.SST[0:5][0:10][10:20]").unwrap();
        let dds = hd.dds.dds(&c).unwrap();
        println!("{}", dds);

        // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds?SST.SST[0:5][0:10][10:20]
        let tds = r#"Dataset {
    Structure {
        Float32 SST[TIME = 6][COADSY = 11][COADSX = 11];
    } SST;
} ../data/coads_climatology.nc4;"#;

        assert_eq!(dds.to_string(), tds);
        assert_eq!(dds.size(), 4 * 6 * 11 * 11);
        assert_eq!(dds.dods_size(), 8 + 4 * 6 * 11 * 11);
    }

    #[test]
    fn coads_sst_time_struct_span() {
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap();

        let c = Constraint::parse("SST.TIME[0:5]").unwrap();
        let dds = hd.dds.dds(&c).unwrap();
        println!("{}", dds);

        // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.dds?SST.TIME[0:5]
        let tds = r#"Dataset {
    Structure {
        Float64 TIME[TIME = 6];
    } SST;
} ../data/coads_climatology.nc4;"#;

        assert_eq!(dds.to_string(), tds);
        assert_eq!(dds.size(), 8 * 6);
    }

    #[test]
    fn dimensions_1d() {
        let hd = Hdf5Dataset::open("../data/h5/dims_1d.h5").unwrap();
        println!("DDS:\n{}", hd.dds.all());

        let res = r#"Dataset {
    Float32 data[x1 = 2];
    Int64 x1[x1 = 2];
} ../data/h5/dims_1d.h5;"#;

        assert_eq!(hd.dds.all().to_string(), res);
    }

    #[test]
    fn dimensions_2d() {
        let hd = Hdf5Dataset::open("../data/h5/dims_2d.h5").unwrap();
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
} ../data/h5/dims_2d.h5;"#;

        assert_eq!(hd.dds.all().to_string(), res);
    }
}
