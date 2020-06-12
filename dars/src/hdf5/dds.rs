///! HDF5 files have dimensions defined through various special attributes, linking them using ID's
///! reference lists.
use hdf5_sys as hs;
use libc;
use std::convert::TryInto;
use std::ptr;
use std::slice;

use dap2::dds::{self, Variable};

use super::HDF5File;

fn hdf5_vartype(dtype: &hdf5::Datatype) -> dds::VarType {
    use dds::VarType;

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
        _ => panic!("Unimplemented type: {:?}", dtype),
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

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

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
        unsafe { slice::from_raw_parts(self.ptr as *const _, self.len()) }
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
            .map(|(m, d)| Variable {
                name: m.clone(),
                vartype: hdf5_vartype(&d.dtype().unwrap()),
                dimensions: hdf5_dimensions(m, &d),
            })
            .collect()
    }

    fn dimension_length(&self, dim: &str) -> usize {
        self.0.dataset(dim).unwrap().size()
    }

    fn file_name(&self) -> String {
        self.1.to_string_lossy().to_string()
    }
}
