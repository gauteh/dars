use dap2::das;

use super::HDF5File;

impl das::ToDas for &HDF5File {
    fn has_global_attributes(&self) -> bool {
        self.0.attribute_names().is_ok()
    }

    fn global_attributes(&self) -> Box<dyn Iterator<Item = das::Attribute>> {
        Box::new(
            self.0
                .attribute_names()
                .unwrap()
                .iter()
                .map(|n| h5attr_to_das(n, self.0.attribute(n).unwrap()))
                .collect::<Vec<das::Attribute>>()
                .into_iter(),
        )
    }

    fn variables(&self) -> Box<dyn Iterator<Item = String>> {
        Box::new(
            self.0
                .group("/")
                .unwrap()
                .member_names()
                .unwrap()
                .iter()
                .map(|m| self.0.dataset(m).map(|d| (m, d)))
                .filter_map(Result::ok)
                .filter(|(_, d)| d.is_chunked() || d.offset().is_some()) // skipping un-allocated datasets.
                .map(|(m, _)| m.clone())
                .collect::<Vec<String>>()
                .into_iter(),
        )
    }

    fn variable_attributes(&self, variable: &str) -> Box<dyn Iterator<Item = das::Attribute>> {
        Box::new(
            self.0
                .dataset(variable)
                .unwrap()
                .attribute_names()
                .unwrap()
                .iter()
                .filter_map(|n| {
                    Result::ok(
                        self.0
                            .dataset(variable)
                            .unwrap()
                            .attribute(n)
                            .map(|a| h5attr_to_das(n, a)),
                    )
                })
                .collect::<Vec<das::Attribute>>()
                .into_iter(),
        )
    }
}

fn h5attr_to_das(n: &str, a: hdf5::Attribute) -> das::Attribute {
    use das::AttrValue::*;
    use hdf5::types::TypeDescriptor as h5t;
    use hdf5::types::{FloatSize, IntSize};

    if n == "DIMENSION_LIST"
        || n == "REFERENCE_LIST"
        || n == "_NCProperties"
        || n == "_nc3_strict"
        || n == "_Netcdf4Dimid"
        || n == "CLASS"
        || n == "NAME"
    {
        das::Attribute {
            name: n.to_string(),
            value: Ignored("Dimension metadata".into()),
        }
    } else if let Ok(dtype) = a.dtype().unwrap().to_descriptor() {
        das::Attribute {
            name: n.to_string(),
            value: if a.is_scalar() {
                match dtype {
                    h5t::Integer(IntSize::U2) => Short(a.read_scalar().unwrap()),
                    h5t::Integer(IntSize::U4) => Int(a.read_scalar().unwrap()),
                    h5t::Float(FloatSize::U4) => Float(a.read_scalar().unwrap()),
                    h5t::Float(FloatSize::U8) => Double(a.read_scalar().unwrap()),
                    h5t::FixedAscii(n) => { fixedascii_to_string(&*a).map(|s| Str(s)).unwrap_or_else(|_| Unimplemented(format!("(fixed ascii) unsupported: {:?}", n))) },
                    dtype => Unimplemented(format!("(scalar) {:?}", dtype)),
                }
            } else {
                match dtype {
                    h5t::Integer(IntSize::U2) => Shorts(a.read_raw().unwrap()),
                    h5t::Integer(IntSize::U4) => Ints(a.read_raw().unwrap()),
                    h5t::Float(FloatSize::U4) => Floats(a.read_raw().unwrap()),
                    h5t::Float(FloatSize::U8) => Doubles(a.read_raw().unwrap()),
                    dtype => Unimplemented(format!("(vector) {:?}", dtype)),
                }
            },
        }
    } else {
        das::Attribute {
            name: n.to_string(),
            value: Unimplemented("Unimplemented dtype".into()),
        }
    }
}

macro_rules! branch_array_impl {
    ($a:expr, $u:expr, $( $ns:expr ),*) => {
        match $u {
            $(
                $ns => Some(fixedascii_attr_value::<[u8; $ns]>($a)),
            )*
            _ => None
        }
    };
}

fn fixedascii_to_string(c: &hdf5::Container) -> Result<String, anyhow::Error> {
    if let Ok(hdf5::types::TypeDescriptor::FixedAscii(n)) = c.dtype()?.to_descriptor() {
        // values from: hdf5_types.rs/array.rs
        branch_array_impl!(
            c, n, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
            23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
            45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 70, 72,
            80, 90, 96, 100, 110, 120, 128, 130, 140, 150, 160, 170, 180, 190, 192, 200, 210, 220,
            224, 230, 240, 250, 256, 300, 384, 400, 500, 512, 600, 700, 768, 800, 900, 1000, 1024,
            2048, 4096, 8192, 16384, 32768
        )
        .ok_or_else(|| anyhow!("Unsupported FixedAscii length: {}", n))?
    } else {
        Err(anyhow!("not FixedAscii"))
    }
}

fn fixedascii_attr_value<T: hdf5::types::Array<Item = u8>>(
    c: &hdf5::Container,
) -> Result<String, anyhow::Error> {
    Ok(c.read_scalar::<hdf5::types::FixedAscii<T>>()?
        .as_str()
        .to_owned())
}

#[cfg(test)]
mod tests {
    use super::super::Hdf5Dataset;
    use crate::data::test_db;
    use test::Bencher;

    #[bench]
    fn coads(b: &mut Bencher) {
        let db = test_db();
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap();

        b.iter(|| hd.das.to_string());

        println!("DAS:\n{}", hd.das);
    }

    #[test]
    fn coads_das() {
        let db = test_db();
        let hd = Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap();

        // from: https://remotetest.unidata.ucar.edu/thredds/dodsC/testdods/coads_climatology.nc.das
        // re-ordered fields and removed unlimited dimension TIME.
        let tds = r#"Attributes {
    NC_GLOBAL {
        String history "FERRET V4.30 (debug/no GUI) 15-Aug-96";
    }
    AIRT {
        Float32 _FillValue -1.0E34;
        String history "From coads_climatology";
        String long_name "AIR TEMPERATURE";
        Float32 missing_value -1.0E34;
        String units "DEG C";
    }
    COADSX {
        String modulo " ";
        String point_spacing "even";
        String units "degrees_east";
    }
    COADSY {
        String point_spacing "even";
        String units "degrees_north";
    }
    SST {
        Float32 _FillValue -1.0E34;
        String history "From coads_climatology";
        String long_name "SEA SURFACE TEMPERATURE";
        Float32 missing_value -1.0E34;
        String units "Deg C";
    }
    TIME {
        String modulo " ";
        String time_origin "1-JAN-0000 00:00:00";
        String units "hour since 0000-01-01 00:00:00";
    }
    UWND {
        Float32 _FillValue -1.0E34;
        String history "From coads_climatology";
        String long_name "ZONAL WIND";
        Float32 missing_value -1.0E34;
        String units "M/S";
    }
    VWND {
        Float32 _FillValue -1.0E34;
        String history "From coads_climatology";
        String long_name "MERIDIONAL WIND";
        Float32 missing_value -1.0E34;
        String units "M/S";
    }
}"#;

        assert_eq!(tds, hd.das.to_string());
    }
}
