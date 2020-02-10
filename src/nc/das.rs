use std::fmt;
use std::sync::Arc;

/// Constructs a DAS (Data Attribute Structure) string from
/// NetCDF file. The DAS string is static and must be regenerated
/// if the file changes.
pub struct NcDas {
    das: String,
}

impl fmt::Display for NcDas {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.das.clone())
    }
}

impl NcDas {
    fn format_attr(indent: usize, a: &netcdf::Attribute) -> String {
        use netcdf::attribute::AttrValue::*;

        match a.value() {
            Ok(Str(s)) => format!(
                "{}String {} \"{}\";\n",
                " ".repeat(indent),
                a.name(),
                s.escape_default()
            ),
            Ok(Float(f)) => format!("{}Float32 {} {:+E};\n", " ".repeat(indent), a.name(), f),
            Ok(Floats(f)) => format!(
                "{}Float32 {} {};\n",
                " ".repeat(indent),
                a.name(),
                f.iter()
                    .map(|f| format!("{:+E}", f))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Ok(Double(f)) => format!("{}Float64 {} {:+E};\n", " ".repeat(indent), a.name(), f),
            Ok(Doubles(f)) => format!(
                "{}Float64 {} {};\n",
                " ".repeat(indent),
                a.name(),
                f.iter()
                    .map(|f| format!("{:+E}", f))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Ok(Short(f)) => format!("{}Int16 {} {};\n", " ".repeat(indent), a.name(), f),
            Ok(Int(f)) => format!("{}Int32 {} {};\n", " ".repeat(indent), a.name(), f),
            Ok(Ints(f)) => format!(
                "{}Int32 {} {};\n",
                " ".repeat(indent),
                a.name(),
                f.iter()
                    .map(|f| format!("{}", f))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Ok(Uchar(n)) => format!("{}Byte {} {};\n", " ".repeat(indent), a.name(), n),
            Ok(v) => format!(
                "{}Unimplemented {} {:?};\n",
                " ".repeat(indent),
                a.name(),
                v
            ),
            Err(_) => "Err".to_string(),
        }
    }

    pub fn build(nc: &Arc<netcdf::File>) -> anyhow::Result<NcDas> {
        let indent = 4;
        let mut das: String = "Attributes {\n".to_string();

        if nc.attributes().next().is_some() {
            das.push_str("    NC_GLOBAL {\n");
            das.push_str(
                &nc.attributes()
                    .map(|aa| NcDas::format_attr(2 * indent, &aa))
                    .collect::<String>(),
            );
            // NcDas::push_attr(2*indent, &mut das, nc.attributes());
            das.push_str("    }\n");
        }

        for var in nc.variables() {
            das.push_str(&format!("    {} {{\n", var.name()));
            das.push_str(
                &var.attributes()
                    .map(|aa| NcDas::format_attr(2 * indent, &aa))
                    .collect::<String>(),
            );
            das.push_str("    }\n");
        }

        // TODO: Groups

        // XXX: maybe not needed for RO?
        // if nc.dimensions().any(|d| d.is_unlimited()) {
        //     das.push_str("    DODS_EXTRA {\n");
        //     for dim in nc.dimensions() {
        //         das.push_str(&format!("        String Unlimited_Dimension \"{}\";\n", dim.name()));
        //     }
        //     das.push_str("    }\n");
        // }

        das.push_str("}");

        Ok(NcDas { das })
    }
}
