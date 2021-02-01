//! # Data Attribute Structure
//!
//! DAS responses contain additional information about each variable like _fill value_ or history
//! fields.
//!
//! DAS responses are static once constructed from a source.
use std::fmt::{self, Write};
use bytes::Bytes;

/// DAS (Data Attribute Structure)
pub struct Das(Bytes);

#[derive(Debug)]
pub struct Attribute {
    pub name: String,
    pub value: AttrValue,
}

#[derive(Debug, Clone)]
pub enum AttrValue {
    Str(String),
    Float(f32),
    Floats(Vec<f32>),
    Double(f64),
    Doubles(Vec<f64>),
    Ushort(u16),
    Ushorts(Vec<u16>),
    Short(i16),
    Shorts(Vec<i16>),
    Uint(u32),
    Uints(Vec<u32>),
    Int(i32),
    Ints(Vec<i32>),
    Uchar(u8),
    Unimplemented(String),
    Ignored(String),
}

/// File type handlers or readers should implement this trait so that a DAS structure can be built.
pub trait ToDas {
    /// Whether dataset has global attributes.
    fn has_global_attributes(&self) -> bool;

    /// Global attributes in dataset.
    fn global_attributes(&self) -> Box<dyn Iterator<Item = Attribute>>;

    /// Variables in dataset.
    fn variables(&self) -> Box<dyn Iterator<Item = String>>;

    /// Attributes for variable in dataset.
    fn variable_attributes(&self, variable: &str) -> Box<dyn Iterator<Item = Attribute>>;
}

impl fmt::Display for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use AttrValue::*;

        match &self.value {
            Str(s) => write!(f, "String {} \"{}\";", self.name, s.escape_default()),

            Float(v) => write!(f, "Float32 {} {:+.1E};", self.name, v),

            Floats(v) => write!(
                f,
                "Float32 {} {};",
                self.name,
                v.iter()
                    .map(|f| format!("{:+.1E}", f))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),

            Double(v) => write!(f, "Float64 {} {:+.1E};", self.name, v),

            Doubles(v) => write!(
                f,
                "Float64 {} {};",
                self.name,
                v.iter()
                    .map(|f| format!("{:+.1E}", f))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),

            Short(v) => write!(f, "Int16 {} {};", self.name, v),

            Shorts(v) => write!(
                f,
                "Int16 {} {};",
                self.name,
                v.iter()
                    .map(|f| format!("{}", f))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),

            Int(v) => write!(f, "Int32 {} {};", self.name, v),

            Ints(v) => write!(
                f,
                "Int32 {} {};",
                self.name,
                v.iter()
                    .map(|f| format!("{}", f))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),

            Uchar(n) => write!(f, "Byte {} {};", self.name, n),

            Ignored(n) => {
                debug!("Ignored (hidden) DAS field: {:?}: {:?}", self.name, n);
                write!(f, "")
            }

            Unimplemented(v) => {
                debug!("Unimplemented attribute: {:?}: {:?}", self.name, v);
                write!(f, "")
            }

            v => {
                debug!("Unimplemented DAS field: {:?}: {:?}", self.name, v);
                write!(f, "")
            }
        }
    }
}

// Tedious to use TryFrom because of: https://github.com/rust-lang/rust/issues/50133
impl<T> From<T> for Das
where
    T: ToDas,
{
    fn from(dataset: T) -> Self {
        let mut das: String = "Attributes {\n".to_string();

        if dataset.has_global_attributes() {
            writeln!(das, "{:4}NC_GLOBAL {{", "").unwrap();

            for a in dataset
                .global_attributes()
                .filter(|a| !matches!(a.value, AttrValue::Unimplemented(_) | AttrValue::Ignored(_)))
            {
                writeln!(das, "{:8}{}", "", a).unwrap();
            }

            writeln!(das, "{:4}}}", "").unwrap();
        }

        for var in dataset.variables() {
            writeln!(das, "{:4}{} {{", "", var).unwrap();

            for a in dataset
                .variable_attributes(&var)
                .filter(|a| !matches!(a.value, AttrValue::Unimplemented(_) | AttrValue::Ignored(_)))
            {
                writeln!(das, "{:8}{}", "", a).unwrap();
            }

            writeln!(das, "    }}").unwrap();
        }

        write!(das, "}}").unwrap();

        trace!("Generated DAS: {}", das);

        Das(Bytes::from(das))
    }
}

impl fmt::Display for Das {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_str())
    }
}

impl Das {
    pub fn bytes(&self) -> Bytes {
        self.0.clone()
    }

    pub fn as_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.0)
    }
}
