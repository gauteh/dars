use std::fmt;

/// DAS (Data Attribute Structure)
pub struct Das(String);

#[derive(Debug)]
pub struct Attribute {
    name: String,
    value: AttrValue
}

#[derive(Debug)]
pub enum AttrValue {
    Str(String),
    Float(f32),
    Floats(Vec<f32>),
    Double(f64),
    Doubles(Vec<f64>),
    Short(i16),
    Int(i32),
    Ints(Vec<i32>),
    Uchar(u8),
    Unimplemented(String),
}

pub trait ToDas {
    /// Whether dataset has global attributes.
    fn has_global_attributes(&self) -> bool;

    /// Global attributes in dataset.
    fn global_attributes(&self) -> Box<dyn Iterator<Item = Attribute>>;

    /// Variables in dataset.
    fn variables(&self) -> Box<dyn Iterator<Item = &str>>;

    /// Attributes for variable in dataset.
    fn variable_attributes(&self, variable: &str) -> Box<dyn Iterator<Item = Attribute>>;
}

impl<T> From<T> for Das
where
    T: ToDas,
{
    fn from(dataset: T) -> Self {
        let indent = 4;
        let mut das: String = "Attributes {\n".to_string();

        if dataset.has_global_attributes() {
            das.push_str(&format!("{}NC_GLOBAL {{\n",
                    " ".repeat(indent)));
            das.push_str(
                &dataset.global_attributes()
                    .map(|a| Das::format_attr(2 * indent, a))
                    .collect::<String>());
            das.push_str(
                &format!("{}}}\n", " ".repeat(indent)));
        }


        for var in dataset.variables() {
            das.push_str(&format!("    {} {{\n", var));
            das.push_str(
                &dataset.variable_attributes(var)
                    .map(|a| Das::format_attr(2 * indent, a))
                    .collect::<String>(),
            );
            das.push_str("    }\n");
        }

        Das(das)
    }
}

impl fmt::Display for Das {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Das {
    fn format_attr(indent: usize, a: Attribute) -> String {
        use AttrValue::*;

        match a.value {
            Str(s) => format!(
                "{}String {} \"{}\";\n",
                " ".repeat(indent),
                a.name,
                s.escape_default()
            ),
            Float(f) => format!("{}Float32 {} {:+E};\n", " ".repeat(indent), a.name, f),
            Floats(f) => format!(
                "{}Float32 {} {};\n",
                " ".repeat(indent),
                a.name,
                f.iter()
                    .map(|f| format!("{:+E}", f))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Double(f) => format!("{}Float64 {} {:+E};\n", " ".repeat(indent), a.name, f),
            Doubles(f) => format!(
                "{}Float64 {} {};\n",
                " ".repeat(indent),
                a.name,
                f.iter()
                    .map(|f| format!("{:+E}", f))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Short(f) => format!("{}Int16 {} {};\n", " ".repeat(indent), a.name, f),
            Int(f) => format!("{}Int32 {} {};\n", " ".repeat(indent), a.name, f),
            Ints(f) => format!(
                "{}Int32 {} {};\n",
                " ".repeat(indent),
                a.name,
                f.iter()
                    .map(|f| format!("{}", f))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Uchar(n) => format!("{}Byte {} {};\n", " ".repeat(indent), a.name, n),

            v => format!(
                "{}Unimplemented {} {:?};\n",
                " ".repeat(indent),
                a.name,
                v
            )
        }
    }
}

