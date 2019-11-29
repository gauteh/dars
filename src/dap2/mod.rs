use anyhow;

pub fn parse_hyberslab(s: &str) -> anyhow::Result<Vec<u8>> {
    if s.len() < 3 || !s.starts_with("[") || !s.ends_with("]") {
        return Err(anyhow!("Failed to parse hyberslab"));
    }

    let s = &s[1..s.len()-1];

    match s.split(":").map(|h| h.parse::<u8>())
        .collect::<Result<Vec<u8>,_>>()
        .map_err(|_| anyhow!("Failed to parse index")) {
            Ok(v) => match v.len() {
                l if l <= 3 => Ok(v),
                _ => Err(anyhow!("Too many values to unpack."))
            },
            e => e
        }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hyberslab() {
        assert_eq!(parse_hyberslab("[0:30]").unwrap(), [0, 30]);
    }

    #[test]
    fn test_stride() {
        assert_eq!(parse_hyberslab("[0:2:30]").unwrap(), [0, 2, 30]);
    }

    #[test]
    fn too_many_values() {
        assert!(parse_hyberslab("[0:3:4:40]").is_err());
    }

    #[test]
    fn too_wrong_indx() {
        assert!(parse_hyberslab("[0:a:40]").is_err());
    }
}
