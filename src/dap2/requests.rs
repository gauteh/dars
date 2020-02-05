use super::hyperslab::{count_slab, parse_hyberslab};
use percent_encoding::percent_decode_str;

/// Parse a DODS query consisting of variables and hyperslabs.
pub fn parse_query(
    query: &str,
) -> Result<Vec<(String, Option<Vec<usize>>, Option<Vec<usize>>)>, anyhow::Error> {
    query
        .split(',')
        .filter(|v| v.len() > 0)
        .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
        .map(|v| match v.find("[") {
            Some(i) => {
                let slab = parse_hyberslab(&v[i..])?;
                let v = &v[..i];

                let indices = slab.iter().map(|slab| slab[0]).collect::<Vec<usize>>();
                let counts = slab.iter().map(|v| count_slab(&v)).collect::<Vec<usize>>();

                Ok((v.to_string(), Some(indices), Some(counts)))
            }
            None => Ok((v.to_string(), None, None)),
        })
        .collect::<Result<Vec<_>, _>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_query() {
        assert!(parse_query("").unwrap().len() == 0);
    }

    #[test]
    fn single_var() {
        assert_eq!(
            parse_query("SST").unwrap(),
            [("SST".to_string(), None, None)]
        );
    }

    #[test]
    fn multiple_vars() {
        assert_eq!(
            parse_query("SST,TIME").unwrap(),
            [
                ("SST".to_string(), None, None),
                ("TIME".to_string(), None, None)
            ]
        );
    }

    #[test]
    fn multiple_vars_slab() {
        assert_eq!(
            parse_query("SST[1:10],TIME").unwrap(),
            [
                ("SST".to_string(), Some(vec![1]), Some(vec![10])),
                ("TIME".to_string(), None, None)
            ]
        );
    }
}
