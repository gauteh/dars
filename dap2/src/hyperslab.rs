/// Hyperslabs
///
/// OPeNDAP constraint expressions for ranges can consist of:
/// - single values:            [1]     -> [1]
/// - a range:                  [1:5]   -> [1, 2, 3, 4, 5]
/// - a range with strides:     [1:2:7] -> [1, 3, 5, 7]
///                             [1:2:8] -> [1, 3, 5, 7]

pub fn count_slab(slab: &[usize]) -> usize {
    if slab.len() == 1 {
        1
    } else if slab.len() == 2 {
        slab[1] - slab[0] + 1
    } else if slab.len() == 3 {
        (slab[2] - slab[0] + 1) / slab[1]
    } else {
        panic!("too much slabs");
    }
}

fn parse_slice(s: &str) -> anyhow::Result<Vec<usize>> {
    match s
        .split(':')
        .map(|h| h.parse::<usize>())
        .collect::<Result<Vec<usize>, _>>()
        .map_err(|_| anyhow!("Failed to parse index"))
    {
        Ok(v) => match v.len() {
            l if l <= 3 => Ok(v),
            _ => Err(anyhow!("Too many values to unpack.")),
        },
        e => e,
    }
}

pub fn parse_hyperslab(s: &str) -> anyhow::Result<Vec<Vec<usize>>> {
    if s.len() < 3 || !s.starts_with('[') || !s.ends_with(']') {
        return Err(anyhow!("Hyperslab missing brackets"));
    }

    s.split(']')
        .filter(|slab| !slab.is_empty())
        .map(|slab| {
            if slab.starts_with('[') {
                parse_slice(&slab[1..])
            } else {
                Err(anyhow!("Missing start bracket"))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hyperslab() {
        assert_eq!(parse_hyperslab("[0:30]").unwrap(), [[0, 30]]);
    }

    #[test]
    fn test_stride() {
        assert_eq!(parse_hyperslab("[0:2:30]").unwrap(), [[0, 2, 30]]);
    }

    #[test]
    fn too_many_values() {
        assert!(parse_hyperslab("[0:3:4:40]").is_err());
    }

    #[test]
    fn too_wrong_indx() {
        assert!(parse_hyperslab("[0:a:40]").is_err());
    }

    #[test]
    fn test_multidim() {
        assert_eq!(parse_hyperslab("[0][1]").unwrap(), [[0], [1]]);
    }

    #[test]
    fn test_multidim_slice() {
        assert_eq!(
            parse_hyperslab("[0:30][1][0:1200]").unwrap(),
            vec!(vec![0, 30], vec![1], vec![0, 1200])
        );
    }
}

