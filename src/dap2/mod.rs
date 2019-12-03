use anyhow;

fn parse_slice(s: &str) -> anyhow::Result<Vec<usize>> {
    match s.split(":").map(|h| h.parse::<usize>())
        .collect::<Result<Vec<usize>,_>>()
        .map_err(|_| anyhow!("Failed to parse index")) {
            Ok(v) => match v.len() {
                l if l <= 3 => Ok(v),
                _ => Err(anyhow!("Too many values to unpack."))
            },
            e => e
        }
}

pub fn parse_hyberslab(s: &str) -> anyhow::Result<Vec<Vec<usize>>> {
    if s.len() < 3 || !s.starts_with("[") || !s.ends_with("]") {
        return Err(anyhow!("Hyberslab missing brackets"));
    }

    s.split("]")
        .filter(|slab| slab.len() != 0)
        .map(|slab| {
            if slab.starts_with("[") {
                parse_slice(&slab[1..])
            } else {
                return Err(anyhow!("Missing start bracket"));
            }
        }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hyperslab() {
        assert_eq!(parse_hyberslab("[0:30]").unwrap(), [[0, 30]]);
    }

    #[test]
    fn test_stride() {
        assert_eq!(parse_hyberslab("[0:2:30]").unwrap(), [[0, 2, 30]]);
    }

    #[test]
    fn too_many_values() {
        assert!(parse_hyberslab("[0:3:4:40]").is_err());
    }

    #[test]
    fn too_wrong_indx() {
        assert!(parse_hyberslab("[0:a:40]").is_err());
    }

    #[test]
    fn test_multidim() {
        assert_eq!(parse_hyberslab("[0][1]").unwrap(), [[0], [1]]);
    }

    #[test]
    fn test_multidim_slice() {
        assert_eq!(parse_hyberslab("[0:30][1][0:1200]").unwrap(), vec!(vec![0, 30], vec![1], vec![0, 1200]));
    }
}
