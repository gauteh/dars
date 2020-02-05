use std::cmp::Ordering;

/// A HDF5 chunk. A chunk is read and written in its entirety by the HDF5 library. This is
/// usually necessary since the chunk can be compressed and filtered.
///
/// > Note: The official HDF5 library uses a 1MB dataset cache by default.
///
/// [HDF5 chunking](https://support.hdfgroup.org/HDF5/doc/Advanced/Chunking/index.html).
#[derive(Debug, Eq)]
pub struct Chunk {
    pub offset: Vec<u64>,
    pub size: u64,
    pub addr: u64,
}

impl Ord for Chunk {
    fn cmp(&self, other: &Self) -> Ordering {
        for (s, o) in self.offset.iter().zip(&other.offset) {
            match s.cmp(&o) {
                Ordering::Greater => return Ordering::Greater,
                Ordering::Less => return Ordering::Less,
                Ordering::Equal => ()
            }
        }

        Ordering::Equal
    }
}

impl PartialOrd for Chunk {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Chunk {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering() {
        let mut v = vec![
            Chunk { offset: vec![10, 0, 0], size: 5, addr: 5 },
            Chunk { offset: vec![0, 0, 0], size: 10, addr: 50 },
            Chunk { offset: vec![10, 1, 0], size: 1, addr: 1 }
        ];
        v.sort();

        assert_eq!(v[0].offset, vec![0, 0, 0]);
        assert_eq!(v[1].offset, vec![10, 0, 0]);
        assert_eq!(v[2].offset, vec![10, 1, 0]);
    }
}
