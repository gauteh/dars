#[derive(Debug)]
pub struct Chunk {
    pub offset: Vec<u64>,
    pub size: u64,
    pub addr: u64,
}
