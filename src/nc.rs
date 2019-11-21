use super::Dataset;

pub struct NcDataset {
    /* a dataset may consist of several files */
    pub filenames: Vec<String>,
    pub mtime: std::time::SystemTime
}

impl Dataset for NcDataset {
    fn name(&self) -> String {
        self.filenames[0].clone()
    }
}

impl NcDataset {
    pub fn open(filename: String) -> std::io::Result<NcDataset> {
        info!("opening: {}", filename);
        use std::fs;

        let md = fs::metadata(&filename)?;
        let mtime = md.modified()?;
        debug!("{}: mtime: {:?}", filename, mtime.elapsed().unwrap());

        Ok(NcDataset {
            filenames: vec![String::from(filename.trim_start_matches("data/"))],
            mtime: mtime
        })
    }
}

