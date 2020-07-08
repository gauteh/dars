use std::path::{Path, PathBuf};

use bytes::Bytes;
use roxmltree::Node;
use walkdir::WalkDir;

use crate::hdf5::HDF5File;

mod member;
use member::NcmlMember;

/// The coordinate dimension is cached since it is always requested and requires all files to be
/// opened and read.
pub struct CoordinateDimension {
    bytes: Bytes,
    /// Data type size
    dsz: usize,
}

/// # NCML aggregated datasets
///
/// Reference: https://www.unidata.ucar.edu/software/netcdf-java/current/ncml/Aggregation.html
///
/// ## JoinExisting
///
/// The aggregating dimension must already have a coordinate variable. Only the outer (slowest varying) dimension
/// (first index) may be joined.
///
/// No handling of overlapping coordinate variable is done, it is concatenated in order listed.
pub struct NcmlDataset {
    path: PathBuf,
    das: dap2::Das,
    // dds: dap2::Dds,
    /// Aggregation dimension
    dimension: String,

    modified: std::time::SystemTime,
    members: Vec<NcmlMember>,
}

impl NcmlDataset {
    pub fn open<P>(path: P) -> anyhow::Result<NcmlDataset>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let modified = std::fs::metadata(path)?.modified()?;
        info!("Loading {:?}..", path);

        // Parse NCML file.
        let xml = std::fs::read_to_string(&path)?;
        let xml = roxmltree::Document::parse(&xml)?;
        let root = xml.root_element();

        let aggregation = root
            .first_element_child()
            .ok_or_else(|| anyhow!("no aggregation tag found"))?;
        ensure!(
            aggregation.tag_name().name() == "aggregation",
            "expected aggregation tag"
        );

        let aggregation_type = aggregation
            .attribute("type")
            .ok_or_else(|| anyhow!("aggregation type not specified"))?;
        ensure!(
            aggregation_type == "joinExisting",
            "only 'joinExisting' type aggregation supported"
        );

        // TODO: only available on certain aggregation types
        let dimension = aggregation
            .attribute("dimName")
            .ok_or_else(|| anyhow!("aggregation dimension not specified"))?
            .to_string();

        let files = NcmlDataset::get_member_files(path.parent(), &aggregation)?;
        let mut members = files
            .iter()
            .flatten()
            .map(|p| NcmlMember::open(p, &dimension))
            .collect::<Result<Vec<NcmlMember>, _>>()?;

        members.sort_by(|a, b|
            a.rank
                .partial_cmp(&b.rank)
                .unwrap_or(std::cmp::Ordering::Equal));

        ensure!(members.len() > 0, "no members in aggregate.");

        let das = {
            let path = &members[0].path;
            let hf = HDF5File(hdf5::File::open(path)?, path.to_path_buf());

            (&hf).into()
        };

        // TODO: DDS
        // TODO: Read coordinate variable (use cache reader, or just stream..)
        // TODO: Create streamer (have code I think)

        Ok(NcmlDataset {
            path: path.into(),
            das,
            // dds,
            dimension,
            modified,
            members
        })
    }

    fn get_member_files(base: Option<&Path>, aggregation: &Node) -> anyhow::Result<Vec<PathBuf>> {
        aggregation
            .children()
            .filter(|c| c.is_element())
            .filter_map(|e| match e.tag_name().name() {
                "netcdf" => e.attribute("location").map(|l| {
                    let l = PathBuf::from(l);
                    if l.is_relative() {
                        Ok(vec![base.map_or(l.clone(), |b| b.join(l))])
                    } else {
                        Ok(vec![l])
                    }
                }),
                "scan" => e.attribute("location").map(|l| {
                    let l: PathBuf = match PathBuf::from(l) {
                        l if l.is_relative() => base.map_or(l.clone(), |b| b.join(l)),
                        l => l,
                    };

                    if let Some(sf) = e.attribute("suffix") {
                        let ignore = e.attribute("ignore");
                        trace!("Scanning {:?}, ignore: {:?}, suffix: {}", l, ignore, sf);
                        WalkDir::new(l)
                            .follow_links(true)
                            .into_iter()
                            .filter_entry(|entry| {
                                !entry
                                    .file_name()
                                    .to_str()
                                    .map(|s| s.starts_with('.'))
                                    .unwrap_or(false)
                            })
                            .filter_map(|entry| {
                                entry.ok()
                                    .map(|entry| {
                                    entry.metadata().ok()
                                        .map(|m| {
                                        if m.is_file()
                                            && entry
                                            .path()
                                            .to_str()
                                            .map(|s| s.ends_with(sf) && !ignore.map(|i| s.contains(i)).unwrap_or(false))
                                            .unwrap_or(false) {
                                                Some(entry.into_path())
                                            } else {
                                                None
                                            }
                                    }).flatten()
                                }).flatten()
                            })
                            .map(|path|
                                std::fs::canonicalize(path)
                                    .map_err(|e| anyhow!("failed to scan member: {:?}", e)))
                            .collect::<Result<Vec<_>,_>>()
                    } else {
                        Err(anyhow!("no suffix specified in ncml scan tag"))
                    }
                }),
                t => {
                    error!("unknown tag: {}", t);
                    None
                }
            })
            .collect::<Result<Vec<Vec<_>>, _>>()
                .map(|vecs| vecs.into_iter().flatten().collect())
    }
}
