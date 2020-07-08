use std::path::{Path, PathBuf};

use bytes::Bytes;
use roxmltree::Node;
use walkdir::WalkDir;

use hidefix::reader::stream;
use crate::hdf5::HDF5File;

mod dds;
mod member;
use member::NcmlMember;

/// # NCML aggregated datasets
///
/// Reference: https://www.unidata.ucar.edu/software/netcdf-java/current/ncml/Aggregation.html
///
/// ## JoinExisting
///
/// The aggregating dimension must already have a coordinate variable. Only the slowest varying or outer dimension
/// (first index) may be joined.
///
/// No handling of overlapping coordinate variable is done, it is concatenated in order listed.
pub struct NcmlDataset {
    path: PathBuf,
    das: dap2::Das,
    dds: dap2::Dds,
    /// Aggregation dimension
    dimension: String,
    coordinates: CoordinateVariable,
    modified: std::time::SystemTime,
    members: Vec<NcmlMember>,
}

impl NcmlDataset {
    pub async fn open<P>(path: P) -> anyhow::Result<NcmlDataset>
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

        members.sort_by(|a, b| {
            a.rank
                .partial_cmp(&b.rank)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        ensure!(members.len() > 0, "no members in aggregate.");

        let das = {
            // DAS should be the same regardless of files, using first member.
            let path = &members[0].path;
            let hf = HDF5File(hdf5::File::open(path)?, path.to_path_buf());
            (&hf).into()
        };

        let dds = dds::NcmlDdsBuilder::new(
            hdf5::File::open(path)?,
            path.into(),
            dimension.clone(),
            members[0].n,
        )
        .into();

        let coordinates = CoordinateVariable::from(&members, &dimension).await?;

        Ok(NcmlDataset {
            path: path.into(),
            das,
            dds,
            dimension,
            coordinates,
            modified,
            members,
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
                                entry
                                    .ok()
                                    .map(|entry| {
                                        entry
                                            .metadata()
                                            .ok()
                                            .map(|m| {
                                                if m.is_file()
                                                    && entry
                                                        .path()
                                                        .to_str()
                                                        .map(|s| {
                                                            s.ends_with(sf)
                                                                && !ignore
                                                                    .map(|i| s.contains(i))
                                                                    .unwrap_or(false)
                                                        })
                                                        .unwrap_or(false)
                                                {
                                                    Some(entry.into_path())
                                                } else {
                                                    None
                                                }
                                            })
                                            .flatten()
                                    })
                                    .flatten()
                            })
                            .map(|path| {
                                std::fs::canonicalize(path)
                                    .map_err(|e| anyhow!("failed to scan member: {:?}", e))
                            })
                            .collect::<Result<Vec<_>, _>>()
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

/// The coordinate variable is cached since it is always requested and requires all files to be
/// opened and read.
pub struct CoordinateVariable {
    bytes: Bytes,
    /// Data type size
    dsz: usize,
    n: usize,
}

impl CoordinateVariable {
    pub async fn from(members: &Vec<NcmlMember>, dimension: &str) -> anyhow::Result<CoordinateVariable> {
        use bytes::BytesMut;
        use futures::future;
        use futures::stream::TryStreamExt;

        ensure!(!members.is_empty(), "no members");

        let dsz = members[0].idx.dataset(dimension).ok_or_else(|| anyhow!("dimension dataset not found."))?.dsize;
        let n = members.iter().map(|m| m.n).sum();

        let mut bytes = BytesMut::with_capacity(n * dsz);

        for m in members {
            let ds = m.idx.dataset(dimension).ok_or_else(|| anyhow!("dimension dataset not found."))?;
            let reader = stream::DatasetReader::with_dataset(&ds, &m.path)?;
            let reader = reader.stream(None, None);

            reader.try_for_each(|b| {
                bytes.extend_from_slice(b.as_ref());
                future::ready(Ok(()))
            }).await?;
        }

        Ok(CoordinateVariable {
            bytes: bytes.freeze(),
            dsz,
            n
        })
    }
}
