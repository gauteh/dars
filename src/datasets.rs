use async_trait::async_trait;
use colored::Colorize;
use hyper::{Body, Response, StatusCode};
use notify::{RecommendedWatcher, Watcher};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use super::{nc::NcDataset, ncml::NcmlDataset};

pub struct Data {
    pub root: PathBuf,
    pub datasets: HashMap<String, Box<dyn Dataset + Send + Sync>>,
    watcher: Option<RecommendedWatcher>,
}

enum DsRequestType {
    Das,
    Dds,
    Dods,
    Nc,
    Unknown,
}

struct DsRequest(String, DsRequestType);

impl Data {
    pub fn new() -> Data {
        Data {
            root: "./".into(),
            datasets: HashMap::new(),
            watcher: None,
        }
    }

    pub fn make_key(&self, p: &Path) -> String {
        if self.root.to_string_lossy().ends_with("/") {
            // remove root
            p.to_str()
                .unwrap()
                .trim_start_matches(self.root.to_str().unwrap())
                .to_string()
        } else {
            p.to_str().unwrap().to_string()
        }
    }

    pub fn init_root<P>(&mut self, root: P, watch: bool) -> ()
    where
        P: Into<PathBuf>,
    {
        let root = root.into();
        self.root = root.clone();
        self.datasets.clear();

        info!(
            "Scanning {} for datasets..",
            self.root.to_string_lossy().yellow()
        );

        for entry in WalkDir::new(&self.root)
            .follow_links(true)
            .into_iter()
            .filter_entry(|entry| {
                !entry
                    .file_name()
                    .to_str()
                    .map(|s| s.starts_with("."))
                    .unwrap_or(false)
            })
        {
            if let Ok(entry) = entry {
                match entry.metadata() {
                    Ok(m) if m.is_file() => match entry.path().extension() {
                        Some(ext) if ext == "nc" => match NcDataset::open(entry.path()) {
                            Ok(ds) => {
                                self.datasets
                                    .insert(self.make_key(entry.path().into()), Box::new(ds));
                            }
                            Err(e) => warn!("Could not open: {:?} ({:?})", entry.path(), e),
                        },
                        Some(ext) if ext == "ncml" => {
                            match NcmlDataset::open(entry.path(), watch) {
                                Ok(ds) => {
                                    self.datasets
                                        .insert(self.make_key(entry.path().into()), Box::new(ds));
                                }
                                Err(e) => warn!("Could not open: {:?} ({:?})", entry.path(), e),
                            }
                        }
                        _ => (),
                    },
                    _ => (),
                }
            }
        }

        if watch {
            info!("Watching {:?}", root);
            self.watcher = Some(
                Watcher::new_immediate(|res| match res {
                    Ok(event) => Data::data_event(event),
                    Err(e) => error!("watch error: {:?}", e),
                })
                .expect("could not watch data root"),
            );

            self.watcher.as_mut().map(|w| {
                w.watch(root, notify::RecursiveMode::Recursive)
                    .expect("could not watch data root")
            });
        }
    }

    pub fn data_event(e: notify::Event) -> () {
        use notify::event::{CreateKind, EventKind::*, RemoveKind};

        match e.kind {
            Create(ck) => match ck {
                CreateKind::File => e.paths.iter().map(|p| Data::reload_file(p)).collect(),
                CreateKind::Folder => unimplemented!("scan dir"),
                _ => (),
            },
            Modify(_mk) => e.paths.iter().map(|p| Data::reload_file(p)).collect(),
            Remove(rk) => match rk {
                RemoveKind::File => e.paths.iter().map(|p| Data::reload_file(p)).collect(),
                RemoveKind::Folder => unimplemented!("scan dir"),
                _ => (),
            },
            _ => debug!("Unhandled event: {:?}", e),
        }
    }

    pub fn reload_file<T>(pb: T)
    where
        T: Borrow<PathBuf>,
    {
        let pb = pb.borrow();
        debug!("Checking file: {:?}", pb);

        if let Some(ext) = pb.extension() {
            if ext == "nc" || ext == "ncml" {
                use super::DATA;

                let mut data = futures::executor::block_on(DATA.write());

                if let Some(fname) = pb.file_name() {
                    if fname.to_string_lossy().starts_with(".") {
                        return;
                    }
                } else {
                    return;
                }

                let pb = if let Ok(pb) = pb.strip_prefix(data.root.canonicalize().unwrap()) {
                    data.root.join(pb)
                } else {
                    warn!("{:?} not in root: {:?}", pb, data.root);
                    return;
                };

                let key = data.make_key(&pb);

                if data.datasets.remove(&key).is_some() {
                    info!("Removed dataset: {}", key);
                }

                if pb.exists() {
                    if ext == "nc" {
                        match NcDataset::open(pb.clone()) {
                            Ok(ds) => {
                                data.datasets.insert(key, Box::new(ds));
                            }
                            Err(e) => warn!("Could not open: {:?} ({:?})", pb, e),
                        }
                    } else if ext == "ncml" {
                        match NcmlDataset::open(pb.clone(), true) {
                            Ok(ds) => {
                                data.datasets.insert(key, Box::new(ds));
                            }
                            Err(e) => warn!("Could not open: {:?} ({:?})", pb, e),
                        }
                    }
                }
            }
        }
    }

    pub async fn datasets(
        &self,
        _req: hyper::Request<Body>,
    ) -> Result<Response<Body>, hyper::http::Error> {
        let mut datasets: Vec<String> = {
            self.datasets
                .keys()
                .map(|s| format!("  /data/{}", s))
                .collect()
        };

        datasets.sort();

        Response::builder().body(Body::from(format!(
            "Index of datasets:\n\n{}\n",
            datasets.join("\n")
        )))
    }

    fn parse_request(ds: String) -> DsRequest {
        if ds.ends_with(".das") {
            DsRequest(
                String::from(ds.trim_end_matches(".das")),
                DsRequestType::Das,
            )
        } else if ds.ends_with(".dds") {
            DsRequest(
                String::from(ds.trim_end_matches(".dds")),
                DsRequestType::Dds,
            )
        } else if ds.ends_with(".dods") {
            DsRequest(
                String::from(ds.trim_end_matches(".dods")),
                DsRequestType::Dods,
            )
        } else if ds.ends_with(".nc") {
            DsRequest(String::from(&ds), DsRequestType::Nc)
        } else {
            DsRequest(String::from(&ds), DsRequestType::Unknown)
        }
    }

    pub async fn dataset(
        &self,
        req: hyper::Request<Body>,
    ) -> Result<Response<Body>, hyper::http::Error> {
        let ds: String = req.uri().path().trim_start_matches("/data/").to_string();
        let DsRequest(ds, dst) = Data::parse_request(ds);

        let ds = {
            match self.datasets.get(&ds) {
                Some(ds) => Some(ds.clone()),
                None => None,
            }
        };

        match ds {
            Some(ds) => match dst {
                DsRequestType::Das => ds.das().await,
                DsRequestType::Dds => ds.dds(req.uri().query().map(|s| s.to_string())).await,
                DsRequestType::Dods => ds.dods(req.uri().query().map(|s| s.to_string())).await,
                DsRequestType::Nc => ds.nc().await,
                _ => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty()),
            },
            None => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty()),
        }
    }
}

pub enum FileEvent {
    ScanMember(PathBuf, notify::Event),
}

#[async_trait]
pub trait Dataset {
    fn name(&self) -> String;

    async fn das(&self) -> Result<Response<Body>, hyper::http::Error>;
    async fn dds(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error>;
    async fn dods(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error>;
    async fn nc(&self) -> Result<Response<Body>, hyper::http::Error>;
    fn file_event(&mut self, event: FileEvent) -> Result<(), anyhow::Error>;
}
