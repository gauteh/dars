use hyper::{Response, Body, StatusCode};
use async_trait::async_trait;
use std::sync::Arc;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;
use colored::Colorize;

use super::{
    nc::NcDataset,
    ncml::NcmlDataset
};

pub struct Data {
    pub root: PathBuf,
    pub datasets: HashMap<String, Arc<dyn Dataset + Send + Sync>>
}

enum DsRequestType {
    Das,
    Dds,
    Dods,
    Nc,
    Unknown
}

struct DsRequest(String, DsRequestType);

impl Data {
    pub fn new() -> Data {
        Data {
            root: "./".into(),
            datasets: HashMap::new()
        }
    }

    pub fn make_key(&self, p: &Path) -> String {
        if self.root.to_string_lossy().ends_with("/") {
            // remove root
            p.to_str().unwrap().trim_start_matches(self.root.to_str().unwrap()).to_string()
        } else {
            p.to_str().unwrap().to_string()
        }
    }

    pub fn init_root<P>(&mut self, root: P) -> ()
        where P: Into<PathBuf>
    {
        self.root = root.into();
        self.datasets.clear();

        info!("Scanning {} for datasets..", self.root.to_string_lossy().yellow());

        for entry in WalkDir::new(&self.root)
            .follow_links(true)
            .into_iter()
            .filter_entry(|entry| !entry.file_name().to_str().map(|s| s.starts_with(".")).unwrap_or(false))
        {
            if let Ok(entry) = entry {
                match entry.metadata() {
                    Ok(m) if m.is_file() => {
                        match entry.path().extension() {
                            Some(ext) if ext == "nc" => {
                                match NcDataset::open(entry.path()) {
                                    Ok(ds) => { self.datasets.insert(self.make_key(entry.path().into()),
                                    Arc::new(ds)); },
                                    Err(e) => warn!("Could not open: {:?} ({:?})", entry.path(), e)
                                }
                            },
                            Some(ext) if ext == "ncml" => {
                                match NcmlDataset::open(entry.path()) {
                                    Ok(ds) => { self.datasets.insert(self.make_key(entry.path().into()),
                                    Arc::new(ds)); },
                                    Err(e) => warn!("Could not open: {:?} ({:?})", entry.path(), e)
                                }
                            },
                            _ => ()
                        }
                    },
                    _ => ()
                }
            }
        }
    }

    pub fn data_event(e: notify::DebouncedEvent) -> () {
        use notify::DebouncedEvent::*;

        match e {
            Create(pb) | Write(pb) | Remove(pb) => Data::reload_file(pb),
            Rename(pba, pbb) => { Data::reload_file(pba); Data::reload_file(pbb) },
            _ => debug!("Unhandled event: {:?}", e)
        }
    }

    pub fn reload_file(pb: PathBuf) {
        debug!("Checking file: {:?}", pb);

        if let Some(ext) = pb.extension() {
            if ext == "nc" || ext == "ncml" {
                use super::DATA;

                let rdata = DATA.clone();
                let mut data = rdata.write().unwrap();

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
                            Ok(ds) => { data.datasets.insert(key, Arc::new(ds)); },
                            Err(e) => warn!("Could not open: {:?} ({:?})", pb, e)
                        }
                    } else if ext == "ncml" {
                        match NcmlDataset::open(pb.clone()) {
                            Ok(ds) => { data.datasets.insert(key, Arc::new(ds)); },
                            Err(e) => warn!("Could not open: {:?} ({:?})", pb, e)
                        }
                    }
                }
            }
        }
    }

    pub fn datasets(_req: hyper::Request<Body>) -> Result<Response<Body>, hyper::http::Error> {
        let mut datasets: Vec<String> = {
            let rdata = super::DATA.clone();
            let data = rdata.read().unwrap();

            data.datasets.keys().map(|s| format!("  /data/{}", s)).collect()
        };

        datasets.sort();

        Response::builder().body(Body::from(
                format!("Index of datasets:\n\n{}\n", datasets.join("\n"))))
    }

    fn parse_request(ds: String) -> DsRequest {
        if ds.ends_with(".das") {
            DsRequest(String::from(ds.trim_end_matches(".das")), DsRequestType::Das)
        } else if ds.ends_with(".dds") {
            DsRequest(String::from(ds.trim_end_matches(".dds")), DsRequestType::Dds)
        } else if ds.ends_with(".dods") {
            DsRequest(String::from(ds.trim_end_matches(".dods")), DsRequestType::Dods)
        } else if ds.ends_with(".nc") {
            DsRequest(String::from(&ds), DsRequestType::Nc)
        } else {
            DsRequest(String::from(&ds), DsRequestType::Unknown)
        }
    }

    pub async fn dataset(req: hyper::Request<Body>) -> Result<Response<Body>, hyper::http::Error> {
        use super::DATA;

        let ds: String = req.uri().path().trim_start_matches("/data/").to_string();
        let DsRequest(ds, dst) = Data::parse_request(ds);

        let ds = {
            let rdata = DATA.clone();
            let data = rdata.read().unwrap();

            match data.datasets.get(&ds) {
                Some(ds) => Some(ds.clone()),
                None => None
            }
        };

        match ds {
            Some(ds) => {
                match dst {
                    DsRequestType::Das => ds.das().await,
                    DsRequestType::Dds => ds.dds(req.uri().query().map(|s| s.to_string())).await,
                    DsRequestType::Dods => ds.dods(req.uri().query().map(|s| s.to_string())).await,
                    DsRequestType::Nc => ds.nc().await,
                    _ => Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty())
                }
            },
            None => {
                Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty())
            }
        }
    }
}

#[async_trait]
pub trait Dataset {
    fn name(&self) -> String;

    async fn das(&self) -> Result<Response<Body>, hyper::http::Error>;
    async fn dds(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error>;
    async fn dods(&self, query: Option<String>) -> Result<Response<Body>, hyper::http::Error>;
    async fn nc(&self) -> Result<Response<Body>, hyper::http::Error>;
}
