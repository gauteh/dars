use std::convert::Infallible;
use std::sync::Arc;
use warp::Rejection;

use super::{Dataset, DatasetType, State};

pub async fn list_datasets(state: State) -> Result<impl warp::Reply, Infallible> {
    Ok(format!(
        "Index of datasets:\n\n{}",
        state
            .datasets
            .keys()
            .map(|s|
                format!("   {} [<a href=\"/data/{}\">dap</a>][<a href=\"/data/{}\">raw</a>] ([<a href=\"/data/{}.das\">das</a>][<a href=\"/data/{}.dds\">dds</a>][<a href=\"/data/{}.dods\">dods</a>])",
                s, s, s, s, s, s)
            )
            .collect::<Vec<String>>()
            .join("\n")
    ))
}

pub async fn list_datasets_json(state: State) -> Result<impl warp::Reply, Infallible> {
    Ok(warp::reply::json(
        &state.datasets.keys().map(|s| &**s).collect::<Vec<&str>>(),
    ))
}

pub async fn das(dataset: Arc<DatasetType>) -> Result<impl warp::Reply, Rejection> {
    match &*dataset {
        DatasetType::HDF5(dataset) => Ok(dataset.das().await.0.clone()),
    }
}

pub async fn dds(dataset: Arc<DatasetType>) -> Result<impl warp::Reply, Infallible> {
    Ok("hello dds")
}

pub async fn dods(dataset: Arc<DatasetType>) -> Result<impl warp::Reply, Infallible> {
    Ok("hello dods")
}

pub async fn raw(dataset: Arc<DatasetType>) -> Result<impl warp::Reply, Infallible> {
    Ok("hello raw")
}
