use futures::stream::TryStreamExt;
use std::convert::Infallible;
use std::sync::Arc;
use warp::{http::Response, http::StatusCode, hyper::Body, reply::Reply};

use super::DatasetType;
use dap2::{Constraint, Dap2, Dods};

#[cfg(not(feature = "catalog"))]
use super::State;

#[cfg(not(feature = "catalog"))]
pub async fn list_datasets_json(state: State) -> Result<impl warp::Reply, Infallible> {
    Ok(warp::reply::json(
        &state.datasets.keys().map(|s| &**s).collect::<Vec<&str>>(),
    ))
}

pub async fn das(dataset: Arc<DatasetType>) -> Result<impl warp::Reply, Infallible> {
    Ok(Response::builder().body(Body::from(dataset.das().await.bytes())))
}

pub async fn dds(
    dataset: Arc<DatasetType>,
    constraint: Constraint,
) -> Result<impl warp::Reply, Infallible> {
    dataset
        .dds()
        .await
        .dds(&constraint)
        .map(|dds| dds.to_string().into_response())
        .or_else(|e| {
            error!("Error parsing DDS: {:?}", e);
            Ok(warp::http::StatusCode::BAD_REQUEST.into_response())
        })
}

#[derive(Debug)]
struct DodsError;
impl warp::reject::Reject for DodsError {}

pub async fn dods(
    dataset: Arc<DatasetType>,
    constraint: Constraint,
) -> Result<impl warp::Reply, warp::Rejection> {
    let dataset = Arc::clone(&dataset);
    let (content_length, body) = dataset.dods(constraint).await.map_err(|e| {
        error!("Error constructing DODS response: {:?}", e);
        warp::reject::custom(DodsError)
    })?;

    Ok(Response::builder()
        .header("Content-Type", "application/octet-stream")
        .header("Content-Description", "dods-data")
        .header("Content-Length", content_length)
        .header("XDODS-Server", "dars")
        .body(Body::wrap_stream(body.map_err(|e| {
            error!("Error while streaming: {:?}", e);
            std::io::Error::from(std::io::ErrorKind::UnexpectedEof)
        }))))
}

pub async fn raw(dataset: Arc<DatasetType>) -> Result<impl warp::Reply, Infallible> {
    match &*dataset {
        DatasetType::HDF5(dataset) => dataset
            .raw()
            .await
            .map(|(sz, s)| {
                Response::builder()
                    .header("Content-Type", "application/octet-stream")
                    .header("Content-Disposition", "attachment")
                    .header("XDODS-Server", "dars")
                    .header("Content-Length", sz)
                    .body(Body::wrap_stream(s))
            })
            .or_else(|_| Ok(Ok(StatusCode::NOT_FOUND.into_response()))),
        _ => Ok(Ok(StatusCode::NOT_FOUND.into_response())),
    }
}
