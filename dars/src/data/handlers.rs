use futures::stream::TryStreamExt;
use hyper::Body;
use std::convert::Infallible;
use std::sync::Arc;
use warp::reply::Reply;

use dap2::{Constraint, Dap2, Dods};
use super::DatasetType;

#[cfg(not(feature = "catalog"))]
use super::State;

#[cfg(not(feature = "catalog"))]
pub async fn list_datasets_json(state: State) -> Result<impl warp::Reply, Infallible> {
    Ok(warp::reply::json(
        &state.datasets.keys().map(|s| &**s).collect::<Vec<&str>>(),
    ))
}

pub async fn das(dataset: Arc<DatasetType>) -> Result<impl warp::Reply, Infallible> {
    Ok(dataset.das().await.0.clone())
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
    let (content_length, body) = dataset.dods(constraint).await.or_else(|e| {
        error!("Error constructing DODS response: {:?}", e);
        Err(warp::reject::custom(DodsError))
    })?;

    Ok(warp::http::Response::builder()
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
                warp::http::Response::builder()
                    .header("Content-Type", "application/octet-stream")
                    .header("Content-Disposition", "attachment")
                    .header("XDODS-Server", "dars")
                    .header("Content-Length", sz)
                    .body(Body::wrap_stream(s))
            })
            .or_else(|_| Ok(Ok(warp::http::StatusCode::NOT_FOUND.into_response()))),
        _ => Ok(Ok(warp::http::StatusCode::NOT_FOUND.into_response())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::{block_on, block_on_stream};
    use test::Bencher;

    #[bench]
    fn coads_build_sst_struct(b: &mut Bencher) {
        use crate::hdf5::Hdf5Dataset;

        let db = crate::data::test_db();
        let hd = Arc::new(DatasetType::HDF5(
            Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap(),
        ));

        let c = Constraint::parse("SST.SST").unwrap();

        b.iter(|| {
            let hd = hd.clone();
            let c = c.clone();
            block_on(dods(hd, c))
        })
    }

    #[bench]
    fn coads_stream_sst_struct(b: &mut Bencher) {
        use crate::hdf5::Hdf5Dataset;
        use warp::reply::Reply;

        let db = crate::data::test_db();
        let hd = Arc::new(DatasetType::HDF5(
            Hdf5Dataset::open("../data/coads_climatology.nc4", "coads".into(), &db).unwrap(),
        ));

        let c = Constraint::parse("SST.SST").unwrap();

        b.iter(|| {
            let hd = hd.clone();
            let c = c.clone();
            let response = block_on(dods(hd, c)).unwrap().into_response();
            block_on_stream(response.into_body()).for_each(drop);
        })
    }
}
