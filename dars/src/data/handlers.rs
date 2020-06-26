use futures::stream::FuturesOrdered;
use futures::stream::{self, StreamExt, TryStreamExt};
use hyper::Body;
use std::convert::Infallible;
use std::iter;
use std::sync::Arc;
use warp::reply::Reply;

use dap2::dds::{ConstrainedVariable, DdsVariableDetails};
use dap2::Constraint;

use byte_slice_cast::IntoByteVec;
use dap2::dods::XdrPack;

use super::{DatasetType, State};

pub async fn list_datasets(state: State) -> Result<impl warp::Reply, Infallible> {
    Ok(format!(
        "Index of datasets:\n\n{}",
        state
            .datasets
            .keys()
            .map(|s|
                format!("   {} [<a href=\"/data/{}\">dap</a>][<a href=\"/data/{}\">raw</a>] ([<a href=\"/data/{}.das\">das</a>][<a href=\"/data/{}.dds\">dds</a>][<a href=\"/data/{}.dods\">dods</a>]<br />)",
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

pub async fn das(dataset: Arc<DatasetType>) -> Result<impl warp::Reply, Infallible> {
    match &*dataset {
        DatasetType::HDF5(dataset) => Ok(dataset.das().await.0.clone()),
    }
}

pub async fn dds(
    dataset: Arc<DatasetType>,
    constraint: Constraint,
) -> Result<impl warp::Reply, Infallible> {
    match &*dataset {
        DatasetType::HDF5(dataset) => dataset
            .dds()
            .await
            .dds(&constraint)
            .map(|dds| dds.to_string().into_response())
            .or_else(|_| Ok(warp::http::StatusCode::BAD_REQUEST.into_response())),
    }
}

#[derive(Debug)]
struct DodsError;
impl warp::reject::Reject for DodsError {}

pub async fn dods(
    dataset: Arc<DatasetType>,
    constraint: Constraint,
) -> Result<impl warp::Reply, warp::Rejection> {
    match &*dataset {
        DatasetType::HDF5(dataset) => {
            let dds = dataset
                .dds()
                .await
                .dds(&constraint)
                .or_else(|_| Err(warp::reject::custom(DodsError)))?;

            let dds_bytes = dds.to_string().as_bytes().to_vec();

            let readers = dds
                .variables
                .into_iter()
                .map(move |c| match c {
                    ConstrainedVariable::Variable(v) => Box::new(iter::once(v))
                        as Box<dyn Iterator<Item = DdsVariableDetails> + Send + Sync + 'static>,
                    ConstrainedVariable::Structure {
                        variable: _,
                        member,
                    } => Box::new(iter::once(member)),
                    ConstrainedVariable::Grid {
                        variable,
                        dimensions,
                    } => Box::new(iter::once(variable).chain(dimensions.into_iter())),
                })
                .flatten()
                .map(|c| async move { dataset.variable(&c).await })
                .collect::<FuturesOrdered<_>>()
                .try_collect::<Vec<_>>()
                .await
                .or_else(|e| {
                    debug!("error building variable stream: {:?}", e);
                    Err(warp::reject::custom(DodsError))
                })?
                .into_iter()
                .map(|(len, stream)| {
                    let length = if let Some(len) = len {
                        let mut length = vec![len as u32, len as u32];
                        length.pack();
                        let length = length.into_byte_vec();
                        length
                    } else {
                        Vec::new()
                    };

                    stream::once(async move { Ok(length) }).chain(stream)
                });

            let stream = stream::once(async move { Ok(dds_bytes) })
                .chain(stream::once(async move {
                    Ok("\n\nData:\n".as_bytes().to_vec())
                }))
                .chain(stream::iter(readers).flatten());

            // TODO: Send length of stream in Content-Length

            Ok(warp::http::Response::builder()
                .header("Content-Type", "application/octet-stream")
                .header("Content-Description", "dods-data")
                .header("XDODS-Server", "dars")
                .body(Body::wrap_stream(stream)))
        }
    }
}

pub async fn raw(dataset: Arc<DatasetType>) -> Result<impl warp::Reply, Infallible> {
    match &*dataset {
        DatasetType::HDF5(dataset) => dataset
            .raw()
            .await
            .map(|s| {
                warp::http::Response::builder()
                    .header("Content-Type", "application/octet-stream")
                    .header("Content-Disposition", "attachment")
                    .header("XDODS-Server", "dars")
                    .body(Body::wrap_stream(s))
            })
            .or_else(|_| Ok(Ok(warp::http::StatusCode::NOT_FOUND.into_response()))),
    }
}
