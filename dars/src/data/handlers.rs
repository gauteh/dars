use async_stream::stream;
use bytes::Bytes;
use futures::pin_mut;
use futures::stream::{StreamExt, TryStreamExt};
use hyper::Body;
use std::convert::Infallible;
use std::sync::Arc;
use warp::reply::Reply;

use dap2::dds::ConstrainedVariable;
use dap2::Constraint;

use super::{DatasetType, State};

pub async fn list_datasets(state: State) -> Result<impl warp::Reply, Infallible> {
    Ok(warp::http::Response::builder()
        .header("Content-Type", "text/html")
        .body(Body::from(
        format!(
        "Index of datasets:<br/><br/>{}",
        state
            .datasets
            .keys()
            .map(|s|
                format!("   {} [<a href=\"/data/{}\">dap</a>][<a href=\"/data/{}\">raw</a>] ([<a href=\"/data/{}.das\">das</a>][<a href=\"/data/{}.dds\">dds</a>][<a href=\"/data/{}.dods\">dods</a>])<br />",
                s, s, s, s, s, s)
            )
            .collect::<Vec<String>>()
            .join("\n")
    ))))
}

pub async fn list_datasets_json(state: State) -> Result<impl warp::Reply, Infallible> {
    Ok(warp::reply::json(
        &state.datasets.keys().map(|s| &**s).collect::<Vec<&str>>(),
    ))
}

pub async fn das(dataset: Arc<DatasetType>) -> Result<impl warp::Reply, Infallible> {
    match &*dataset {
        DatasetType::HDF5(dataset) => Ok(dataset.das().await.0.clone()),
        DatasetType::NCML(dataset) => Ok(dataset.das().await.0.clone()),
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
            .or_else(|e| {
                error!("Error parsing DDS: {:?}", e);
                Ok(warp::http::StatusCode::BAD_REQUEST.into_response())
            }),
        DatasetType::NCML(dataset) => dataset
            .dds()
            .await
            .dds(&constraint)
            .map(|dds| dds.to_string().into_response())
            .or_else(|e| {
                error!("Error parsing DDS: {:?}", e);
                Ok(warp::http::StatusCode::BAD_REQUEST.into_response())
            }),
    }
}

#[derive(Debug)]
struct DodsError;
impl warp::reject::Reject for DodsError {}

pub async fn dods(
    dataset: Arc<DatasetType>,
    db: sled::Db,
    constraint: Constraint,
) -> Result<impl warp::Reply, warp::Rejection> {
    match &*dataset {
        DatasetType::HDF5(inner) => {
            let dds = inner.dds().await.dds(&constraint).or_else(|e| {
                error!("Error parsing DDS: {:?}", e);
                Err(warp::reject::custom(DodsError))
            })?;

            let dds_bytes = Bytes::from(dds.to_string());
            let content_length = dds.dods_size() + dds_bytes.len() + 8;

            let body = stream! {
                let dataset = Arc::clone(&dataset);
                let dataset = if let DatasetType::HDF5(dataset) = &*dataset { dataset } else {
                    panic!("we are already matched on HDF5") };

                yield Ok::<_, anyhow::Error>(dds_bytes);
                yield Ok(Bytes::from_static(b"\n\nData:\n"));

                for c in dds.variables {
                    match c {
                        ConstrainedVariable::Variable(v) |
                            ConstrainedVariable::Structure { variable: _, member: v }
                            => {
                            let reader = dataset.variable(&v, db.clone()).await?;

                            pin_mut!(reader);

                            while let Some(b) = reader.next().await {
                                yield b;
                            }
                        },
                        ConstrainedVariable::Grid {
                            variable,
                            dimensions,
                        } => {
                            for variable in std::iter::once(variable).chain(dimensions) {
                                let reader = dataset.variable(&variable, db.clone()).await?;

                                pin_mut!(reader);

                                while let Some(b) = reader.next().await {
                                    yield b;
                                }
                            }
                        }
                    }
                }
            }
            .map_err(|e| {
                error!("Error while streaming: {:?}", e);
                std::io::Error::from(std::io::ErrorKind::UnexpectedEof)
            });

            Ok(warp::http::Response::builder()
                .header("Content-Type", "application/octet-stream")
                .header("Content-Description", "dods-data")
                .header("Content-Length", content_length)
                .header("XDODS-Server", "dars")
                .body(Body::wrap_stream(body)))
        }
        DatasetType::NCML(inner) => {
            let dds = inner.dds().await.dds(&constraint).or_else(|e| {
                error!("Error parsing DDS: {:?}", e);
                Err(warp::reject::custom(DodsError))
            })?;

            let dds_bytes = Bytes::from(dds.to_string());
            let content_length = dds.dods_size() + dds_bytes.len() + 8;

            let body = stream! {
                let dataset = Arc::clone(&dataset);
                let dataset = if let DatasetType::NCML(dataset) = &*dataset { dataset } else {
                    panic!("we are already matched on NCML") };

                yield Ok::<_, anyhow::Error>(dds_bytes);
                yield Ok(Bytes::from_static(b"\n\nData:\n"));

                for c in dds.variables {
                    match c {
                        ConstrainedVariable::Variable(v) |
                            ConstrainedVariable::Structure { variable: _, member: v }
                            => {
                            let reader = dataset.variable(&v, db.clone()).await?;

                            pin_mut!(reader);

                            while let Some(b) = reader.next().await {
                                yield b;
                            }
                        },
                        ConstrainedVariable::Grid {
                            variable,
                            dimensions,
                        } => {
                            for variable in std::iter::once(variable).chain(dimensions) {
                                let reader = dataset.variable(&variable, db.clone()).await?;

                                pin_mut!(reader);

                                while let Some(b) = reader.next().await {
                                    yield b;
                                }
                            }
                        }
                    }
                }
            }
            .map_err(|e| {
                error!("Error while streaming: {:?}", e);
                std::io::Error::from(std::io::ErrorKind::UnexpectedEof)
            });

            Ok(warp::http::Response::builder()
                .header("Content-Type", "application/octet-stream")
                .header("Content-Description", "dods-data")
                .header("Content-Length", content_length)
                .header("XDODS-Server", "dars")
                .body(Body::wrap_stream(body)))
        }
    }
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

        let hd = Arc::new(DatasetType::HDF5(
            Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap(),
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

        let hd = Arc::new(DatasetType::HDF5(
            Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap(),
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
