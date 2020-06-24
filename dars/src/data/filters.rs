use std::convert::Infallible;
///! This module holds the collection of datasets which are available. It utilizes the `dap2`
///! module to parse queries and dispatch metadata or data requests to the `Dataset` implementation
///! on each dataset-source.
use std::sync::Arc;
use warp::Filter;

use dap2::dds::{ConstrainedVariable, DdsVariableDetails};
use dap2::Constraint;

use super::handlers;
use super::State;
use super::{Dataset, DatasetType};

pub fn datasets(
    state: State,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    dataset_list(state.clone())
        .or(das(state.clone()))
        .or(dds(state.clone()))
        .or(dods(state.clone()))
        .or(raw(state.clone()))
}

pub fn dataset_list(
    state: State,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("data")
        .and(warp::get())
        .and(with_state(state))
        .and_then(handlers::list_datasets)
}

pub fn das(
    state: State,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path::path("data")
        .and(warp::get())
        .and(
            ends_with(".das")
                .and(with_state(state))
                .and_then(with_dataset),
        )
        .and_then(handlers::das)
}

pub fn dds(
    state: State,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path::path("data")
        .and(warp::get())
        .and(
            ends_with(".dds")
                .and(with_state(state))
                .and_then(with_dataset),
        )
        .and(constraint())
        .and_then(handlers::dds)
}

pub fn dods(
    state: State,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path::path("data")
        .and(warp::get())
        .and(
            ends_with(".dods")
                .and(with_state(state))
                .and_then(with_dataset),
        )
        .and_then(handlers::dods)
}

pub fn raw(
    state: State,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path::path("data")
        .and(warp::get())
        .and(
            warp::path::tail()
                .map(|t: warp::path::Tail| String::from(t.as_str()))
                .and(with_state(state))
                .and_then(with_dataset),
        )
        .and_then(handlers::raw)
}

fn with_state(state: State) -> impl Filter<Extract = (State,), Error = Infallible> + Clone {
    warp::any().map(move || Arc::clone(&state))
}

fn ends_with<'a>(
    ext: &'a str,
) -> impl Filter<Extract = (String,), Error = warp::reject::Rejection> + Clone + 'a {
    warp::path::tail().and_then(move |tail: warp::filters::path::Tail| async move {
        if tail.as_str().ends_with(ext) {
            Ok(String::from(
                &tail.as_str()[..tail.as_str().len() - ext.len()],
            ))
        } else {
            Err(warp::reject::reject())
        }
    })
}

fn constraint() -> impl Filter<Extract = (Constraint,), Error = warp::reject::Rejection> + Clone {
    warp::any()
        .and(warp::query::raw().and_then(move |s: String| async move {
            Constraint::parse(s.as_str()).or_else(|_| Err(warp::reject::reject()))
        }))
        .or(warp::any()
            .and_then(|| async move { Ok::<_, warp::reject::Rejection>(Constraint::empty()) }))
        .unify()
}

async fn with_dataset(
    dataset: String,
    state: State,
) -> Result<Arc<DatasetType>, warp::reject::Rejection> {
    let state = Arc::clone(&state);

    if let Some(dataset) = state.datasets.get(&dataset) {
        Ok(Arc::clone(dataset))
    } else {
        debug!("Could not find dataset: {}", dataset);
        Err(warp::reject::not_found())
    }
}
// impl Datasets {
//             Dods => {
//                 let dds = dset.dds().await.dds(&constraint).or_else(|e| {
//                     Err(Error::from_str(
//                         StatusCode::BadRequest,
//                         format!("Invalid DDS request: {}", e.to_string()),
//                     ))
//                 })?;

//                 let mut dds_str = dds.to_string();
//                 dds_str.push_str("\n\nData:\n");
//                 let dds_bytes = dds_str.as_bytes().to_vec();
//                 let len = dds_bytes.len() + dds.dods_size();

//                 let readers = dds
//                     .variables
//                     .into_iter()
//                     .map(|c| match c {
//                         ConstrainedVariable::Variable(v) => Box::new(iter::once(v))
//                             as Box<dyn Iterator<Item = DdsVariableDetails> + Send + Sync + 'static>,

//                         ConstrainedVariable::Structure { variable: _, member } => {
//                             Box::new(iter::once(member))
//                         }

//                         ConstrainedVariable::Grid {
//                             variable,
//                             dimensions,
//                         } => Box::new(iter::once(variable).chain(dimensions.into_iter())),
//                     })
//                     .flatten()
//                     .map(|c| async move { dset.variable(&c).await.map(|d| d.as_reader()) })
//                     .collect::<stream::FuturesOrdered<_>>()
//                     .try_collect::<Vec<_>>()
//                     .await
//                     .map_err(|e| {
//                         Error::from_str(
//                             StatusCode::BadRequest,
//                             format!("Could not read variables: {}", e.to_string()),
//                         )
//                     })?;

//                 let reader = BufReader::new(AsyncReadFlatten::from(Box::pin(stream::iter(
//                     readers.into_iter(),
//                 ))));

//                 Ok(tide::Body::from_reader(
//                     Box::pin(
//                         stream::once(async move { Ok(dds_bytes) })
//                     )
//                     .into_async_read()
//                     .chain(reader),
//                     Some(len),
//                 )
//                 .into())
//             }

//             // TODO: why is this slower than from_file?
//             Raw => dset
//                 .raw()
//                 .await
//                 .map(|(reader, len)| Ok(tide::Body::from_reader(reader, len).into()))
//                 .or_else(|e| {
//                     Err(Error::from_str(
//                         StatusCode::BadRequest,
//                         format!("Invalid DDS request: {}", e.to_string()),
//                     ))
//                 })?,
//         }
//     }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data;
    use crate::hdf5;
    use futures::executor::block_on;
    use std::sync::Arc;
    use test::Bencher;

    fn test_state() -> data::State {
        let mut data = data::Datasets::default();
        data.datasets.insert(
            "coads_climatology.nc4".to_string(),
            Arc::new(data::DatasetType::HDF5(
                hdf5::Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap(),
            )),
        );
        data.datasets.insert(
            "nested/coads_climatology.nc4".to_string(),
            Arc::new(data::DatasetType::HDF5(
                hdf5::Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap(),
            )),
        );
        Arc::new(data)
    }

    // #[bench]
    // fn coads_get_sst(b: &mut Bencher) {
    //     use crate::Server;

    //     let mut data = Datasets::default();
    //     data.datasets.insert(
    //         "coads_climatology.nc4".to_string(),
    //         DatasetType::HDF5(hdf5::Hdf5Dataset::open("../data/coads_climatology.nc4").unwrap()),
    //         );

    //     use tide::http::{Url, Method, Request};
    //     let req = Request::new(Method::Get, Url::parse("http://localhost/data/coads_climatology.nc4.dods?SST").unwrap());

    //     let server = Server { data };
    //     let mut dars = tide::with_state(server);
    //     dars.at("/data/:dataset")
    //         .get(|req: tide::Request<Server>| async move { req.state().data.dataset(&req).await });

    //     b.iter(|| {
    //         let req = req.clone();

    //         block_on(async {
    //             let mut res: tide::Response = dars.respond(req).await.unwrap();
    //             assert_eq!(res.status(), 200);
    //             res.take_body().into_bytes().await.unwrap()
    //         })
    //     })
    // }

    #[test]
    fn dap_methods() {
        let state = test_state();

        assert!(block_on(
            warp::test::request()
                .path("/data/coads_climatology.nc4.das")
                .matches(&das(state.clone()))
        ));

        assert!(block_on(
            warp::test::request()
                .path("/data/coads_climatology.nc4.dds")
                .matches(&dds(state.clone()))
        ));

        assert!(block_on(
            warp::test::request()
                .path("/data/coads_climatology.nc4.dods")
                .matches(&dods(state.clone()))
        ));

        assert!(block_on(
            warp::test::request()
                .path("/data/nested/coads_climatology.nc4.dods")
                .matches(&dods(state.clone()))
        ));

        assert!(block_on(
            warp::test::request()
                .path("/data/coads_climatology.nc4")
                .matches(&raw(state.clone()))
        ));

        assert_eq!(
            block_on(
                warp::test::request()
                    .path("/test.das")
                    .filter(&ends_with(".das"))
            )
            .unwrap(),
            "test"
        );

        assert_eq!(
            block_on(
                warp::test::request()
                    .path("/test.das?fasd")
                    .filter(&ends_with(".das"))
            )
            .unwrap(),
            "test"
        );
    }

    #[bench]
    fn coads_das(b: &mut Bencher) {
        let state = test_state();
        let das = das(state.clone());

        b.iter(|| {
            let res = block_on(
                warp::test::request()
                    .path("/data/coads_climatology.nc4.das")
                    .reply(&das),
            );

            assert_eq!(res.status(), 200);
            test::black_box(|| res.body());
        });
    }

    #[bench]
    fn dds_constraint(b: &mut Bencher) {
        let filter = constraint();

        b.iter(|| {
            let res = block_on(
                warp::test::request()
                    .path("/data/coads_climatology.nc4.dds?SST[0:5][0:70][0:70],TIME,COADSX,COADSY")
                    .filter(&filter),
            )
            .unwrap();
        })
    }

    #[bench]
    fn coads_dds_constrained(b: &mut Bencher) {
        let state = test_state();
        let dds = dds(state.clone());

        b.iter(|| {
            let res = block_on(
                warp::test::request()
                    .path("/data/coads_climatology.nc4.dds?SST[0:5][0:70][0:70],TIME,COADSX,COADSY")
                    .reply(&dds),
            );

            assert_eq!(res.status(), 200);
            test::black_box(|| res.body());
        })
    }

    #[bench]
    fn coads_dds_unconstrained(b: &mut Bencher) {
        let state = test_state();
        let dds = dds(state.clone());

        b.iter(|| {
            let res = block_on(
                warp::test::request()
                    .path("/data/coads_climatology.nc4.dds")
                    .reply(&dds),
            );

            assert_eq!(res.status(), 200);
            test::black_box(|| res.body());
        })
    }
}
