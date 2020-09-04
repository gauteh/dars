use std::convert::Infallible;
///! This module holds the collection of datasets which are available. It utilizes the `dap2`
///! module to parse queries and dispatch metadata or data requests to the `Dataset` implementation
///! on each dataset-source.
use std::sync::Arc;
use warp::Filter;

use dap2::Constraint;

use super::handlers;
use super::DatasetType;
use super::State;

pub fn datasets(
    state: State,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    // TODO: Include recover filter to catch errors without falling through to raw
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
        .and(with_state(state.clone()))
        .and(warp::header::exact_ignore_case(
            "accept",
            "application/json",
        ))
        .and_then(handlers::list_datasets_json)
        .or(warp::path!("data")
            .and(warp::get())
            .and(with_state(state))
            .and_then(handlers::list_datasets))
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
                .and(with_state(state.clone()))
                .and_then(with_dataset),
        )
        .and(constraint())
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

fn ends_with(
    ext: &'static str,
) -> impl Filter<Extract = (String,), Error = warp::Rejection> + Clone {
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

    if let Some(dataset) = state.get(&dataset) {
        Ok(Arc::clone(dataset))
    } else {
        debug!("Could not find dataset: {}", dataset);
        Err(warp::reject::not_found())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::test_state;
    use futures::executor::block_on;
    use test::Bencher;

    #[tokio::test]
    async fn dap_methods() {
        let state = test_state();

        assert!(
            warp::test::request()
                .path("/data/coads_climatology.nc4.das")
                .matches(&das(state.clone()))
                .await
        );

        assert!(
            warp::test::request()
                .path("/data/coads_climatology.nc4.dds")
                .matches(&dds(state.clone()))
                .await
        );

        assert!(
            warp::test::request()
                .path("/data/coads_climatology.nc4.dods")
                .matches(&dods(state.clone()))
                .await
        );

        assert!(
            warp::test::request()
                .path("/data/nested/coads_climatology.nc4.dods")
                .matches(&dods(state.clone()))
                .await
        );

        assert!(
            warp::test::request()
                .path("/data/coads_climatology.nc4")
                .matches(&raw(state.clone()))
                .await
        );

        assert_eq!(
            warp::test::request()
                .path("/test.das")
                .filter(&ends_with(".das"))
                .await
                .unwrap(),
            "test"
        );

        assert_eq!(
            warp::test::request()
                .path("/test.das?fasd")
                .filter(&ends_with(".das"))
                .await
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
            test::black_box(res.body());
        });
    }

    #[bench]
    fn dds_constraint(b: &mut Bencher) {
        let filter = constraint();

        b.iter(|| {
            block_on(
                warp::test::request()
                    .path("/data/coads_climatology.nc4.dds?SST[0:5][0:70][0:70],TIME,COADSX,COADSY")
                    .filter(&filter),
            )
            .unwrap()
        })
    }

    #[test]
    fn dds_strides_unsupported() {
        let state = test_state();
        let dds = dds(state.clone());

        let res = block_on(
            warp::test::request()
                .path("/data/coads_climatology.nc4.dds?SST[0:2:5][0:70][0:70],TIME,COADSX,COADSY")
                .reply(&dds),
        );

        assert_eq!(res.status(), 400);
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
            test::black_box(res.body());
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
            test::black_box(res.body());
        })
    }

    #[test]
    fn coads_dods_constrained() {
        let state = test_state();
        let dods = dods(state.clone());

        let res = block_on(
            warp::test::request()
                .path("/data/coads_climatology.nc4.dods?SST.SST")
                .reply(&dods),
        );

        assert_eq!(res.status(), 200);
    }
}
