use std::convert::Infallible;
use std::sync::Arc;
use tera::Tera;
use warp::Filter;

use crate::handlers;
use crate::Catalog;

pub fn catalog<T: Catalog + Clone>(
    root: String,
    tera: Arc<Tera>,
    catalog: T,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    index_json(catalog.clone())
        .or(index(root.clone(), tera.clone(), catalog.clone()))
        .or(folder(root, tera, catalog))
}

fn index<T: Catalog + Clone>(
    root: String,
    tera: Arc<Tera>,
    catalog: T,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path::end()
        .and(warp::get())
        .and(with_root(root))
        .map(move |r| (r, Arc::clone(&tera), catalog.clone()))
        .untuple_one()
        .and_then(handlers::index)
}

fn index_json<T: Catalog + Clone>(
    catalog: T,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("data")
        .and(warp::get())
        .and(warp::header::exact_ignore_case(
            "accept",
            "application/json",
        ))
        .map(move || catalog.clone())
        .and_then(handlers::index_json)
}

fn folder<T: Catalog + Clone>(
    root: String,
    tera: Arc<Tera>,
    catalog: T,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("data")
        .and(warp::get())
        .and(with_root(root))
        .map(move |r| (r, Arc::clone(&tera)))
        .untuple_one()
        .and(elements(catalog))
        .and_then(handlers::folder)
}

/// Return the path, the folders in the path and the elements in the path, if the path is a folder.
fn elements<T: Catalog + Clone>(
    catalog: T,
) -> impl Filter<Extract = (String, (Vec<String>, Vec<String>)), Error = warp::reject::Rejection> + Clone
{
    warp::path::peek()
        .map(|p: warp::filters::path::Peek| p.as_str().to_string())
        .and(
            warp::path::tail().and_then(move |tail: warp::filters::path::Tail| {
                let catalog = catalog.clone();
                async move {
                    let mut tail = tail.as_str().to_string();
                    if tail.len() > 0 && !tail.ends_with('/') {
                        tail.push('/');
                    }

                    let (folders, mut paths) = catalog
                        .paths()
                        .filter_map(|p| {
                            if p.starts_with(&tail) {
                                Some(String::from(&p[tail.len()..]))
                            } else {
                                None
                            }
                        })
                        .partition::<Vec<String>, _>(|p| p.contains('/'));

                    paths.sort();

                    // Remove trailing names + make unique
                    let mut folders = folders
                        .iter()
                        .map(|p| p[..(1 + p[1..].find('/').unwrap())].to_string())
                        .collect::<Vec<String>>();
                    folders.sort();
                    folders.dedup();

                    // No such path or matches exact data source
                    if (folders.len() == 0 && paths.len() == 0)
                        || (folders.len() == 0 && paths.len() == 1 && paths[0] == tail)
                    {
                        Err(warp::reject())
                    } else {
                        Ok((folders, paths))
                    }
                }
            }),
        )
}

fn with_root(root: String) -> impl Filter<Extract = (String,), Error = Infallible> + Clone {
    warp::any().map(move || root.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::*;
    use futures::executor::block_on;

    #[test]
    fn elements_subpath() {
        let catalog = TestCatalog::test();
        let path1 = block_on(
            warp::test::request()
                .path("/path1/")
                .filter(&elements(catalog)),
        )
        .unwrap();

        assert_eq!(path1.0, "path1/");

        assert_eq!(path1.1 .0, ["sub"]);

        assert_eq!(path1.1 .1, ["hula.nc", "hula2.nc"]);
    }

    #[test]
    fn elements_only_folder_in_path() {
        let catalog = TestCatalog::test();

        assert_eq!(
            block_on(
                warp::test::request()
                    .path("/path3/")
                    .filter(&elements(catalog))
            )
            .unwrap()
            .1
             .0,
            ["path4"]
        );
    }
}
