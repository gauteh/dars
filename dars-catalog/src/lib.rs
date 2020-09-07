use lazy_static::lazy_static;
use rust_embed::RustEmbed;
use std::sync::Arc;
use tera::Tera;
use warp::Filter;

mod filters;
mod handlers;

#[derive(RustEmbed)]
#[folder = "src/templates/"]
struct Templates;

/// Builds a catalog with root-url `url`. The handlers for this filter takes list of datasets.
pub fn catalog<T: Catalog + Clone>(
    root: String,
    catalog: T,
) -> Result<impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone, anyhow::Error>
{
    lazy_static! {
        static ref TERA: Arc<Tera> = {
            let mut tera = Tera::default();
            for t in &["base.html", "folder.html", "index.html"] {
                let template = Templates::get(&t).unwrap();
                let template = std::str::from_utf8(&template).unwrap();
                tera.add_raw_template(&t, &template).unwrap();
            }
            Arc::new(tera)
        };
    }

    Ok(filters::catalog(root, Arc::clone(&TERA), catalog))
}

pub trait Catalog: Send + Sync {
    /// List of all paths to data sources.
    fn paths<'a>(&'a self) -> Box<dyn Iterator<Item = &str> + 'a>;
}

impl<T: Catalog> Catalog for Arc<T> {
    fn paths<'a>(&'a self) -> Box<dyn Iterator<Item = &str> + 'a> {
        T::paths(self)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use futures::executor::block_on;

    #[derive(Debug)]
    pub struct TestCatalog {
        paths: Vec<String>,
    }

    impl TestCatalog {
        pub fn test() -> Arc<TestCatalog> {
            Arc::new(TestCatalog {
                paths: [
                    "coads1.nc",
                    "coads2.nc",
                    "path1/hula.nc",
                    "path1/hula2.nc",
                    "path1/sub/hula3.nc",
                    "path2/bula.nc",
                ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            })
        }
    }

    impl Catalog for Arc<TestCatalog> {
        fn paths<'a>(&'a self) -> Box<dyn Iterator<Item = &str> + 'a> {
            Box::new(self.paths.iter().map(|s| s.as_str()))
        }
    }

    #[test]
    fn setup_catalog() {
        catalog("http://localhost:8001".into(), TestCatalog::test()).unwrap();
    }

    #[test]
    fn does_not_match_data_source() {
        let f = catalog("http://localhost:8001".into(), TestCatalog::test()).unwrap();

        assert_eq!(
            block_on(
                warp::test::request()
                    .method("GET")
                    .path("/data/coads1.nc")
                    .reply(&f)
            )
            .status(),
            404
        );

        assert_eq!(
            block_on(
                warp::test::request()
                    .method("GET")
                    .path("/data/path1/hula.nc")
                    .reply(&f)
            )
            .status(),
            404
        );

        assert_eq!(
            block_on(
                warp::test::request()
                    .method("GET")
                    .path("/data/path1/non-exist.nc")
                    .reply(&f)
            )
            .status(),
            404
        );
    }

    #[test]
    fn matches_root() {
        let f = catalog("http://localhost:8001".into(), TestCatalog::test()).unwrap();

        assert_eq!(
            block_on(warp::test::request().method("GET").path("/data/").reply(&f)).status(),
            200
        );

        assert_eq!(
            block_on(warp::test::request().method("GET").path("/data").reply(&f)).status(),
            200
        );
    }

    #[test]
    fn matches_subpath() {
        let f = catalog("http://localhost:8001".into(), TestCatalog::test()).unwrap();

        assert_eq!(
            block_on(
                warp::test::request()
                    .method("GET")
                    .path("/data/path1/")
                    .reply(&f)
            )
            .status(),
            200
        );

        assert_eq!(
            block_on(
                warp::test::request()
                    .method("GET")
                    .path("/data/path1")
                    .reply(&f)
            )
            .status(),
            200
        );
    }

    #[test]
    fn does_not_match_missing_subpath() {
        let f = catalog("http://localhost:8001".into(), TestCatalog::test()).unwrap();

        assert_eq!(
            block_on(
                warp::test::request()
                    .method("GET")
                    .path("/data/missing_path1/")
                    .reply(&f)
            )
            .status(),
            404
        );
    }
}
