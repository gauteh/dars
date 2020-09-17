use crate::Catalog;
use hyper::Body;
use serde::Serialize;
use std::sync::Arc;
use tera::Tera;
use warp::Reply;

#[derive(Serialize)]
struct Element {
    path: String,
    display: String,
}

pub async fn index<T: Catalog + Clone>(
    root: String,
    tera: Arc<Tera>,
    catalog: T,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut ctx = tera::Context::new();
    ctx.insert("root", &root);
    ctx.insert("ndatasets", &catalog.paths().count());
    ctx.insert("title", "");

    tera.render("index.html", &ctx)
        .and_then(|html| {
            Ok(warp::http::Response::builder()
                .header("Content-Type", "text/html")
                .body(Body::from(html)))
        })
        .or_else(|e| {
            println!("error: {:?}", e);
            Ok(Ok(
                warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
            ))
        })
}

pub async fn index_json<T: Catalog + Clone>(
    catalog: T,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    Ok(warp::reply::json(&catalog.paths().collect::<Vec<&str>>()))
}

pub async fn folder(
    root: String,
    tera: Arc<Tera>,
    folder: String,
    elements: (Vec<String>, Vec<String>),
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut ctx = tera::Context::new();
    let folder = folder.trim_end_matches('/');
    ctx.insert("title", &folder);
    ctx.insert("root", &root);

    let folders = elements
        .0
        .iter()
        .map(|e| Element {
            path: format!(
                "{}/data/{}{}",
                root,
                if folder.is_empty() {
                    "".to_string()
                } else {
                    format!("{}/", folder)
                },
                e
            ),
            display: e.to_string(),
        })
        .collect::<Vec<_>>();

    let datasets = elements
        .1
        .iter()
        .map(|e| Element {
            path: format!(
                "{}/data/{}{}",
                root,
                if folder.is_empty() {
                    "".to_string()
                } else {
                    format!("{}/", folder)
                },
                e
            ),
            display: e.to_string(),
        })
        .collect::<Vec<_>>();

    ctx.insert("folders", &folders);
    ctx.insert("datasets", &datasets);

    tera.render("folder.html", &ctx)
        .and_then(|html| {
            Ok(warp::http::Response::builder()
                .header("Content-Type", "text/html")
                .body(Body::from(html)))
        })
        .or_else(|e| {
            println!("error: {:?}", e);
            Ok(Ok(
                warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
            ))
        })
}
