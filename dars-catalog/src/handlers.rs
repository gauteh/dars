use hyper::Body;
use std::sync::Arc;
use tera::Tera;
use warp::Reply;

pub async fn folder(
    root: String,
    tera: Arc<Tera>,
    folder: String,
    elements: (Vec<String>, Vec<String>),
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut ctx = tera::Context::new();
    ctx.insert("title", &folder.as_str());

    ctx.insert("folders", &elements.0);
    ctx.insert("datasets", &elements.1);

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
