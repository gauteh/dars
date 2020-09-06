use hyper::Body;
use std::convert::Infallible;
use std::sync::Arc;
use tera::Tera;

pub async fn folder(
    root: String,
    tera: Arc<Tera>,
    elements: Vec<String>,
) -> Result<impl warp::Reply, Infallible> {
    Ok(warp::http::Response::builder()
        .header("Content-Type", "text/html")
        .body(Body::from(
        format!(
        "Index of datasets:<br/><br/>{}",
        elements.iter()
            .map(|s|
                format!("   {} [<a href=\"/data/{}\">dap</a>][<a href=\"/data/{}\">raw</a>] ([<a href=\"/data/{}.das\">das</a>][<a href=\"/data/{}.dds\">dds</a>][<a href=\"/data/{}.dods\">dods</a>])<br />",
                s, s, s, s, s, s)
            )
            .collect::<Vec<String>>()
            .join("\n")
    ))))
}
