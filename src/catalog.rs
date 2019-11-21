use http::status::StatusCode;
use super::Data;

pub async fn catalog(_cx: tide::Context<Data>) -> tide::EndpointResult {
    warn!("catalog.xml not implemented");
    Err(StatusCode::NOT_IMPLEMENTED)?
}

