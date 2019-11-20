use http::status::StatusCode;

pub async fn catalog(_cx: tide::Context<()>) -> tide::EndpointResult {
    warn!("catalog.xml not implemented");
    Err(StatusCode::NOT_IMPLEMENTED)?
}

