use async_trait::async_trait;

use futures::AsyncBufRead;

use dap2::das::Das;
use dap2::dds::Dds;

/// A dataset provides endpoints for the metadata or data requests over the DAP2 or DAP4 protocol.
///
/// Provide stream of data and access to metadata.
#[async_trait]
pub trait Dataset {
    async fn das(&self) -> &Das;
    async fn dds(&self) -> &Dds;
    async fn raw(&self) -> tide::Result;

    // TODO: Any way we can get rid of the Box here? Maybe a wrapper that can take any
    // AsyncBufRead? difficult to do without make Dataset -> Dataset<T>. Then we need
    // to type out T when impl'ing, which is maybe doable.
    async fn dods(&self, variable: &str /* constraints */) -> Box<dyn AsyncBufRead> {
        todo!();
    }
}
