use async_trait::async_trait;
use futures::AsyncBufRead;

use dap2::dods::{Dods, DodsVariable, ConstrainedVariable};

use super::Hdf5Dataset;

#[async_trait]
impl Dods for Hdf5Dataset {
    async fn variable(&self, variable: &ConstrainedVariable) -> DodsVariable {
        todo!()
    }
}

