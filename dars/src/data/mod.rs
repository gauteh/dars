use std::sync::Arc;

mod dataset;
pub mod filters;
mod handlers;

pub use dataset::{Dataset, DatasetType, Datasets};
pub type State = Arc<Datasets>;
