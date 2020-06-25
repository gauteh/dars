use std::sync::Arc;

mod dataset;
pub mod filters;
mod handlers;

pub use dataset::{DatasetType, Datasets};
pub type State = Arc<Datasets>;
