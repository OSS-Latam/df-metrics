use thiserror::Error;
mod core;
mod metrics;
mod storage;
mod test;

#[derive(Error, Debug)]
pub enum MetricError {
    #[error("DataFusionError: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),
    #[error("Not supported storage backend: {0}")]
    StorageBackendNotSupported(String),
}
