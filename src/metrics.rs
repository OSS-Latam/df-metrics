use arrow::array::RecordBatch;

use crate::core::computing::execute;
use crate::core::definition::Transformation;
use crate::storage::StorageBackend;
use crate::MetricError;

/// `MetricsManager` is responsible for managing and executing transformations on data record batches.
/// # Examples
/// ```ignore
/// MetricsManager::default()
///             .transform(BuiltInMetricsBuilder::new().count_null("value", None))
///             .execute(vec![record_batch.unwrap()])
///             .publish(StorageBackend::Stdout)
///             .await
///             .unwrap()
/// ```
#[derive(Debug, Default)]
struct MetricsManager {
    transformation: Transformation,
    batches: Vec<RecordBatch>,
}
impl MetricsManager {
    pub fn default() -> MetricsManager {
        MetricsManager {
            transformation: Transformation::default(),
            batches: Vec::new(),
        }
    }
    pub fn transform(mut self, transformation: Transformation) -> MetricsManager {
        self.transformation = transformation;
        self
    }

    pub fn execute(mut self, batches: Vec<RecordBatch>) -> MetricsManager {
        self.batches = batches;
        self
    }

    /// Execution the instructions and publishes the results of the transformation to the specified storage backend.
    ///
    /// # Arguments
    ///
    /// * `storage_backend` - The `StorageBackend` where the results will be published.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    ///
    /// # Errors
    ///
    /// This function will return an error if the specified storage backend is not supported.
    pub async fn publish(&self, storage_backend: StorageBackend) -> Result<(), MetricError> {
        let result = execute(self.batches.clone(), &self.transformation)
            .await
            .unwrap();

        match storage_backend {
            StorageBackend::Stdout => {
                for batch in result {
                    //todo: use std::io::stdout instead of print
                    println!("{:?}", batch);
                }
                Ok(())
            }
            _ => Err(MetricError::StorageBackendNotSupported(
                storage_backend.to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::core::definition::{AggregateType, BuiltInMetricsBuilder, TransformationBuilder};
    use crate::metrics::MetricsManager;
    use crate::storage::StorageBackend;
    use crate::test::generate_dataset;

    #[tokio::test]
    async fn test_metrics_manager() {
        let record_batch = generate_dataset();
        MetricsManager::default()
            .transform(
                TransformationBuilder::new()
                    .select(vec!["id", "value", "category"])
                    .aggregate(AggregateType::Sum, vec!["value"])
                    .group_by(vec!["category"])
                    .build(),
            )
            .execute(vec![record_batch.unwrap()])
            .publish(StorageBackend::Stdout)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_count_null_metrics() {
        let record_batch = generate_dataset();
        MetricsManager::default()
            .transform(BuiltInMetricsBuilder::new().count_null("value", None))
            .execute(vec![record_batch.unwrap()])
            .publish(StorageBackend::Stdout)
            .await
            .unwrap()
    }
}
