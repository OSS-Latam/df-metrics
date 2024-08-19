# df-metrics

`df-metrics` is a Rust library for generating operational metrics of analytical workloads from in-memory datasets using Apache Arrow and Datafusion. It provides a flexible and efficient way to manipulate and generate metrics.

## Features

- Metrics Building which currently support data selection, filtering and aggregation
- Built-in metrics. (wip) 
- Support for different execution engines. (wip)
- Publishing metrics to different data stores (wip)

## Usage

Here's a basic example of how to use df-metrics library:
```rust
use crate::core::definition::{AggregateType,TransformationBuilder};
use crate::metrics::MetricsManager;
use crate::storage::StorageBackend;
use arrow::array::{Int32Array, StringArray, Float32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

async fn main() {
    let col_id = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
    let col_category = Arc::new(StringArray::from(vec!["a", "a", "b", "b", "c"]));
    let col_value = Arc::new(Float32Array::from(vec![2.0, 3.0, 5.0, 12.3, 9.5]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Float32, true),
    ]));
    let record_batch = RecordBatch::try_new(schema.clone(), vec![col_id, col_category, col_value]).unwrap();

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
```

This is another example using the built-in metrics

```rust
use crate::core::definition::BuiltInMetricsBuilder;
use crate::metrics::MetricsManager;
use crate::storage::StorageBackend;
use arrow::array::{Int32Array, StringArray, Float32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

async fn main() {

    let col_id = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
    let col_category = Arc::new(StringArray::from(vec!["a", "a", "b", "b", "c"]));
    let col_value = Arc::new(Float32Array::from(vec![Some(2.0), None, Some(5.0), Some(12.3), Some(9.5)]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Float32, true),
    ]));
    let record_batch = RecordBatch::try_new(schema.clone(), vec![col_id, col_category, col_value]).unwrap();
    
    MetricsManager::default()
        .transform(BuiltInMetricsBuilder::new().count_null("value", None))
        .execute(vec![record_batch.unwrap()])
        .publish(StorageBackend::Stdout)
        .await
        .unwrap()
}


```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

### Submitting your work
Please consider the following guidelines before submitting a PR
* Make sure all test are correctly executing and passing by running `cargo test`
* Make sure to run `cargo fmt` to keep code formatting consistent
* Make sure to run `cargo clippy` to catch any potential issues

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

