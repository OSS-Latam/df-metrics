use std::sync::Arc;

use crate::core::definition::Transformation;
use crate::core::parser::parse;
use arrow::array::RecordBatch;
use datafusion::{datasource::MemTable, error::DataFusionError, prelude::SessionContext};

pub async fn execute(
    batches: Vec<RecordBatch>,
    transformations: &Transformation,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let dataset_schema = batches.first().unwrap().schema();
    let table = MemTable::try_new(dataset_schema, vec![batches])?;
    let ctx = SessionContext::new();
    ctx.register_table("obs_table", Arc::new(table))?;
    let table = ctx.table("obs_table").await?;
    let logical_plan = parse(&transformations.instructions, table).await?;
    logical_plan.clone().show().await?;
    logical_plan.collect().await
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::core::definition::{AggregateType, TransformationBuilder};
    use crate::test::{assert_record_batches_equal, generate_dataset};
    use arrow::array::Int64Array;
    use arrow::{
        array::{RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::execute;

    #[tokio::test]
    async fn test_execute_dataset() {
        let record_batch = generate_dataset();
        let builder = TransformationBuilder::new();
        let transform = builder
            .select(vec!["id", "value", "category"])
            .aggregate(AggregateType::Count, vec!["value"])
            .group_by(vec!["category"])
            .build();
        let result = execute(vec![record_batch.unwrap()], &transform)
            .await
            .unwrap();

        result.iter().for_each(|batch| {
            assert_eq!(batch.num_columns(), 2);
        });
    }

    #[tokio::test]
    async fn test_execute_dataset_with_filter() {
        let record_batch = generate_dataset();

        let builder = TransformationBuilder::new();
        let transform = builder
            .select(vec!["id", "value", "category"])
            .filter("value > 5")
            .aggregate(AggregateType::Count, vec!["value"])
            .group_by(vec!["category"])
            .build();
        let result = execute(vec![record_batch.unwrap()], &transform)
            .await
            .unwrap();
        let result = result.into_iter().filter(|r| r.num_rows() > 0).collect();

        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]));

        let expected_result = vec![
            RecordBatch::try_new(
                expected_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["c"])),
                    Arc::new(Int64Array::from(vec![1])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                expected_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["b"])),
                    Arc::new(Int64Array::from(vec![1])),
                ],
            )
            .unwrap(),
        ];

        assert_record_batches_equal(result, expected_result);
    }
}
