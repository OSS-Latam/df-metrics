use std::{borrow::Borrow, sync::Arc};

use anyhow::Error;
use arrow::array::RecordBatch;
use datafusion::{datasource::MemTable, error::DataFusionError, prelude::SessionContext};

use crate::definition::{ExpressionType, Transformation};

async fn execute(batches:Vec<RecordBatch>, transformation_plan:Transformation) -> Result<Vec<RecordBatch>,Error>{
    let dataset_schema = batches.first().unwrap().schema();
    let table = MemTable::try_new(dataset_schema, vec![batches])?;
    let ctx = SessionContext::new();
    ctx.register_table("obs_table", Arc::new(table))?;
    let table = ctx.table("obs_table").await?;
    //TODO: FINISH IMPLEMENTATION
    // let aggregated_expressions = &transformation_plan.aggregated_expressions();
    // let projection_expressions = &transformation_plan.projection_expressions();
    // if aggregated_expressions.len() > 0 {
    //     let table = table.aggregate(aggregated_expressions.get(&ExpressionType::GROUP).unwrap().clone(), aggregated_expressions.get(&ExpressionType::AGGREGATE).unwrap().clone())?;
    // }

    // if projection_expressions.len() > 0 {
    //     let table = table.select(projection_expressions.clone());
    // }
    
    Err(anyhow::Error::msg("message"))
}

#[cfg(test)]
mod test{
    use std::sync::Arc;

    use arrow::{array::{Float32Array, Int32Array, RecordBatch, StringArray}, datatypes::{DataType, Field, Schema}};

    use super::execute;

    #[test]
    fn test_execute_dataset() {
        let col_id = Arc::new(Int32Array::from(vec![1,2,3,4,5]));
        let col_category = Arc::new(StringArray::from(vec!["a","a","b","b","c"]));
        let col_value = Arc::new(Float32Array::from(vec![2.0,3.0,5.0,12.3,9.5]));
        let schem = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32,false),
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Float32, true),
            ]));
        let record_batch = RecordBatch::try_new(schem.clone(), vec![col_id,col_category,col_value]);
        // execute(vec![record_batch.unwrap()]);
    }
}