use std::{collections::HashMap, sync::Arc};


use arrow::array::RecordBatch;
use datafusion::{dataframe::DataFrame, datasource::MemTable, error::DataFusionError, logical_expr::Expr, prelude::SessionContext};

use crate::definition::{ExpressionType, Transformation};

async fn execute(batches:Vec<RecordBatch>, transformation_plan:Transformation) -> Result<Vec<RecordBatch>,DataFusionError>{
    let dataset_schema = batches.first().unwrap().schema();
    let table = MemTable::try_new(dataset_schema, vec![batches])?;
    let ctx = SessionContext::new();
    ctx.register_table("obs_table", Arc::new(table))?;
    let table = ctx.table("obs_table").await?;
    let aggregated_expressions = transformation_plan.aggregated_expressions();
    let table = step(aggregated_expressions, table).await?;
    
    table.collect().await
}

async fn step(expressions: HashMap<ExpressionType,Vec<Expr>>, mut dataframe:DataFrame) -> Result<DataFrame,DataFusionError>{
    for (expr_type, expression_vec) in &expressions{
        match expr_type {
            ExpressionType::SELECT => {
                dataframe = dataframe.select(expression_vec.clone())?;
            },
            ExpressionType::AGGREGATE =>{
                dataframe = dataframe.aggregate(expressions.get(&ExpressionType::GROUP).unwrap().clone(), expressions.get(&ExpressionType::AGGREGATE).unwrap().clone())?;
            },
            _ => todo!()
        }
    }
    Ok(dataframe)
}

#[cfg(test)]
mod test{
    use std::sync::Arc;

    use arrow::{array::{Float32Array, Int32Array, RecordBatch, StringArray}, datatypes::{DataType, Field, Schema}};

    use crate::definition::TransformationBuilder;

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
        let builder = TransformationBuilder::new();
        let transform = builder
        .select(vec!["id","value","category"])
        .count(vec!["value"])
        .group_by(vec!["category"])
        .build();

        execute(vec![record_batch.unwrap()],transform);
    }
}