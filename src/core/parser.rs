use crate::core::definition::{AggregateType, Instruction};
use datafusion::common::DataFusionError;
use datafusion::dataframe::DataFrame;
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::logical_expr::{col, Expr};

pub async fn parse(
    instructions: &Vec<Instruction>,
    mut dataframe: DataFrame,
) -> Result<DataFrame, DataFusionError> {
    for instruction in instructions {
        match instruction {
            Instruction::Select(columns) => {
                let exprs: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
                dataframe = dataframe.select(exprs)?;
            }
            Instruction::GroupBy(columns) => {
                let exprs: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
                if let Some(Instruction::Aggregate(_, _)) = instructions
                    .iter()
                    .find(|i| matches!(i, Instruction::Aggregate(_, _)))
                {
                    let agg_exprs: Vec<Expr> = instructions
                        .iter()
                        .filter_map(|i| {
                            if let Instruction::Aggregate(_, cols) = i {
                                Some(
                                    cols.iter()
                                        .map(|c| match i {
                                            Instruction::Aggregate(AggregateType::Sum, _) => {
                                                sum(col(c))
                                            }
                                            Instruction::Aggregate(AggregateType::Avg, _) => {
                                                avg(col(c))
                                            }
                                            Instruction::Aggregate(AggregateType::Min, _) => {
                                                min(col(c))
                                            }
                                            Instruction::Aggregate(AggregateType::Max, _) => {
                                                max(col(c))
                                            }
                                            Instruction::Aggregate(AggregateType::Count, _) => {
                                                count(col(c))
                                            }
                                            _ => unreachable!(),
                                        })
                                        .collect::<Vec<Expr>>(),
                                )
                            } else {
                                None
                            }
                        })
                        .flatten()
                        .collect();
                    dataframe = dataframe.aggregate(exprs, agg_exprs)?;
                }
            }
            Instruction::Filter(condition) => {
                let filter_expr = dataframe.parse_sql_expr(condition)?;
                dataframe = dataframe.filter(filter_expr)?;
            }
            _ => {}
        }
    }

    Ok(dataframe)
}
