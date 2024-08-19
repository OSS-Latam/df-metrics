use crate::core::definition::{AggregateType, ExprValue, Instruction};
use datafusion::common::DataFusionError;
use datafusion::dataframe::DataFrame;
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::logical_expr::Expr;

pub async fn parse(
    instructions: &Vec<Instruction>,
    mut dataframe: DataFrame,
) -> Result<DataFrame, DataFusionError> {
    for instruction in instructions {
        match instruction {
            Instruction::Select(columns) => {
                dataframe = dataframe.select(columns.to_vec())?;
            }
            // aggregated instructions are tightly couple to group by
            Instruction::GroupBy(columns) => {
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
                                        .map(|ExprValue(alias, c)| match i {
                                            Instruction::Aggregate(AggregateType::Sum, _) => {
                                                sum(c.clone()).alias(alias)
                                            }
                                            Instruction::Aggregate(AggregateType::Avg, _) => {
                                                avg(c.clone()).alias(alias)
                                            }
                                            Instruction::Aggregate(AggregateType::Min, _) => {
                                                min(c.clone()).alias(alias)
                                            }
                                            Instruction::Aggregate(AggregateType::Max, _) => {
                                                max(c.clone()).alias(alias)
                                            }
                                            Instruction::Aggregate(AggregateType::Count, _) => {
                                                count(c.clone()).alias(alias)
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
                    dataframe = dataframe.aggregate(columns.to_vec(), agg_exprs)?;
                }
            }
            Instruction::Filter(condition) => {
                let filter_expr = dataframe.parse_sql_expr(condition)?;
                dataframe = dataframe.filter(filter_expr)?;
            }
            Instruction::Literal(alias, expr) | Instruction::NewCol(alias, expr) => {
                dataframe = dataframe.with_column(alias, expr.clone())?;
            }
            _ => {}
        }
    }

    Ok(dataframe)
}
