use std::collections::HashMap;

use datafusion::{functions_aggregate::sum::sum, logical_expr::{avg, col, count, max, min, Expr}};

#[derive(Debug)]
struct TransformationBuilder{
    select:Vec<Expr>,
    group_by:Vec<Expr>,
    sum:Vec<Expr>,
    avg:Vec<Expr>,
    min:Vec<Expr>,
    max:Vec<Expr>,
    count:Vec<Expr>
}

impl TransformationBuilder {
    fn new() -> Self{
        Self { 
            select: Vec::new(), 
            group_by: Vec::new(), 
            sum: Vec::new(), 
            avg: Vec::new(), 
            min: Vec::new(), 
            max: Vec::new(), 
            count: Vec::new() }
    }

    fn select(mut self,columns:Vec<&str>) -> Self{
        self.select = columns.iter().map(|c| col(c.to_string())).collect();
        self
    }

    fn group_by(mut self,columns:Vec<&str>) -> Self{
        self.group_by = columns.iter().map(|c| col(c.to_string())).collect();
        self
    }

    fn sum(mut self, columns:Vec<&str>) -> Self{
        self.sum = columns.iter().map(|c|sum(col(c.to_string()))).collect();
        self
    }
    
    fn avg(mut self, columns:Vec<&str>) -> Self{
        self.avg = columns.iter().map(|c|avg(col(c.to_string()))).collect();
        self
    }

    fn min(mut self, columns:Vec<&str>) -> Self{
        self.min = columns.iter().map(|c|min(col(c.to_string()))).collect();
        self
    }

    fn max(mut self, columns:Vec<&str>) -> Self{
        self.max = columns.iter().map(|c|max(col(c.to_string()))).collect();
        self
    }

    fn count(mut self, columns:Vec<&str>) -> Self{
        self.count = columns.iter().map(|c|count(col(c.to_string()))).collect();
        self
    }

    fn build(&self) -> Transformation{
        //TODO: Handle ERRORS
        let agg_op_vec = vec![self.avg.clone(),self.sum.clone(),self.min.clone(),self.max.clone(),self.count.clone()].concat();
        
        let mut expression_map = HashMap::<ExpressionType,Vec<Expr>>::new();
        if self.select.len() > 0{
            expression_map.insert(ExpressionType::SELECT, self.select.clone());
        }

        // If there are not aggregated operation but there are groups then return error
        if self.group_by.len() > 0{
            expression_map.insert(ExpressionType::GROUP, self.group_by.clone());
        }

        if agg_op_vec.len() > 0{
            expression_map.insert(ExpressionType::AGGREGATE, agg_op_vec);
        }

        Transformation {
            aggregated_expressions: expression_map,
            projection_expressions: self.select.clone()
        }
        
    }
}

#[derive(Debug,PartialEq,Hash,Eq)]
pub enum ExpressionType{
    SELECT,
    GROUP,
    AGGREGATE
}

#[derive(Debug,PartialEq)]
pub struct Transformation{
    aggregated_expressions: HashMap<ExpressionType,Vec<Expr>>,
    projection_expressions: Vec<Expr>
}


impl Transformation {
    pub fn aggregated_expressions(self) -> HashMap<ExpressionType, Vec<Expr>>{
        self.aggregated_expressions
    }

    pub fn projection_expressions(self) -> Vec<Expr>{
        self.projection_expressions
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::{col, count};

    use super::TransformationBuilder;

    #[test]
    fn test_build_transformation(){
        let builder = TransformationBuilder::new();
        let transform = builder
        .select(vec!["id","value","category"])
        .count(vec!["value"])
        .group_by(vec!["category"])
        .build();
        let expected_aggs = vec![count(col("value"))];
        assert_eq!(transform.aggregated_expressions().get(&super::ExpressionType::AGGREGATE).unwrap(),&expected_aggs)
    }
}