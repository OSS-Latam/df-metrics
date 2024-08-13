

// Define the Instruction enum
#[derive(Debug, Clone,PartialEq)]
pub enum Instruction {
    Select(Vec<String>),
    GroupBy(Vec<String>),
    Aggregate(AggregateType, Vec<String>),
}

#[derive(Debug, Clone,PartialEq)]
pub enum AggregateType {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}


#[derive(Debug)]
pub struct TransformationBuilder {
    instructions: Vec<Instruction>,
}

impl TransformationBuilder {
    pub fn new() -> Self {
        Self {
            instructions: Vec::new(),
        }
    }

    pub fn select(mut self, columns: Vec<&str>) -> Self {
        self.instructions.push(Instruction::Select(columns.iter().map(|&c| c.to_string()).collect()));
        self
    }

    pub fn group_by(mut self, columns: Vec<&str>) -> Self {
        self.instructions.push(Instruction::GroupBy(columns.iter().map(|&c| c.to_string()).collect()));
        self
    }

    pub fn aggregate(mut self, agg_type: AggregateType, columns: Vec<&str>) -> Self {
        self.instructions.push(Instruction::Aggregate(agg_type, columns.iter().map(|&c| c.to_string()).collect()));
        self
    }

    pub fn build(self) -> Transformation {
        Transformation {
            instructions: self.instructions,
        }
    }
}


// Define the Transformation struct to hold the list of Instructions
#[derive(Debug,PartialEq)]
pub struct Transformation {
    pub instructions: Vec<Instruction>,
}

#[derive(Debug,PartialEq,Hash,Eq)]
pub enum ExpressionType{
    SELECT,
    GROUP,
    AGGREGATE
}

#[cfg(test)]
mod tests {

    use super::{AggregateType, Instruction, TransformationBuilder};

    #[test]
    fn test_build_transformation(){
        let builder = TransformationBuilder::new();
        let transform = builder
        .select(vec!["id","value","category"])
        .aggregate(AggregateType::Count,vec!["value"])
        .group_by(vec!["category"])
        .build();
        let expected_instruction = Instruction::Select(vec!["".to_string(),"".to_string(),"".to_string()]);

        assert_eq!(transform.instructions.contains(&expected_instruction),true)
    }
}