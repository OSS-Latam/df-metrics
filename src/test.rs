use arrow::array::{Float32Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use std::iter::zip;
use std::sync::Arc;

pub fn generate_dataset() -> Result<RecordBatch, ArrowError> {
    let col_id = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
    let col_category = Arc::new(StringArray::from(vec!["a", "a", "b", "b", "c"]));
    let col_value = Arc::new(Float32Array::from(vec![
        Some(2.0),
        None,
        Some(5.0),
        Some(12.3),
        Some(9.5),
    ]));
    let schem = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Float32, true),
    ]));
    RecordBatch::try_new(schem.clone(), vec![col_id, col_category, col_value])
}

pub fn assert_record_batches_equal(
    actual_records: Vec<RecordBatch>,
    expected_records: Vec<RecordBatch>,
) {
    assert_eq!(
        actual_records.len(),
        expected_records.len(),
        "Vectors have different number of record batches"
    );
    let iter = zip(actual_records, expected_records);
    for (batch1, batch2) in iter {
        assert_eq!(batch1.schema(), batch2.schema(), "Schemas do not match");

        assert_eq!(
            batch1.num_columns(),
            batch2.num_columns(),
            "Number of columns do not match"
        );

        assert_eq!(
            batch1.num_rows(),
            batch2.num_rows(),
            "Number of rows do not match"
        );
    }
}
