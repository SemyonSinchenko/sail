use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};

use super::statistics::clear_floating_bounds;

#[test]
fn only_floating_bounds_are_removed() {
    let types = [
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Float16)),
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Float32)),
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Float64)),
        DataType::Int64,
        DataType::Utf8,
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
    ];
    let schema = Schema::new(
        types
            .iter()
            .enumerate()
            .map(|(i, t)| Field::new(i.to_string(), t.clone(), true))
            .collect::<Vec<_>>(),
    );
    let column = ColumnStatistics {
        null_count: Precision::Exact(2),
        min_value: Precision::Exact(ScalarValue::Float64(Some(0.5))),
        max_value: Precision::Inexact(ScalarValue::Float64(Some(0.5))),
        sum_value: Precision::Absent,
        distinct_count: Precision::Exact(3),
        byte_size: Precision::Inexact(100),
    };
    let mut statistics = Statistics {
        num_rows: Precision::Exact(5),
        total_byte_size: Precision::Exact(700),
        column_statistics: vec![column.clone(); types.len()],
    };
    let original = statistics.clone();
    clear_floating_bounds(&mut statistics, &schema);
    assert_eq!(statistics.num_rows, original.num_rows);
    assert_eq!(statistics.total_byte_size, original.total_byte_size);
    for (index, actual) in statistics.column_statistics.iter().enumerate() {
        let mut expected = column.clone();
        if index < 6 {
            expected.min_value = Precision::Absent;
            expected.max_value = Precision::Absent;
        }
        assert_eq!(actual, &expected);
    }
    let once = statistics.clone();
    clear_floating_bounds(&mut statistics, &schema);
    assert_eq!(statistics, once);
}
