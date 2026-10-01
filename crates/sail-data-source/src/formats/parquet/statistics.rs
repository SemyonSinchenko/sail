use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion_common::Statistics;
use datafusion_common::stats::Precision;

/// Parquet bounds can exclude NaNs and therefore cannot prove that a floating
/// column is constant. DataFusion can otherwise replace the whole column with
/// an exact singleton bound, losing NaNs (and possibly the sign of zero).
/// Clear the bounds before file statistics are cached or aggregated. Keep row
/// and null counts, including the independent proof that a column is all null.
pub(super) fn clear_floating_bounds(statistics: &mut Statistics, schema: &Schema) {
    for (column, field) in statistics.column_statistics.iter_mut().zip(schema.fields()) {
        if is_floating(field.data_type()) {
            column.min_value = Precision::Absent;
            column.max_value = Precision::Absent;
        }
    }
}

fn is_floating(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
        DataType::Dictionary(_, value) => is_floating(value),
        _ => false,
    }
}
