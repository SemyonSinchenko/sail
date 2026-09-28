//! Admitted fixed-width Arrow batches and checked wire rows.
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use grust_procedures::MemoryReservation;
use sail_argentea_core::Resources;
use sail_native_resource_ffi::MemoryLease;

use super::error;

struct OutputOwner {
    _reservation: MemoryReservation,
    _lease: Arc<MemoryLease>,
}

pub struct BatchBuilder {
    schema: SchemaRef,
    integers: Vec<Vec<i64>>,
    values: Vec<Vec<f64>>,
    owner: Arc<OutputOwner>,
}

impl BatchBuilder {
    pub fn new(schema: SchemaRef, capacity: usize, resources: &Resources) -> Result<Self> {
        let bytes = capacity
            .checked_mul(schema.fields().len() * 8)
            .and_then(|v| v.checked_add(8192))
            .ok_or_else(|| error("output admission overflow"))?;
        let reservation = resources.execution.reserve(bytes).map_err(error)?;
        let integer_count = schema
            .fields()
            .iter()
            .filter(|f| f.data_type() == &DataType::Int64)
            .count();
        let float_count = schema
            .fields()
            .iter()
            .filter(|f| f.data_type() == &DataType::Float64)
            .count();
        if integer_count + float_count != schema.fields().len() {
            return Err(error("unsupported batch builder schema"));
        }
        let mut integers = Vec::with_capacity(integer_count);
        for _ in 0..integer_count {
            let mut column = Vec::new();
            column.try_reserve_exact(capacity).map_err(error)?;
            integers.push(column);
        }
        let mut values = Vec::with_capacity(float_count);
        for _ in 0..float_count {
            let mut column = Vec::new();
            column.try_reserve_exact(capacity).map_err(error)?;
            values.push(column);
        }
        Ok(Self {
            schema,
            integers,
            values,
            owner: Arc::new(OutputOwner {
                _reservation: reservation,
                _lease: resources.lease.clone(),
            }),
        })
    }

    pub fn push(&mut self, integers: &[i64], value: f64) {
        self.push_values(integers, &[value]);
    }
    pub fn push_values(&mut self, integers: &[i64], values: &[f64]) {
        debug_assert_eq!(integers.len(), self.integers.len());
        for (column, value) in self.integers.iter_mut().zip(integers) {
            column.push(*value);
        }
        debug_assert_eq!(values.len(), self.values.len());
        for (column, value) in self.values.iter_mut().zip(values) {
            column.push(*value);
        }
    }
    pub fn len(&self) -> usize {
        self.values
            .first()
            .map(Vec::len)
            .or_else(|| self.integers.first().map(Vec::len))
            .unwrap_or(0)
    }

    pub fn finish(self) -> Result<RecordBatch> {
        let mut integers = self.integers.into_iter();
        let mut values = self.values.into_iter();
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let array: ArrayRef = match field.data_type() {
                DataType::Int64 => {
                    Arc::new(Int64Array::from(integers.next().expect("fixed schema")))
                }
                DataType::Float64 => {
                    Arc::new(Float64Array::from(values.next().expect("fixed schema")))
                }
                _ => return Err(error("unexpected native output type")),
            };
            columns
                .push(grust_arrow::retain_array_owner(&array, self.owner.clone()).map_err(error)?);
        }
        RecordBatch::try_new(self.schema, columns).map_err(error)
    }
}

pub fn integers<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    let array = batch
        .column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| error(format!("missing Int64 column {name}")))?;
    if array.null_count() != 0 {
        return Err(error(format!("null value in {name}")));
    }
    Ok(array)
}

#[derive(Clone, Copy, Debug)]
pub struct Message {
    pub owner: i64,
    pub kind: i64,
    pub producer: i64,
    pub sequence: i64,
    pub target: i64,
    pub value: f64,
    pub producer_worker: i64,
    pub adjacency_id: i64,
}

pub struct Messages<'a> {
    integers: [&'a Int64Array; 7],
    values: &'a Float64Array,
}
impl<'a> Messages<'a> {
    pub fn new(batch: &'a RecordBatch, expected: &SchemaRef) -> Result<Self> {
        if batch.schema() != *expected {
            return Err(error("wire batch schema/metadata changed"));
        }
        let values = batch
            .column_by_name("value")
            .and_then(|a| a.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| error("missing Float64 value column"))?;
        if values.null_count() != 0 {
            return Err(error("null message value"));
        }
        Ok(Self {
            integers: [
                integers(batch, "owner")?,
                integers(batch, "kind")?,
                integers(batch, "producer")?,
                integers(batch, "sequence")?,
                integers(batch, "target")?,
                integers(batch, "producer_worker")?,
                integers(batch, "adjacency_id")?,
            ],
            values,
        })
    }
    pub fn get(&self, row: usize) -> Message {
        let [
            owner,
            kind,
            producer,
            sequence,
            target,
            producer_worker,
            adjacency_id,
        ] = self.integers.map(|a| a.value(row));
        Message {
            owner,
            kind,
            producer,
            sequence,
            target,
            value: self.values.value(row),
            producer_worker,
            adjacency_id,
        }
    }
}
