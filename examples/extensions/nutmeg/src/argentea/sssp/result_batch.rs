//! Nullable weighted result arrays keep their shared pool admission after slicing.
use super::super::error;
use arrow::{
    array::{ArrayRef, Float64Builder, Int64Builder},
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion_common::Result;
use grust_procedures::MemoryReservation;
use sail_argentea_core::Resources;
use sail_native_resource_ffi::MemoryLease;
use std::sync::Arc;
struct Owner {
    _admission: MemoryReservation,
    _lease: Arc<MemoryLease>,
}
pub struct ResultBatch {
    schema: SchemaRef,
    columns: Vec<Int64Builder>,
    distance: Float64Builder,
    owner: Arc<Owner>,
    len: usize,
}
impl ResultBatch {
    pub fn new(schema: SchemaRef, capacity: usize, resources: &Resources) -> Result<Self> {
        let bytes = capacity
            .checked_mul(schema.fields().len() * 16)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| error("v5 result admission overflow"))?;
        let admission = resources.execution.reserve(bytes).map_err(error)?;
        let columns = (0..schema.fields().len() - 1)
            .map(|_| Int64Builder::with_capacity(capacity))
            .collect();
        Ok(Self {
            schema,
            columns,
            distance: Float64Builder::with_capacity(capacity),
            owner: Arc::new(Owner {
                _admission: admission,
                _lease: resources.lease.clone(),
            }),
            len: 0,
        })
    }
    pub fn push(&mut self, values: &[Option<i64>], distance: Option<f64>) {
        assert_eq!(values.len(), self.columns.len());
        for (c, v) in self.columns.iter_mut().zip(values) {
            c.append_option(*v);
        }
        self.distance.append_option(distance);
        self.len += 1;
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn finish(mut self) -> Result<RecordBatch> {
        let mut integers = self.columns.iter_mut();
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let array: ArrayRef = if field.data_type() == &DataType::Float64 {
                Arc::new(self.distance.finish())
            } else {
                Arc::new(integers.next().expect("fixed SSSP result schema").finish())
            };
            columns
                .push(grust_arrow::retain_array_owner(&array, self.owner.clone()).map_err(error)?);
        }
        RecordBatch::try_new(self.schema, columns).map_err(error)
    }
}
