//! Nullable integer result arrays keep their shared pool admission after slicing.
use super::super::error;
use arrow::{
    array::{ArrayRef, Int64Builder},
    datatypes::SchemaRef,
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
    owner: Arc<Owner>,
    len: usize,
}
impl ResultBatch {
    pub fn new(schema: SchemaRef, capacity: usize, resources: &Resources) -> Result<Self> {
        let bytes = capacity
            .checked_mul(schema.fields().len() * 16)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| error("v3 result admission overflow"))?;
        let admission = resources.execution.reserve(bytes).map_err(error)?;
        let columns = (0..schema.fields().len())
            .map(|_| Int64Builder::with_capacity(capacity))
            .collect();
        Ok(Self {
            schema,
            columns,
            owner: Arc::new(Owner {
                _admission: admission,
                _lease: resources.lease.clone(),
            }),
            len: 0,
        })
    }
    pub fn push(&mut self, values: &[Option<i64>]) {
        assert_eq!(values.len(), self.columns.len());
        for (c, v) in self.columns.iter_mut().zip(values) {
            c.append_option(*v);
        }
        self.len += 1;
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn finish(mut self) -> Result<RecordBatch> {
        let columns = self
            .columns
            .iter_mut()
            .map(|b| {
                let array: ArrayRef = Arc::new(b.finish());
                grust_arrow::retain_array_owner(&array, self.owner.clone()).map_err(error)
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(self.schema, columns).map_err(error)
    }
}
