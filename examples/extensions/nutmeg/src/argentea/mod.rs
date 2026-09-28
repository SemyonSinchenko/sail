//! Argentea worker role in the Nutmeg wheel. Existing driver Banda is unchanged.
mod batches;
mod input;
mod output;
mod plan;
mod request;
mod state;

use datafusion::catalog::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
use datafusion_execution::{TaskContext, TaskContextProvider};
use datafusion_ffi::{execution_plan::FFI_ExecutionPlan, table_provider::FFI_TableProvider};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyCapsule, PyDict},
};
use sail_native_resource_ffi::{MEMORY_LEASE_CAPSULE, MemoryLease};
use std::sync::Arc;

use crate::{
    RUNTIME,
    context::{ContextProvider, OwnedProvider},
};
use plan::ArgenteaTable;
use request::Request;
use state::{Incarnation, WorkerState};

fn error(message: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!("argentea: {message}"))
}
fn py_error(message: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(message.to_string())
}

fn import_inputs(inputs: Vec<Bound<'_, PyCapsule>>) -> PyResult<Vec<Arc<dyn ExecutionPlan>>> {
    inputs
        .into_iter()
        .map(|capsule| {
            let pointer = capsule.pointer_checked(Some(c"datafusion_execution_plan"))?;
            // SAFETY: loader checks exact DataFusion/Arrow package identity before
            // entry; capsule ownership remains live while importing the FFI plan.
            let ffi = unsafe { pointer.cast::<FFI_ExecutionPlan>().as_ref() };
            Arc::<dyn ExecutionPlan>::try_from(ffi).map_err(py_error)
        })
        .collect()
}

fn export_provider(py: Python<'_>, table: ArgenteaTable) -> PyResult<Bound<'_, PyCapsule>> {
    let context = Arc::new(ContextProvider(Arc::new(TaskContext::default())));
    let provider: Arc<dyn TableProvider> = Arc::new(OwnedProvider {
        inner: Arc::new(table),
        context: context.clone(),
    });
    let context: Arc<dyn TaskContextProvider> = context;
    let ffi = FFI_TableProvider::new(
        provider,
        false,
        Some(RUNTIME.handle().clone()),
        &context,
        None,
    );
    PyCapsule::new_with_value(py, ffi, c"datafusion_table_provider")
}

/// Schema-only driver bootstrap: imports placeholders but executes nothing and
/// creates no graph state or memory lease.
#[pyfunction]
pub fn plan_worker_relation<'py>(
    py: Python<'py>,
    type_url: &str,
    payload: &[u8],
    inputs: Vec<Bound<'py, PyCapsule>>,
) -> PyResult<Bound<'py, PyDict>> {
    let request = Request::parse(type_url, payload).map_err(py_error)?;
    let inputs = import_inputs(inputs)?;
    request.validate_inputs(&inputs).map_err(py_error)?;
    let result = PyDict::new(py);
    result.set_item("operation_id", &request.operation_id)?;
    result.set_item("partitions", request.partitions)?;
    let mut routing = Vec::new();
    for _ in &inputs {
        let route = PyDict::new(py);
        route.set_item("kind", "integer_range")?;
        route.set_item("column", "owner")?;
        route.set_item(
            "split_points",
            (1..request.partitions as i64).collect::<Vec<_>>(),
        )?;
        routing.push(route);
    }
    result.set_item("input_routing", routing)?;
    result.set_item(
        "provider",
        export_provider(
            py,
            ArgenteaTable {
                request,
                inputs,
                state: None,
            },
        )?,
    )?;
    Ok(result)
}

#[pyclass]
pub struct BoundArgentea {
    state: Arc<WorkerState>,
}

#[pymethods]
impl BoundArgentea {
    #[new]
    fn new(
        incarnation: &str,
        memory_bytes: usize,
        host_resource: Bound<'_, PyCapsule>,
    ) -> PyResult<Self> {
        if incarnation.len() > 8192 {
            return Err(py_error("argentea: oversized host scope"));
        }
        let incarnation: Incarnation = serde_json::from_str(incarnation).map_err(py_error)?;
        let pointer = host_resource.pointer_checked(Some(MEMORY_LEASE_CAPSULE))?;
        // SAFETY: named live host lease, checked fixed ABI header and exact quota
        // before callback clone. Rust allocator/Arc layouts never cross this ABI.
        let lease =
            unsafe { MemoryLease::import(pointer, memory_bytes as u64) }.map_err(py_error)?;
        Ok(Self {
            state: WorkerState::new(incarnation, memory_bytes, lease).map_err(py_error)?,
        })
    }

    fn scalar_udfs(&self) -> Vec<Py<PyAny>> {
        vec![]
    }

    fn plan_relation<'py>(
        &self,
        py: Python<'py>,
        type_url: &str,
        payload: &[u8],
        inputs: Vec<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let request = Request::parse(type_url, payload).map_err(py_error)?;
        let inputs = import_inputs(inputs)?;
        request.validate_inputs(&inputs).map_err(py_error)?;
        self.state.configure(&request).map_err(py_error)?;
        export_provider(
            py,
            ArgenteaTable {
                request,
                inputs,
                state: Some(self.state.clone()),
            },
        )
    }

    /// Idempotent host CloseJob callback, even while plans or streams hold owner.
    fn close(&self) -> PyResult<()> {
        self.state.close().map_err(py_error)
    }
}

#[cfg(test)]
mod tests;
