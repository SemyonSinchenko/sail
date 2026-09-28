//! Package bootstrap for worker-native relations. Graph state belongs to one
//! host-issued Sail job and operation, never to the process-wide code cache.
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    Partitioning, PhysicalExpr, PhysicalSortExpr, RangePartitioning, SplitPoint,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use datafusion_common::{Result, ScalarValue, plan_datafusion_err, plan_err};
use datafusion_expr::{Expr, TableType};
use datafusion_ffi::execution_plan::FFI_ExecutionPlan;
use datafusion_ffi::table_provider::FFI_TableProvider;
use futures::StreamExt;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyCapsuleMethods, PyDict, PyDictMethods, PyList};
use sail_common_datafusion::connect_extension::{
    ConnectExtensionRegistry, ConnectRelationHandler, HostInputExec,
};
use sail_common_datafusion::native_resource::{MEMORY_LEASE_CAPSULE, NativeResourceTracker};
use sail_common_datafusion::worker_extension::{
    WorkerDescriptor, WorkerExtensionExec, WorkerExtensionFactory, WorkerInputRouting,
    WorkerJobIdentity, WorkerTaskScope,
};
use sail_physical_plan::repartition::ExplicitRepartitionExec;
use serde::Deserialize;

use super::driver::InputPlaceholder;
use super::manifest::Manifest;
use super::py_error;
use super::python_owner::PythonOwner;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerPlanning {
    operation_id: String,
    partitions: usize,
    input_routing: Vec<Option<WorkerInputRouting>>,
}

struct WorkerRelationHandler {
    factory: Arc<PythonOwner>,
    identity: String,
    type_url: String,
    distributed: bool,
}

pub(super) fn register_relations(
    registry: &mut ConnectExtensionRegistry,
    manifest: &Manifest,
    identity: String,
    factory: Arc<PythonOwner>,
    distributed: bool,
) -> Result<()> {
    for relation in &manifest.relation_types {
        registry.register(
            relation.type_url.clone(),
            relation.accepts_bare,
            relation.min_inputs,
            relation.max_inputs,
            Arc::new(WorkerRelationHandler {
                factory: factory.clone(),
                identity: identity.clone(),
                type_url: relation.type_url.clone(),
                distributed,
            }),
        )?;
    }
    Ok(())
}

fn export_input<'py>(
    py: Python<'py>,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Bound<'py, PyCapsule>> {
    let ffi = FFI_ExecutionPlan::new(input, tokio::runtime::Handle::try_current().ok());
    PyCapsule::new_with_value(py, ffi, c"datafusion_execution_plan").map_err(py_error)
}

fn import_provider(value: &Bound<'_, PyAny>) -> Result<Arc<dyn TableProvider>> {
    let capsule = value.cast::<PyCapsule>().map_err(py_error)?;
    let pointer = capsule
        .pointer_checked(Some(c"datafusion_table_provider"))
        .map_err(py_error)?;
    // SAFETY: manifest and package identity were independently checked before
    // invoking this trusted factory; clone the FFI owner while the capsule lives.
    let provider = unsafe { pointer.cast::<FFI_TableProvider>().as_ref().clone() };
    Ok(Arc::<dyn TableProvider>::from(&provider))
}

impl ConnectRelationHandler for WorkerRelationHandler {
    fn plan(
        &self,
        payload: &[u8],
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn TableProvider>> {
        if !self.distributed {
            return plan_err!(
                "worker-native relation {} requires distributed Sail execution",
                self.type_url
            );
        }
        Python::attach(|py| {
            let capsules = PyList::empty(py);
            let mut host_inputs = Vec::with_capacity(inputs.len());
            let mut input_names = Vec::with_capacity(inputs.len());
            for input in inputs {
                let host = input
                    .downcast_ref::<HostInputExec>()
                    .ok_or_else(|| py_error("worker native input is missing host adapter"))?;
                let input = host.partitioned_input();
                let name = format!("SailWorkerInput_{}", uuid::Uuid::new_v4());
                let placeholder = Arc::new(InputPlaceholder {
                    name: name.clone(),
                    properties: input.properties().clone(),
                });
                capsules
                    .append(export_input(py, placeholder)?)
                    .map_err(py_error)?;
                input_names.push(name);
                host_inputs.push(input);
            }
            // This unbound factory may describe a plan only. Placeholders cannot
            // execute, and no worker quota or graph state is allocated here.
            let result = self
                .factory
                .bind(py)
                .map_err(py_error)?
                .call_method1(
                    "plan_worker_relation",
                    (&self.type_url, PyBytes::new(py, payload), capsules),
                )
                .map_err(py_error)?;
            let dictionary = result.cast::<PyDict>().map_err(py_error)?;
            let provider = dictionary
                .get_item("provider")
                .map_err(py_error)?
                .ok_or_else(|| py_error("worker planning result has no provider"))?;
            let provider = import_provider(&provider)?;
            let metadata = dictionary.copy().map_err(py_error)?;
            metadata.del_item("provider").map_err(py_error)?;
            let json = py
                .import("json")
                .and_then(|m| m.call_method1("dumps", (metadata,)))
                .and_then(|value| value.extract::<String>())
                .map_err(py_error)?;
            let planning: WorkerPlanning = serde_json::from_str(&json).map_err(py_error)?;
            let descriptor = WorkerDescriptor {
                package_identity: self.identity.clone(),
                type_url: self.type_url.clone(),
                operation_id: planning.operation_id,
                payload: payload.to_vec(),
                input_names,
                input_schemas: host_inputs
                    .iter()
                    .map(|input| input.schema().as_ref().clone())
                    .collect(),
                input_routing: planning.input_routing,
                output_schema: provider.schema().as_ref().clone(),
                partitions: planning.partitions,
            };
            descriptor.validate()?;
            Ok(Arc::new(WorkerTableProvider {
                descriptor,
                inputs: host_inputs,
                _provider: provider,
            }) as Arc<dyn TableProvider>)
        })
    }
}

#[derive(Debug)]
struct WorkerTableProvider {
    descriptor: WorkerDescriptor,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    _provider: Arc<dyn TableProvider>,
}

#[async_trait]
impl TableProvider for WorkerTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::new(self.descriptor.output_schema.clone())
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !filters.is_empty() {
            return plan_err!("worker native relation does not accept filter pushdown");
        }
        let inputs = self
            .inputs
            .iter()
            .zip(&self.descriptor.input_routing)
            .map(|(input, routing)| {
                route_input(restore_worker_routing(input.clone())?, routing.as_ref())
            })
            .collect::<Result<Vec<_>>>()?;
        let mut plan: Arc<dyn ExecutionPlan> =
            Arc::new(WorkerExtensionExec::new(self.descriptor.clone(), inputs)?);
        // Projection is a host node. Do not serialize a foreign scan or change
        // the native descriptor's declared complete output schema.
        if let Some(projection) = projection {
            let expressions = projection
                .iter()
                .map(|&index| {
                    let field = self
                        .descriptor
                        .output_schema
                        .fields()
                        .get(index)
                        .ok_or_else(|| {
                            plan_datafusion_err!("worker projection index out of bounds")
                        })?;
                    Ok((
                        Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                        field.name().clone(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            plan = Arc::new(ProjectionExec::try_new(expressions, plan)?);
        }
        Ok(plan)
    }
}

/// Child Connect relations may already have completed physical optimization.
/// Re-establish all declared routes before the enclosing relation is optimized:
/// otherwise EnsureRequirements can remove a previously lowered range exchange.
fn restore_worker_routing(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(plan
        .transform_up(|node| {
            let Some(worker) = node.downcast_ref::<WorkerExtensionExec>() else {
                return Ok(Transformed::no(node));
            };
            let inputs = worker
                .children()
                .into_iter()
                .zip(&worker.descriptor.input_routing)
                .map(|(input, routing)| route_input(input.clone(), routing.as_ref()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Transformed::yes(
                node.replace_children(inputs, Default::default())?,
            ))
        })?
        .data)
}

fn route_input(
    input: Arc<dyn ExecutionPlan>,
    routing: Option<&WorkerInputRouting>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(WorkerInputRouting::IntegerRange {
        column,
        split_points,
    }) = routing
    else {
        return Ok(input);
    };
    let index = input.schema().index_of(column)?;
    let expression = Arc::new(Column::new(column, index)) as Arc<dyn PhysicalExpr>;
    let ordering = [PhysicalSortExpr::new_default(expression)].into();
    let splits = split_points
        .iter()
        .map(|&point| SplitPoint::new(vec![ScalarValue::Int64(Some(point))]))
        .collect();
    let partitioning = Partitioning::Range(RangePartitioning::try_new(ordering, splits)?);
    if input.is::<ExplicitRepartitionExec>() && input.properties().partitioning == partitioning {
        return Ok(input);
    }
    // Replace an already-lowered matching exchange instead of stacking another
    // one around it on every nested provider scan.
    let input = match input.downcast_ref::<RepartitionExec>() {
        Some(exchange) if exchange.properties().partitioning == partitioning => {
            exchange.input().clone()
        }
        _ => input,
    };
    // This is protocol routing, rather than an optimizer-selected distribution.
    // Sail's existing explicit node preserves it through EnsureRequirements and
    // is lowered to DataFusion's range exchange by RewriteExplicitRepartition.
    Ok(Arc::new(ExplicitRepartitionExec::new(input, partitioning)))
}

#[derive(Default)]
struct WorkerOwners {
    closed: HashSet<WorkerJobIdentity>,
    owners: HashMap<(WorkerJobIdentity, String), Arc<PythonOwner>>,
}

pub(super) struct PythonWorkerFactory {
    identity: String,
    manifest: Manifest,
    factory: Arc<PythonOwner>,
    runtime: Arc<RuntimeEnv>,
    tracker: Arc<NativeResourceTracker>,
    state: Mutex<WorkerOwners>,
}

impl Debug for PythonWorkerFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonWorkerFactory")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl PythonWorkerFactory {
    pub(super) fn new(
        identity: String,
        manifest: Manifest,
        factory: Arc<PythonOwner>,
        runtime: Arc<RuntimeEnv>,
        tracker: Arc<NativeResourceTracker>,
    ) -> Result<Self> {
        manifest.validate()?;
        if manifest.placement != "worker" {
            return plan_err!("worker factory requires explicit worker placement");
        }
        Ok(Self {
            identity,
            manifest,
            factory,
            runtime,
            tracker,
            state: Mutex::default(),
        })
    }

    fn owner(&self, scope: &WorkerTaskScope, operation: &str) -> Result<Arc<PythonOwner>> {
        // Serialize binding with close and other first users: reserving duplicate
        // quotas transiently can falsely refuse a partition that fits its job.
        let mut state = self
            .state
            .lock()
            .map_err(|_| plan_datafusion_err!("worker native owner lock poisoned"))?;
        if state.closed.contains(&scope.job) {
            return plan_err!("worker native job has closed");
        }
        let key = (scope.job.clone(), operation.to_owned());
        if let Some(owner) = state.owners.get(&key) {
            return Ok(owner.clone());
        }
        let bytes = self
            .manifest
            .memory_bytes
            .ok_or_else(|| plan_datafusion_err!("worker quota missing"))?;
        let owner = Python::attach(|py| -> Result<_> {
            let lease = self
                .tracker
                .reserve(&self.runtime.memory_pool, &self.identity, bytes)?;
            let capsule =
                PyCapsule::new_with_value(py, lease, MEMORY_LEASE_CAPSULE).map_err(py_error)?;
            let incarnation = serde_json::json!({
                "session_id": scope.job.session_id, "job_id": scope.job.job_id,
                "worker_id": scope.worker_id, "package_identity": self.identity,
                "operation_id": operation,
            })
            .to_string();
            let bound = self
                .factory
                .bind(py)
                .map_err(py_error)?
                .call_method1("bind_with_resources", (incarnation, bytes, capsule))
                .map_err(py_error)?;
            if !bound
                .getattr("close")
                .is_ok_and(|callback| callback.is_callable())
            {
                return plan_err!("worker bound extension must provide callable close()");
            }
            let functions = bound.call_method0("scalar_udfs").map_err(py_error)?;
            if functions.try_iter().map_err(py_error)?.next().is_some() {
                return plan_err!(
                    "worker-only extension {} cannot export scalar functions",
                    self.manifest.name
                );
            }
            Ok(Arc::new(PythonOwner::new(bound.unbind())))
        })?;
        state.owners.insert(key, owner.clone());
        Ok(owner)
    }

    fn ensure_open(&self, job: &WorkerJobIdentity) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| plan_datafusion_err!("worker native owner lock poisoned"))?;
        if state.closed.contains(job) {
            return plan_err!("worker native job closed during preparation");
        }
        Ok(())
    }
}

#[async_trait]
impl WorkerExtensionFactory for PythonWorkerFactory {
    async fn materialize(
        &self,
        descriptor: &WorkerDescriptor,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        scope: Arc<WorkerTaskScope>,
        context: Arc<TaskContext>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        descriptor.validate()?;
        if descriptor.package_identity != self.identity {
            return plan_err!("worker native package identity mismatch");
        }
        let relation = self
            .manifest
            .relation_types
            .iter()
            .find(|relation| relation.type_url == descriptor.type_url)
            .ok_or_else(|| {
                plan_datafusion_err!("worker native relation type is not in the manifest")
            })?;
        if !(relation.min_inputs..=relation.max_inputs).contains(&inputs.len())
            || inputs.len() != descriptor.input_schemas.len()
            || inputs
                .iter()
                .zip(&descriptor.input_schemas)
                .any(|(input, schema)| input.schema().as_ref() != schema)
        {
            return plan_err!("worker native input arity/schema mismatch");
        }
        if !Arc::ptr_eq(context.memory_pool(), &self.runtime.memory_pool) {
            return plan_err!("worker native task belongs to a different memory resource domain");
        }
        let owner = self.owner(&scope, &descriptor.operation_id)?;
        let handle = tokio::runtime::Handle::try_current().map_err(py_error)?;
        let provider = Python::attach(|py| -> Result<_> {
            let capsules = PyList::empty(py);
            for input in inputs {
                let input = Arc::new(HostInputExec::new_partitioned(
                    input,
                    context.clone(),
                    handle.clone(),
                ));
                capsules
                    .append(export_input(py, input)?)
                    .map_err(py_error)?;
            }
            let result = owner
                .bind(py)
                .map_err(py_error)?
                .call_method1(
                    "plan_relation",
                    (
                        &descriptor.type_url,
                        PyBytes::new(py, &descriptor.payload),
                        capsules,
                    ),
                )
                .map_err(py_error)?;
            import_provider(&result)
        })?;
        if provider.schema().as_ref() != &descriptor.output_schema {
            return plan_err!("worker native provider schema differs from descriptor");
        }
        let session = SessionContext::new_with_config_rt(
            context.session_config().clone(),
            self.runtime.clone(),
        );
        let native = provider.scan(&session.state(), None, &[], None).await?;
        self.ensure_open(&scope.job)?;
        if native.schema().as_ref() != &descriptor.output_schema
            || native.properties().partitioning.partition_count() != descriptor.partitions
        {
            return plan_err!("worker native scan schema/partition count differs from descriptor");
        }
        Ok(Arc::new(OwnedWorkerExec {
            inner: native,
            provider,
            owner,
        }))
    }

    fn close_job(&self, job: &WorkerJobIdentity) {
        let removed = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed.insert(job.clone());
            let keys = state
                .owners
                .keys()
                .filter(|(owner_job, _)| owner_job == job)
                .cloned()
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| state.owners.remove(&key).map(|owner| (key.1, owner)))
                .collect::<Vec<_>>()
        };
        // Cancel producers even when live plans or streams still own the bound
        // object. The callback must be idempotent and preserve retained Arrow
        // buffers. Both callbacks and finalizers run outside the registry lock.
        // One failing callback must not prevent cleanup of another operation.
        for (operation, owner) in removed {
            let result = Python::attach(|py| owner.bind(py)?.call_method0("close").map(|_| ()));
            if let Err(error) = result {
                log::warn!(
                    "worker extension {} close failed for job {:?}, operation {operation}: {error}",
                    self.identity,
                    job
                );
            }
        }
    }
}

struct OwnedWorkerExec {
    inner: Arc<dyn ExecutionPlan>,
    provider: Arc<dyn TableProvider>,
    owner: Arc<PythonOwner>,
}

impl Debug for OwnedWorkerExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedWorkerExec")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}
impl DisplayAs for OwnedWorkerExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OwnedWorkerExec")
    }
}
impl ExecutionPlan for OwnedWorkerExec {
    fn name(&self) -> &str {
        "OwnedWorkerExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("bound worker-native region is an opaque leaf");
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let keepalive = (
            self.owner.clone(),
            self.provider.clone(),
            self.inner.clone(),
        );
        let stream = self.inner.execute(partition, context)?.map(move |batch| {
            let _ = &keepalive;
            batch
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests;
