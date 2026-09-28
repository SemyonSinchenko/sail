//! Serializable worker-native regions with host-owned job identity and lifetime.
//!
//! This transports descriptors and ordinary host child plans, never a driver
//! pointer or a process-local plan identifier. Package factories are installed
//! by the worker session after independent package/build validation.
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use arrow_schema::{DataType, Schema};
use async_trait::async_trait;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{Result, plan_datafusion_err, plan_err};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

use crate::extension::{SessionExtension, SessionExtensionAccessor};

pub const WORKER_CODEC_PREFIX: &[u8] = b"SAIL_WORKER_EXTENSION_V1\0";
pub const MAX_WORKER_DESCRIPTOR_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerInputRouting {
    IntegerRange {
        column: String,
        split_points: Vec<i64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkerDescriptor {
    pub package_identity: String,
    pub type_url: String,
    /// Opaque operation label scoped to the authoritative host job and package.
    /// It grants no cross-job access; graph-specific generation/rounds stay opaque.
    pub operation_id: String,
    pub payload: Vec<u8>,
    pub input_names: Vec<String>,
    pub input_schemas: Vec<Schema>,
    pub input_routing: Vec<Option<WorkerInputRouting>>,
    pub output_schema: Schema,
    pub partitions: usize,
}

impl WorkerDescriptor {
    pub fn validate(&self) -> Result<()> {
        if [&self.package_identity, &self.type_url, &self.operation_id]
            .iter()
            .any(|x| x.is_empty() || x.len() > 4096)
            || self.payload.len() > 262_144
            || !(1..=65_536).contains(&self.partitions)
            || self.input_names.len() > 16
            || self.input_names.len() != self.input_schemas.len()
            || self.input_names.len() != self.input_routing.len()
        {
            return plan_err!("invalid worker extension descriptor bounds");
        }
        let mut names = HashSet::new();
        for ((name, schema), routing) in self
            .input_names
            .iter()
            .zip(&self.input_schemas)
            .zip(&self.input_routing)
        {
            if name.is_empty() || name.len() > 256 || !names.insert(name) {
                return plan_err!("invalid or duplicate worker extension input name");
            }
            if let Some(WorkerInputRouting::IntegerRange {
                column,
                split_points,
            }) = routing
                && (column.is_empty()
                    || column.len() > 256
                    || split_points.len() + 1 != self.partitions
                    || split_points.windows(2).any(|x| x[0] >= x[1])
                    || schema
                        .field_with_name(column)
                        .map(|f| f.data_type() != &DataType::Int64)
                        .unwrap_or(true))
            {
                return plan_err!("invalid worker extension integer range routing");
            }
        }
        Ok(())
    }
}

/// Comes from Sail's task runner, not the serialized extension descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkerJobIdentity {
    pub session_id: String,
    pub job_id: u64,
}

#[derive(Debug, Clone)]
pub struct WorkerTaskScope {
    pub job: WorkerJobIdentity,
    pub worker_id: u64,
    pub stage: usize,
    pub partition: usize,
    pub attempt: usize,
}

impl SessionExtension for WorkerTaskScope {
    fn name() -> &'static str {
        "WorkerTaskScope"
    }
}

#[async_trait]
pub trait WorkerExtensionFactory: Debug + Send + Sync {
    /// Inputs are the prepared, host-local shuffle plans. Factories must retain
    /// FFI/context owners with their returned plan and validate their own closed
    /// job state atomically before publishing any native state after an await.
    async fn materialize(
        &self,
        descriptor: &WorkerDescriptor,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        scope: Arc<WorkerTaskScope>,
        context: Arc<TaskContext>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Idempotently cancel and remove job-owned state. In-flight streams keep
    /// admitted owners until their final drop; this must not invalidate buffers.
    fn close_job(&self, _job: &WorkerJobIdentity) {}
}

#[derive(Debug, Default)]
struct RegistryState {
    factories: HashMap<String, Arc<dyn WorkerExtensionFactory>>,
    // Session-lifetime tombstones prevent late preparation from resurrecting a
    // closed job. A session owns its registry; no process-global job state.
    closed: HashSet<WorkerJobIdentity>,
}

#[derive(Debug, Default)]
pub struct WorkerExtensionRegistry(Mutex<RegistryState>);

impl SessionExtension for WorkerExtensionRegistry {
    fn name() -> &'static str {
        "WorkerExtensionRegistry"
    }
}

impl WorkerExtensionRegistry {
    pub fn register(
        &self,
        identity: String,
        factory: Arc<dyn WorkerExtensionFactory>,
    ) -> Result<()> {
        if identity.is_empty() || identity.len() > 4096 {
            return plan_err!("invalid worker extension package identity");
        }
        let mut state = self
            .0
            .lock()
            .map_err(|_| plan_datafusion_err!("worker extension registry poisoned"))?;
        if state.factories.contains_key(&identity) {
            return plan_err!("duplicate worker extension package identity");
        }
        state.factories.insert(identity, factory);
        Ok(())
    }

    pub fn ensure_open(&self, job: &WorkerJobIdentity) -> Result<()> {
        let state = self
            .0
            .lock()
            .map_err(|_| plan_datafusion_err!("worker extension registry poisoned"))?;
        if state.closed.contains(job) {
            return plan_err!("worker extension job has closed");
        }
        Ok(())
    }

    pub fn close_job(&self, job: &WorkerJobIdentity) -> Result<()> {
        let factories = {
            let mut state = self
                .0
                .lock()
                .map_err(|_| plan_datafusion_err!("worker extension registry poisoned"))?;
            if !state.closed.insert(job.clone()) {
                return Ok(());
            }
            state.factories.values().cloned().collect::<Vec<_>>()
        };
        for factory in factories {
            factory.close_job(job);
        }
        Ok(())
    }

    fn lookup(&self, identity: &str) -> Result<Arc<dyn WorkerExtensionFactory>> {
        self.0.lock().map_err(|_| plan_datafusion_err!("worker extension registry poisoned"))?
            .factories.get(identity).cloned()
            .ok_or_else(|| plan_datafusion_err!("worker extension package is unavailable or has a different identity: {identity}"))
    }
}

#[derive(Debug)]
pub struct WorkerExtensionExec {
    pub descriptor: Arc<WorkerDescriptor>,
    properties: Arc<PlanProperties>,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
}

impl WorkerExtensionExec {
    pub fn new(descriptor: WorkerDescriptor, inputs: Vec<Arc<dyn ExecutionPlan>>) -> Result<Self> {
        descriptor.validate()?;
        if inputs.len() != descriptor.input_schemas.len()
            || inputs
                .iter()
                .zip(&descriptor.input_schemas)
                .any(|(input, schema)| input.schema().as_ref() != schema)
        {
            return plan_err!("worker extension input arity/schema mismatch");
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(descriptor.output_schema.clone())),
            Partitioning::UnknownPartitioning(descriptor.partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            descriptor: Arc::new(descriptor),
            properties,
            inputs,
        })
    }

    pub fn encode(&self, buffer: &mut Vec<u8>) -> Result<()> {
        let descriptor = serde_json::to_vec(self.descriptor.as_ref())
            .map_err(|e| plan_datafusion_err!("worker extension descriptor: {e}"))?;
        if WORKER_CODEC_PREFIX.len() + descriptor.len() > MAX_WORKER_DESCRIPTOR_BYTES {
            return plan_err!("worker extension descriptor exceeds byte limit");
        }
        buffer.extend_from_slice(WORKER_CODEC_PREFIX);
        buffer.extend_from_slice(&descriptor);
        Ok(())
    }

    pub fn decode(
        buffer: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        context: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if buffer.len() > MAX_WORKER_DESCRIPTOR_BYTES {
            return plan_err!("worker extension descriptor exceeds byte limit");
        }
        let payload = buffer
            .strip_prefix(WORKER_CODEC_PREFIX)
            .ok_or_else(|| plan_datafusion_err!("unknown worker extension codec owner/version"))?;
        let descriptor: WorkerDescriptor = serde_json::from_slice(payload)
            .map_err(|e| plan_datafusion_err!("worker extension descriptor: {e}"))?;
        descriptor.validate()?;
        // Reject absent/mismatched package before touching any native FFI layout.
        context
            .extension::<WorkerExtensionRegistry>()?
            .lookup(&descriptor.package_identity)?;
        Ok(Arc::new(Self::new(descriptor, inputs.to_vec())?))
    }
}

impl DisplayAs for WorkerExtensionExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerExtensionExec: package={}, operation={}, partitions={}, retry=disabled",
            self.descriptor.package_identity,
            self.descriptor.operation_id,
            self.descriptor.partitions
        )
    }
}

impl ExecutionPlan for WorkerExtensionExec {
    fn name(&self) -> &str {
        "WorkerExtensionExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution; self.inputs.len()]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.inputs.len()]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new((*self.descriptor).clone(), children)?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let scope = context.extension::<WorkerTaskScope>()?;
        if scope.attempt != 0
            || partition != scope.partition
            || partition >= self.descriptor.partitions
        {
            return plan_err!(
                "worker extension requires its original worker partition and one attempt"
            );
        }
        let registry = context.extension::<WorkerExtensionRegistry>()?;
        registry.ensure_open(&scope.job)?;
        let factory = registry.lookup(&self.descriptor.package_identity)?;
        let descriptor = self.descriptor.clone();
        let inputs = self.inputs.clone();
        let schema = self.schema();
        let stream = futures::stream::once(async move {
            registry.ensure_open(&scope.job)?;
            let plan = factory
                .materialize(&descriptor, inputs, scope.clone(), context.clone())
                .await?;
            registry.ensure_open(&scope.job)?;
            if plan.schema().as_ref() != &descriptor.output_schema
                || plan.properties().partitioning.partition_count() != descriptor.partitions
            {
                return plan_err!("worker extension materialized schema/partition count mismatch");
            }
            plan.execute(partition, context)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

pub fn contains_worker_extension(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<WorkerExtensionExec>() || plan.children().into_iter().any(contains_worker_extension)
}

#[cfg(test)]
#[path = "worker_extension/tests.rs"]
mod tests;
