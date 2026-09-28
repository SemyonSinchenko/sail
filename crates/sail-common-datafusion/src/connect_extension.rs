//! Internal host adapters for the experimental local Connect extension API.
//!
//! This Rust interface stays inside Sail. Native packages cross the independently
//! version-checked DataFusion FFI boundary in the package loader, not this trait.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, ReplaceChildrenOptions,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, exec_err, plan_err};
use futures::Stream;
use tokio::runtime::Handle;

use crate::extension::SessionExtension;

/// Planning must only describe execution: it must not consume input or mutate
/// extension state. AnalyzePlan can invoke this method without executing a query.
pub trait ConnectRelationHandler: Send + Sync + 'static {
    fn plan(
        &self,
        payload: &[u8],
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn TableProvider>>;
}

struct RegisteredHandler {
    accepts_bare: bool,
    min_inputs: usize,
    max_inputs: usize,
    handler: Arc<dyn ConnectRelationHandler>,
}

/// One registry is bound to one server session. Package discovery may be shared;
/// handlers and the graph state they own must be instantiated for this session.
#[derive(Default)]
pub struct ConnectExtensionRegistry {
    handlers: BTreeMap<String, RegisteredHandler>,
}

impl ConnectExtensionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        type_url: String,
        accepts_bare: bool,
        min_inputs: usize,
        max_inputs: usize,
        handler: Arc<dyn ConnectRelationHandler>,
    ) -> Result<()> {
        if type_url.is_empty() || type_url.len() > 512 {
            return plan_err!("extension type URL must contain between 1 and 512 bytes");
        }
        if min_inputs > max_inputs || (accepts_bare && min_inputs != 0) {
            return plan_err!("invalid input arity registration for extension {type_url}");
        }
        if self.handlers.contains_key(&type_url) {
            return plan_err!("duplicate Connect extension type URL: {type_url}");
        }
        self.handlers.insert(
            type_url,
            RegisteredHandler {
                accepts_bare,
                min_inputs,
                max_inputs,
                handler,
            },
        );
        Ok(())
    }

    /// Validate before planning children, so invalid requests cannot trigger
    /// planning callbacks in their nested extensions.
    pub fn resolve(
        &self,
        type_url: &str,
        is_envelope: bool,
        input_count: usize,
    ) -> Result<Arc<dyn ConnectRelationHandler>> {
        let Some(entry) = self.handlers.get(type_url) else {
            let registered = self.handlers.keys().cloned().collect::<Vec<_>>().join(", ");
            return plan_err!(
                "unregistered Connect extension type URL: {type_url}; registered: [{registered}]"
            );
        };
        if !is_envelope && !entry.accepts_bare {
            return plan_err!(
                "Connect extension {type_url} requires a SailExtensionRequest envelope"
            );
        }
        if !is_envelope && input_count != 0 {
            return plan_err!("bare Connect extension {type_url} cannot have inputs");
        }
        if !(entry.min_inputs..=entry.max_inputs).contains(&input_count) {
            return plan_err!(
                "Connect extension {type_url} expects {}..={} inputs, received {input_count}",
                entry.min_inputs,
                entry.max_inputs
            );
        }
        Ok(Arc::clone(&entry.handler))
    }
}

impl SessionExtension for ConnectExtensionRegistry {
    fn name() -> &'static str {
        "ConnectExtensionRegistry"
    }
}

/// Executes a previously planned Sail input under its original host context.
///
/// DataFusion 55's foreign TaskContext reconstruction does not preserve Sail's
/// RuntimeEnv. A foreign consumer therefore receives this wrapper, whose execute
/// ignores the reconstructed context. Local/driver adapters gather every input
/// partition; worker adapters preserve the prepared partitioning explicitly.
/// The host runtime is retained and entered for execution and
/// stream polls: FFI child replacement can discard the FFI wrapper's runtime.
/// This is a local-mode bridge, not a serializable remote node.
pub struct HostInputExec {
    input: Arc<dyn ExecutionPlan>,
    original: Arc<dyn ExecutionPlan>,
    gathered: bool,
    context: Arc<TaskContext>,
    runtime: Handle,
}

impl HostInputExec {
    pub fn gathered_input(&self) -> Arc<dyn ExecutionPlan> {
        self.input.clone()
    }

    /// Remove only the gathering introduced by this adapter. An explicit
    /// coalescer already present in the caller's plan remains part of the input.
    pub fn partitioned_input(&self) -> Arc<dyn ExecutionPlan> {
        self.original.clone()
    }

    pub fn new(input: Arc<dyn ExecutionPlan>, context: Arc<TaskContext>, runtime: Handle) -> Self {
        Self {
            input: Arc::new(CoalescePartitionsExec::new(input.clone())),
            original: input,
            gathered: true,
            context,
            runtime,
        }
    }

    /// Preserve prepared worker partitions while retaining the host runtime and
    /// task context across FFI. Driver/local callers keep using `new` above.
    pub fn new_partitioned(
        input: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        runtime: Handle,
    ) -> Self {
        Self {
            input: input.clone(),
            original: input,
            gathered: false,
            context,
            runtime,
        }
    }
}

impl Debug for HostInputExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostInputExec")
            .field("input", &self.input)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for HostInputExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HostInputExec: host_context=true, host_runtime=true, partitions={}, gathered={}",
            self.input.properties().partitioning.partition_count(),
            self.gathered
        )
    }
}

impl ExecutionPlan for HostInputExec {
    fn name(&self) -> &'static str {
        "HostInputExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("HostInputExec requires exactly one child");
        }
        if children[0].schema() != self.input.schema() {
            return plan_err!("HostInputExec replacement child has a different schema");
        }
        if self.gathered {
            // Re-establish the original single-partition contract after FFI
            // replacement, which does not carry all input requirements.
            Ok(Arc::new(Self::new(
                children.remove(0),
                self.context.clone(),
                self.runtime.clone(),
            )))
        } else {
            if children[0].properties().partitioning.partition_count()
                != self.input.properties().partitioning.partition_count()
            {
                return plan_err!("partitioned host input replacement changes partition count");
            }
            Ok(Arc::new(Self::new_partitioned(
                children.remove(0),
                self.context.clone(),
                self.runtime.clone(),
            )))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _foreign_context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.input.properties().partitioning.partition_count() {
            return exec_err!(
                "HostInputExec partition {partition} is outside the advertised input"
            );
        }
        let _guard = self.runtime.enter();
        let inner = self.input.execute(partition, Arc::clone(&self.context))?;
        Ok(Box::pin(HostInputStream {
            inner,
            runtime: self.runtime.clone(),
        }))
    }
}

struct HostInputStream {
    inner: SendableRecordBatchStream,
    runtime: Handle,
}

impl Stream for HostInputStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _guard = this.runtime.enter();
        this.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for HostInputStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests;
