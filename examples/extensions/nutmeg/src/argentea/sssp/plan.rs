//! Lazy provider: driver planning exposes only schema, worker execution is P-wide.
use std::{fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
};
use datafusion_common::{Result, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use futures::{TryStreamExt, stream};

use super::super::error;
use super::{output::Output, request::Request, state::SsspState};

#[derive(Debug)]
pub struct SsspTable {
    pub request: Request,
    pub inputs: Vec<Arc<dyn ExecutionPlan>>,
    pub state: Option<Arc<SsspState>>,
}

#[async_trait]
impl TableProvider for SsspTable {
    fn schema(&self) -> SchemaRef {
        self.request.output_schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        _session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if projection.is_some() || !filters.is_empty() || limit.is_some() {
            return Err(error(
                "native stage scan requires full output; apply projections/limits in host plan",
            ));
        }
        let state = self
            .state
            .clone()
            .ok_or_else(|| error("schema-only driver provider cannot execute"))?;
        state.base.check()?;
        Ok(Arc::new(ArgenteaSsspExec::new(
            self.request.clone(),
            self.inputs.clone(),
            state,
        )?))
    }
}

#[derive(Debug)]
struct ArgenteaSsspExec {
    request: Request,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    state: Arc<SsspState>,
    properties: Arc<PlanProperties>,
}

impl ArgenteaSsspExec {
    fn new(
        request: Request,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        state: Arc<SsspState>,
    ) -> Result<Self> {
        request.validate_inputs(&inputs)?;
        for input in &inputs {
            if input.properties().partitioning.partition_count() != request.partitions {
                return Err(error(
                    "prepared input partition count differs from native owner count",
                ));
            }
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(request.output_schema()),
            Partitioning::UnknownPartitioning(request.partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            request,
            inputs,
            state,
            properties,
        })
    }
}

impl DisplayAs for ArgenteaSsspExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ArgenteaSsspExec: verb={:?}, phase={}, partitions={}, weighted_distances=true",
            self.request.verb, self.request.phase, self.request.partitions
        )
    }
}

impl ExecutionPlan for ArgenteaSsspExec {
    fn name(&self) -> &str {
        "ArgenteaSsspExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.inputs.len()]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.request.clone(),
            children,
            self.state.clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.state.base.check()?;
        if partition >= self.request.partitions {
            return Err(error("execution partition out of range"));
        }
        let state = self.state.clone();
        let request = self.request.clone();
        let inputs = self.inputs.clone();
        let output = stream::once(async move {
            Output::prepare(state, request, partition, inputs, context)
                .await
                .map(|output| {
                    stream::try_unfold(output, |mut output| async move {
                        // Give peer receivers a chance to drain bounded shuffle
                        // channels between batches, with no native lock retained.
                        tokio::task::yield_now().await;
                        output
                            .next_batch()
                            .map(|batch| batch.map(|batch| (batch, output)))
                    })
                })
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.request.output_schema(),
            output,
        )))
    }
}
