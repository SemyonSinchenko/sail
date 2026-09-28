//! Retained snapshots and integer Arrow buffers; no lock spans output backpressure.
use super::{
    super::{
        batches::BatchBuilder,
        error,
        state::{ExecutionGuard, lock},
    },
    input,
    request::{Request, Verb},
    result_batch::ResultBatch,
    state::BfsState,
    wire,
};
use arrow::record_batch::RecordBatch;
use datafusion::{execution::TaskContext, physical_plan::ExecutionPlan};
use datafusion_common::Result;
use sail_argentea_core::{
    BfsCompletion, BfsConvergence, BfsEmissionCursor, BfsMode, BfsPayload, BfsRowCursor,
    BfsStatistics, BfsWork, Round,
};
use std::sync::Arc;
enum Rows {
    Statistics {
        report: BfsStatistics,
        owner: usize,
        item: usize,
    },
    Updates {
        cursor: Option<BfsEmissionCursor>,
        completion: Option<BfsCompletion>,
        owner: usize,
        total: u64,
    },
    Results {
        cursor: BfsRowCursor,
        certificate: BfsConvergence,
    },
}
pub struct Output {
    guard: ExecutionGuard,
    state: Arc<BfsState>,
    request: Request,
    partition: usize,
    adjacency: u64,
    incoming: u64,
    mode: BfsMode,
    levels: u64,
    local_reached: u64,
    frontier: usize,
    work: BfsWork,
    rows: Rows,
}
impl Output {
    pub async fn prepare(
        state: Arc<BfsState>,
        request: Request,
        partition: usize,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        context: Arc<TaskContext>,
    ) -> Result<Self> {
        let operation = state.claim(&request, partition)?;
        let guard = ExecutionGuard {
            state: state.base.clone(),
            complete: false,
        };
        let phase = Round {
            operation: operation.clone(),
            number: request.phase,
        };
        let part = match request.verb {
            Verb::Init => state.publish(
                input::initialize(&state, &request, operation, partition, &inputs, context).await?,
            )?,
            Verb::Decide | Verb::Result => {
                input::statistics(&state, &request, &phase, partition, &inputs[0], context).await?
            }
            Verb::Apply => {
                input::contributions(&state, &request, &phase, partition, &inputs[0], context)
                    .await?
            }
        };
        if matches!(request.verb, Verb::Decide | Verb::Result) {
            let cap = {
                let mut native = lock(&part)?;
                native
                    .cap_failure(&phase)
                    .map_err(error)?
                    .map(|failure| (native.origin().adjacency_id, failure))
            };
            if let Some((adjacency, failure)) = cap {
                state.audit("failure",&request,partition,adjacency,serde_json::json!({"code":"bfs_level_cap","outcome":"nonconverged","levels":failure.levels,"max_levels":failure.max_levels,"frontier_vertices":failure.frontier,"reached":failure.reached}))?;
                return Err(error(failure));
            }
        }
        let (adjacency, incoming, mode, levels, local_reached, frontier, work, rows) = {
            let mut native = lock(&part)?;
            state.base.check()?;
            let mut mode = native.completed_mode();
            let rows = match request.verb {
                Verb::Init | Verb::Apply => Rows::Statistics {
                    report: native.statistics().map_err(error)?,
                    owner: 0,
                    item: 0,
                },
                Verb::Decide => {
                    let cursor = native.start_emission(&phase).map_err(error)?;
                    mode = cursor.mode();
                    Rows::Updates {
                        cursor: Some(cursor),
                        completion: None,
                        owner: 0,
                        total: 0,
                    }
                }
                Verb::Result => {
                    let certificate = native
                        .seal(&phase)
                        .map_err(error)?
                        .ok_or_else(|| error("v3 plan exhausted without zero frontier"))?;
                    Rows::Results {
                        cursor: native.row_cursor().map_err(error)?,
                        certificate,
                    }
                }
            };
            (
                native.origin().adjacency_id,
                native.incoming_identity().unwrap_or(0),
                mode,
                native.levels(),
                native.reached_count(),
                native.frontier_count(),
                native.last_work(),
                rows,
            )
        };
        if adjacency > i64::MAX as u64 || incoming > i64::MAX as u64 {
            return Err(error("v3 adjacency exceeds wire range"));
        }
        Ok(Self {
            guard,
            state,
            request,
            partition,
            adjacency,
            incoming,
            mode,
            levels,
            local_reached,
            frontier,
            work,
            rows,
        })
    }
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.state.base.check()?;
        let worker = self.state.base.incarnation.worker_id as i64;
        if let Rows::Results {
            cursor,
            certificate,
        } = &mut self.rows
        {
            let mut batch = ResultBatch::new(
                self.request.output_schema(),
                self.request.batch_rows,
                &self.state.base.resources,
            )?;
            while batch.len() < self.request.batch_rows {
                let Some(row) = cursor.next_row().map_err(error)? else {
                    break;
                };
                let distance = row.distance.map(i64::try_from).transpose().map_err(error)?;
                batch.push(&[
                    Some(row.id),
                    distance,
                    distance,
                    row.parent,
                    Some(self.partition as i64),
                    Some(worker),
                    Some(std::process::id() as i64),
                    Some(self.adjacency as i64),
                    Some(self.incoming as i64),
                    Some(i64::try_from(self.request.phase).map_err(error)?),
                    Some(i64::try_from(certificate.levels).map_err(error)?),
                    Some(i64::try_from(certificate.reached).map_err(error)?),
                    Some(1),
                ]);
            }
            if batch.len() > 0 {
                return batch.finish().map(Some);
            }
        } else {
            let mut batch = BatchBuilder::new(
                self.request.output_schema(),
                self.request.batch_rows,
                &self.state.base.resources,
            )?;
            while batch.len() < self.request.batch_rows {
                match &mut self.rows {
                    Rows::Statistics {
                        report,
                        owner,
                        item,
                    } => {
                        if *owner == self.request.partitions {
                            break;
                        }
                        let v = report.values;
                        let (kind, target, value) = if *item < 8 {
                            (
                                wire::STATISTIC,
                                *item as i64,
                                [
                                    v.vertices,
                                    v.arcs,
                                    v.source_count,
                                    v.reached,
                                    v.frontier,
                                    v.frontier_edges,
                                    v.remaining_edges,
                                    v.levels,
                                ][*item],
                            )
                        } else {
                            (wire::COMPLETE, 0, v.vertices)
                        };
                        batch.push_values(
                            &[
                                *owner as i64,
                                kind,
                                self.partition as i64,
                                *item as i64,
                                target,
                                0,
                                0,
                                wire::mode_number(v.completed),
                                i64::try_from(value).map_err(error)?,
                                worker,
                                self.adjacency as i64,
                            ],
                            &[],
                        );
                        *item += 1;
                        if *item == 9 {
                            *item = 0;
                            *owner += 1;
                        }
                    }
                    Rows::Updates {
                        cursor,
                        completion,
                        owner,
                        total,
                    } => {
                        if let Some(active) = cursor.as_mut() {
                            if let Some(m) = active.next_values().map_err(error)? {
                                let (kind, target, parent, value) = match m.payload {
                                    BfsPayload::Topology { source, target } => {
                                        (wire::TOPOLOGY, target, source, 0)
                                    }
                                    BfsPayload::Candidate {
                                        target,
                                        parent,
                                        distance,
                                    } => (
                                        wire::CANDIDATE,
                                        target,
                                        parent,
                                        i64::try_from(distance).map_err(error)?,
                                    ),
                                    BfsPayload::Membership { vertex } => {
                                        (wire::MEMBERSHIP, vertex, 0, 0)
                                    }
                                };
                                batch.push_values(
                                    &[
                                        m.recipient as i64,
                                        kind,
                                        self.partition as i64,
                                        i64::try_from(m.sequence).map_err(error)?,
                                        target,
                                        parent,
                                        value,
                                        wire::mode_number(m.mode),
                                        0,
                                        worker,
                                        self.adjacency as i64,
                                    ],
                                    &[],
                                );
                                continue;
                            }
                            let finished =
                                cursor.take().expect("active").finish().map_err(error)?;
                            *total = finished
                                .sequences
                                .iter()
                                .try_fold(0u64, |a, b| a.checked_add(*b))
                                .ok_or_else(|| error("v3 completion count overflow"))?;
                            *completion = Some(finished);
                        }
                        if *owner == self.request.partitions {
                            break;
                        }
                        let c = completion.as_ref().expect("finished");
                        // Only this recipient's sequence and one scalar total
                        // cross the wire: P markers per producer, not P vectors.
                        batch.push_values(
                            &[
                                *owner as i64,
                                wire::COMPLETE,
                                self.partition as i64,
                                i64::try_from(c.sequences[*owner]).map_err(error)?,
                                0,
                                0,
                                0,
                                wire::mode_number(c.mode),
                                i64::try_from(*total).map_err(error)?,
                                worker,
                                self.adjacency as i64,
                            ],
                            &[],
                        );
                        *owner += 1;
                    }
                    Rows::Results { .. } => unreachable!(),
                }
            }
            if batch.len() > 0 {
                return batch.finish().map(Some);
            }
        }
        let (event, details) = match &self.rows {
            Rows::Statistics { report, .. } => {
                let v = report.values;
                (
                    if self.request.verb == Verb::Init {
                        "init"
                    } else {
                        "apply"
                    },
                    serde_json::json!({"output_phase":report.phase.number,"mode":wire::mode_name(v.completed),"vertices":v.vertices,"local_reached":v.reached,"frontier_vertices":v.frontier,"frontier_edges":v.frontier_edges,"remaining_edges":v.remaining_edges,"levels":v.levels,"incoming_adjacency_id":self.incoming,"examined_edges":self.work.examined_edges,"examined_vertices":self.work.examined_vertices,"emitted_messages":self.work.emitted_messages,"received_messages":self.work.received_messages}),
                )
            }
            Rows::Updates { .. } => (
                "decide",
                serde_json::json!({"mode":wire::mode_name(self.mode),"levels":self.levels,"local_reached":self.local_reached,"frontier_vertices":self.frontier,"incoming_adjacency_id":self.incoming}),
            ),
            Rows::Results { certificate, .. } => (
                "result",
                serde_json::json!({"converged":true,"levels":certificate.levels,"reached":certificate.reached,"local_reached":self.local_reached,"incoming_adjacency_id":self.incoming}),
            ),
        };
        self.state.audit(
            event,
            &self.request,
            self.partition,
            self.adjacency,
            details,
        )?;
        self.guard.complete = true;
        Ok(None)
    }
}
