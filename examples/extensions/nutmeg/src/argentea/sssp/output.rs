//! Retained snapshots and weighted Arrow buffers; no lock spans output backpressure.
use super::{
    super::{
        batches::BatchBuilder,
        error,
        state::{ExecutionGuard, lock},
    },
    input,
    request::{Request, Verb},
    result_batch::ResultBatch,
    state::SsspState,
    wire,
};
use arrow::record_batch::RecordBatch;
use datafusion::{execution::TaskContext, physical_plan::ExecutionPlan};
use datafusion_common::Result;
use sail_argentea_core::{
    Round, SsspCompletion, SsspConvergence, SsspEmissionCursor, SsspMode, SsspPayload,
    SsspRowCursor, SsspStatistics, SsspWork,
};
use std::sync::Arc;
enum Rows {
    Statistics {
        report: SsspStatistics,
        owner: usize,
        item: usize,
    },
    Updates {
        cursor: Option<SsspEmissionCursor>,
        completion: Option<SsspCompletion>,
        owner: usize,
        total: u64,
    },
    Results {
        cursor: SsspRowCursor,
        certificate: SsspConvergence,
    },
}
pub struct Output {
    guard: ExecutionGuard,
    state: Arc<SsspState>,
    request: Request,
    partition: usize,
    adjacency: u64,
    mode: SsspMode,
    rounds: u64,
    work: SsspWork,
    rows: Rows,
}
impl Output {
    pub async fn prepare(
        state: Arc<SsspState>,
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
                state.audit("failure",&request,partition,adjacency,serde_json::json!({"code":"sssp_round_cap","outcome":"nonconverged","rounds":failure.rounds,"max_rounds":failure.max_rounds,"active":failure.active,"reached":failure.reached}))?;
                return Err(error(failure));
            }
        }
        let (adjacency, mode, rounds, work, rows) = {
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
                    let certificate = native.seal(&phase).map_err(error)?.ok_or_else(|| {
                        error("v5 plan exhausted without a complete SSSP certificate")
                    })?;
                    Rows::Results {
                        cursor: native.row_cursor().map_err(error)?,
                        certificate,
                    }
                }
            };
            (
                native.origin().adjacency_id,
                mode,
                native.rounds(),
                native.last_work(),
                rows,
            )
        };
        if adjacency > i64::MAX as u64 {
            return Err(error("v5 adjacency exceeds wire range"));
        }
        Ok(Self {
            guard,
            state,
            request,
            partition,
            adjacency,
            mode,
            rounds,
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
                batch.push(
                    &[
                        Some(row.id),
                        row.label
                            .map(|l| i64::try_from(l.hops()))
                            .transpose()
                            .map_err(error)?,
                        row.label.map(|l| l.parent()),
                        Some(self.partition as i64),
                        Some(worker),
                        Some(std::process::id() as i64),
                        Some(self.adjacency as i64),
                        Some(i64::try_from(self.request.phase).map_err(error)?),
                        Some(i64::try_from(certificate.rounds).map_err(error)?),
                        Some(i64::try_from(certificate.reached).map_err(error)?),
                        Some(1),
                    ],
                    row.label.map(|l| l.distance()),
                );
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
                                    v.reachable_edges,
                                    v.active,
                                    v.bucket_edges,
                                    v.rounds,
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
                            &[0.0, v.bucket.unwrap_or(-1.0)],
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
                                let (kind, target, source, hops, distance) = match m.payload {
                                    SsspPayload::Topology { source, target } => {
                                        (wire::TOPOLOGY, target, source, 0, 0.0)
                                    }
                                    SsspPayload::Candidate { target, label } => (
                                        wire::CANDIDATE,
                                        target,
                                        label.parent(),
                                        i64::try_from(label.hops()).map_err(error)?,
                                        label.distance(),
                                    ),
                                };
                                batch.push_values(
                                    &[
                                        m.recipient as i64,
                                        kind,
                                        self.partition as i64,
                                        i64::try_from(m.sequence).map_err(error)?,
                                        target,
                                        source,
                                        hops,
                                        wire::mode_number(m.mode),
                                        0,
                                        worker,
                                        self.adjacency as i64,
                                    ],
                                    &[distance, m.bucket.unwrap_or(-1.0)],
                                );
                                continue;
                            }
                            let finished =
                                cursor.take().expect("active").finish().map_err(error)?;
                            *total = finished
                                .sequences
                                .iter()
                                .try_fold(0u64, |a, b| a.checked_add(*b))
                                .ok_or_else(|| error("v5 completion count overflow"))?;
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
                            &[0.0, c.bucket.unwrap_or(-1.0)],
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
                    serde_json::json!({"output_phase":report.phase.number,"mode":wire::mode_name(v.completed),"vertices":v.vertices,"arcs":v.arcs,"source_count":v.source_count,"reachable_edges":v.reachable_edges,"reached":v.reached,"active":v.active,"bucket":v.bucket,"bucket_edges":v.bucket_edges,"rounds":v.rounds,"examined_edges":self.work.examined_edges,"examined_vertices":self.work.examined_vertices,"emitted_messages":self.work.emitted_messages,"received_messages":self.work.received_messages}),
                )
            }
            Rows::Updates { completion, .. } => (
                "decide",
                serde_json::json!({"mode":wire::mode_name(self.mode),"rounds":self.rounds,"bucket":completion.as_ref().expect("completed emission").bucket}),
            ),
            Rows::Results { certificate, .. } => (
                "result",
                serde_json::json!({"converged":true,"rounds":certificate.rounds,"reached":certificate.reached}),
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
