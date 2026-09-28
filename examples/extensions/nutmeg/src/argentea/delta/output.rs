//! Each stage freezes its output owner before releasing the partition mutex.
use super::{
    super::{
        batches::BatchBuilder,
        error,
        state::{ExecutionGuard, lock},
    },
    input,
    request::{Request, Verb},
    state::DeltaState,
    wire,
};
use arrow::record_batch::RecordBatch;
use datafusion::{execution::TaskContext, physical_plan::ExecutionPlan};
use datafusion_common::Result;
use sail_argentea_core::{
    Convergence, DeltaCompletion, DeltaEmissionCursor, DeltaRankCursor, DeltaStatistics, Round,
};
use std::sync::Arc;
enum Rows {
    Statistics {
        report: DeltaStatistics,
        owner: usize,
        item: usize,
    },
    Contributions {
        cursor: Option<DeltaEmissionCursor>,
        completion: Option<DeltaCompletion>,
        owner: usize,
    },
    Ranks {
        cursor: DeltaRankCursor,
        certificate: Convergence,
    },
}
pub struct Output {
    guard: ExecutionGuard,
    state: Arc<DeltaState>,
    request: Request,
    partition: usize,
    adjacency: u64,
    rows: Rows,
}
impl Output {
    pub async fn prepare(
        state: Arc<DeltaState>,
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
            Verb::Init => {
                let native =
                    input::initialize(&state, &request, operation, partition, &inputs, context)
                        .await?;
                state.publish(native)?
            }
            Verb::Decide | Verb::Result => {
                input::statistics(&state, &request, &phase, partition, &inputs[0], context).await?
            }
            Verb::Apply => {
                input::contributions(&state, &request, &phase, partition, &inputs[0], context)
                    .await?
            }
        };
        let (adjacency, rows) = {
            let mut native = lock(&part)?;
            state.base.check()?;
            let rows = match request.verb {
                Verb::Init | Verb::Apply => Rows::Statistics {
                    report: native.statistics().map_err(error)?,
                    owner: 0,
                    item: 0,
                },
                Verb::Decide => Rows::Contributions {
                    cursor: Some(native.start_emission(&phase).map_err(error)?),
                    completion: None,
                    owner: 0,
                },
                Verb::Result => {
                    let certificate = native
                        .seal(&phase)
                        .map_err(error)?
                        .ok_or_else(|| error("v2 plan exhausted without a passed certificate"))?;
                    Rows::Ranks {
                        cursor: native.rank_cursor().map_err(error)?,
                        certificate,
                    }
                }
            };
            (native.adjacency_identity(), rows)
        };
        if adjacency > i64::MAX as u64 {
            return Err(error("v2 adjacency identity exceeds wire range"));
        }
        if request.verb == Verb::Init {
            state.audit(
                "init",
                &request,
                partition,
                adjacency,
                serde_json::json!({}),
            )?;
        }
        Ok(Self {
            guard,
            state,
            request,
            partition,
            adjacency,
            rows,
        })
    }
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.state.base.check()?;
        let mut batch = BatchBuilder::new(
            self.request.output_schema(),
            self.request.batch_rows,
            &self.state.base.resources,
        )?;
        let worker = self.state.base.incarnation.worker_id as i64;
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
                    let mode = wire::mode_number(report.completed);
                    let (kind, target, value, aux) = if *item < 3 {
                        (
                            wire::FLOAT_STAT,
                            *item as i64,
                            [report.mass, report.residual_l1, report.min_score][*item],
                            0,
                        )
                    } else if *item < 9 {
                        (
                            wire::INT_STAT,
                            *item as i64,
                            0.0,
                            i64::try_from(
                                [
                                    report.vertices,
                                    report.pushes,
                                    report.certificate_passes,
                                    report.active_vertices,
                                    report.active_edges,
                                    report.reactivated_vertices,
                                ][*item - 3],
                            )
                            .map_err(error)?,
                        )
                    } else {
                        (
                            wire::COMPLETE,
                            0,
                            0.0,
                            i64::try_from(report.vertices).map_err(error)?,
                        )
                    };
                    batch.push(
                        &[
                            *owner as i64,
                            kind,
                            self.partition as i64,
                            *item as i64,
                            target,
                            mode,
                            aux,
                            worker,
                            self.adjacency as i64,
                        ],
                        value,
                    );
                    *item += 1;
                    if *item == 10 {
                        *owner += 1;
                        *item = 0;
                    }
                }
                Rows::Contributions {
                    cursor,
                    completion,
                    owner,
                } => {
                    if let Some(active) = cursor.as_mut() {
                        if let Some(update) = active.next_update().map_err(error)? {
                            batch.push(
                                &[
                                    update.phase.operation.owner(update.target) as i64,
                                    wire::UPDATE,
                                    self.partition as i64,
                                    i64::try_from(update.sequence).map_err(error)?,
                                    update.target,
                                    wire::mode_number(update.mode),
                                    0,
                                    worker,
                                    self.adjacency as i64,
                                ],
                                update.value,
                            );
                            continue;
                        }
                        *completion = Some(
                            cursor
                                .take()
                                .expect("active cursor")
                                .finish()
                                .map_err(error)?,
                        );
                    }
                    if *owner == self.request.partitions {
                        break;
                    }
                    let completed = completion.as_ref().expect("finished cursor");
                    batch.push(
                        &[
                            *owner as i64,
                            wire::COMPLETE,
                            self.partition as i64,
                            i64::try_from(completed.sequences[*owner]).map_err(error)?,
                            0,
                            wire::mode_number(completed.mode),
                            i64::try_from(completed.vertices).map_err(error)?,
                            worker,
                            self.adjacency as i64,
                        ],
                        completed.dangling,
                    );
                    *owner += 1;
                }
                Rows::Ranks {
                    cursor,
                    certificate,
                } => {
                    let Some((id, rank)) = cursor.next_rank().map_err(error)? else {
                        break;
                    };
                    batch.push_values(
                        &[
                            id,
                            self.partition as i64,
                            worker,
                            std::process::id() as i64,
                            self.adjacency as i64,
                            i64::try_from(self.request.phase).map_err(error)?,
                            i64::try_from(certificate.pushes).map_err(error)?,
                            i64::try_from(certificate.certificate_passes).map_err(error)?,
                            1,
                        ],
                        &[
                            rank,
                            certificate.residual_l1,
                            certificate.stationary_error_bound,
                        ],
                    );
                }
            }
        }
        if batch.len() > 0 {
            return batch.finish().map(Some);
        }
        let (event, details) = match &self.rows {
            Rows::Statistics { report, .. } => (
                "statistics",
                serde_json::json!({"output_phase":report.phase.number,"mode":wire::mode_number(report.completed),"vertices":report.vertices,"mass":report.mass,"residual_l1":report.residual_l1,"minimum_score":if report.vertices==0{None}else{Some(report.min_score)},"pushes":report.pushes,"certificate_passes":report.certificate_passes,"active_vertices":report.active_vertices,"active_edges":report.active_edges,"reactivated_vertices":report.reactivated_vertices}),
            ),
            Rows::Contributions { completion, .. } => (
                "emit",
                serde_json::json!({"mode":wire::mode_number(completion.as_ref().expect("finished").mode)}),
            ),
            Rows::Ranks { certificate, .. } => (
                "result",
                serde_json::json!({"converged":true,"pushes":certificate.pushes,"certificate_passes":certificate.certificate_passes,"residual_l1":certificate.residual_l1,"stationary_error_bound":certificate.stationary_error_bound}),
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
