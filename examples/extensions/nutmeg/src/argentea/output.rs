//! Pull-based outputs retain owned CSR/rank snapshots, never a state mutex.
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use sail_argentea_core::{Emission, EmissionCursor, RankCursor, Round};

use super::{
    batches::BatchBuilder,
    error, input,
    request::{Request, Verb},
    state::{ExecutionGuard, WorkerState, lock},
};

enum Rows {
    Messages {
        cursor: Option<EmissionCursor>,
        emission: Option<Emission>,
        marker: usize,
    },
    Ranks(RankCursor),
}

pub struct Output {
    guard: ExecutionGuard,
    request: Request,
    partition: usize,
    adjacency: u64,
    vertices: usize,
    rows: Rows,
}

impl Output {
    pub async fn prepare(
        state: Arc<WorkerState>,
        request: Request,
        partition: usize,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        context: Arc<TaskContext>,
    ) -> Result<Self> {
        let operation = state.claim(&request, partition)?;
        let guard = ExecutionGuard {
            state: state.clone(),
            complete: false,
        };
        let current = Round {
            operation: operation.clone(),
            number: request.round,
        };
        let part = if request.verb == Verb::Init {
            let mut part =
                input::initialize(&state, &request, operation, partition, &inputs, context).await?;
            part.begin(&current).map_err(error)?;
            state.publish(part)?
        } else {
            let previous = Round {
                operation,
                number: if request.verb == Verb::Result {
                    request.round
                } else {
                    request.round - 1
                },
            };
            input::consume(&state, &request, &previous, partition, &inputs[0], context).await?
        };
        let (adjacency, vertices, rows) = {
            let mut native = lock(&part)?;
            state.check()?;
            let rows = if request.verb == Verb::Result {
                Rows::Ranks(native.rank_cursor().map_err(error)?)
            } else {
                if request.verb == Verb::Round {
                    native.begin(&current).map_err(error)?;
                }
                Rows::Messages {
                    cursor: Some(native.start_emission(&current).map_err(error)?),
                    emission: None,
                    marker: 0,
                }
            };
            (native.adjacency_identity(), native.vertex_count(), rows)
        };
        if adjacency > i64::MAX as u64 {
            return Err(error("adjacency identity exceeds wire range"));
        }
        Ok(Self {
            guard,
            request,
            partition,
            adjacency,
            vertices,
            rows,
        })
    }

    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.guard.state.check()?;
        let mut batch = BatchBuilder::new(
            self.request.output_schema(),
            self.request.batch_rows,
            &self.guard.state.resources,
        )?;
        let worker = self.guard.state.incarnation.worker_id as i64;
        match &mut self.rows {
            Rows::Messages {
                cursor,
                emission,
                marker,
            } => {
                while batch.len() < self.request.batch_rows {
                    if let Some(active) = cursor.as_mut() {
                        if let Some(update) = active.next_update().map_err(error)? {
                            batch.push(
                                &[
                                    update.round.operation.owner(update.target) as i64,
                                    0,
                                    self.partition as i64,
                                    i64::try_from(update.sequence).map_err(error)?,
                                    update.target,
                                    worker,
                                    self.adjacency as i64,
                                ],
                                update.value,
                            );
                            continue;
                        }
                        *emission = Some(
                            cursor
                                .take()
                                .expect("active cursor")
                                .finish()
                                .map_err(error)?,
                        );
                        self.guard.state.audit(
                            "emit",
                            &self.request,
                            self.partition,
                            self.adjacency,
                        )?;
                    }
                    if *marker == self.request.partitions {
                        break;
                    }
                    let finished = emission.as_ref().expect("drained emission");
                    batch.push(
                        &[
                            *marker as i64,
                            1,
                            self.partition as i64,
                            i64::try_from(finished.sequences[*marker]).map_err(error)?,
                            self.vertices as i64,
                            worker,
                            self.adjacency as i64,
                        ],
                        finished.dangling_mass,
                    );
                    *marker += 1;
                }
            }
            Rows::Ranks(cursor) => {
                while batch.len() < self.request.batch_rows {
                    let Some((id, rank)) = cursor.next_rank().map_err(error)? else {
                        break;
                    };
                    batch.push(
                        &[
                            id,
                            self.partition as i64,
                            worker,
                            std::process::id() as i64,
                            self.adjacency as i64,
                            self.request.round as i64,
                        ],
                        rank,
                    );
                }
            }
        }
        if batch.len() == 0 {
            if self.request.verb == Verb::Result {
                self.guard
                    .state
                    .audit("result", &self.request, self.partition, self.adjacency)?;
            }
            self.guard.complete = true;
            Ok(None)
        } else {
            batch.finish().map(Some)
        }
    }
}
