//! The native adapter executes exactly the prepared host partition requested.
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use futures::TryStreamExt;
use grust_procedures::MemoryAccount;
use sail_argentea_core::{Operation, PageRankPartition, Round};

use super::{
    batches::{Message, Messages, integers},
    error,
    request::Request,
    state::{Partition, WorkerState, lock},
};

pub async fn next(
    stream: &mut SendableRecordBatchStream,
    state: &WorkerState,
) -> Result<Option<RecordBatch>> {
    state.check()?;
    tokio::select! {
        batch = stream.try_next() => batch,
        _ = state.resources.execution.cancelled() => Err(error("operation cancelled while reading input")),
    }
}

pub async fn initialize(
    state: &Arc<WorkerState>,
    request: &Request,
    operation: Operation,
    partition: usize,
    inputs: &[Arc<dyn ExecutionPlan>],
    context: Arc<TaskContext>,
) -> Result<PageRankPartition> {
    let mut admission = state.resources.execution.memory_account();
    let mut vertices = Vec::new();
    let mut edges = Vec::new();
    let mut nodes_stream = inputs[0].execute(partition, context.clone())?;
    while let Some(batch) = next(&mut nodes_stream, state).await? {
        let ids = integers(&batch, "id")?;
        let owners = integers(&batch, "owner")?;
        admit_rows(&mut admission, batch.num_rows(), 8)?;
        vertices
            .try_reserve_exact(batch.num_rows())
            .map_err(error)?;
        for row in 0..batch.num_rows() {
            state.resources.execution.charge_work(1).map_err(error)?;
            if owners.value(row) != partition as i64 || operation.owner(ids.value(row)) != partition
            {
                return Err(error("vertex arrived at a different owner partition"));
            }
            vertices.push(ids.value(row));
        }
    }
    let mut edges_stream = inputs[1].execute(partition, context)?;
    while let Some(batch) = next(&mut edges_stream, state).await? {
        let sources = integers(&batch, "src")?;
        let targets = integers(&batch, "dst")?;
        let owners = integers(&batch, "owner")?;
        admit_rows(&mut admission, batch.num_rows(), 16)?;
        edges.try_reserve_exact(batch.num_rows()).map_err(error)?;
        for row in 0..batch.num_rows() {
            state.resources.execution.charge_work(1).map_err(error)?;
            if owners.value(row) != partition as i64
                || operation.owner(sources.value(row)) != partition
            {
                return Err(error("edge arrived at a different source owner"));
            }
            edges.push((sources.value(row), targets.value(row)));
        }
    }
    // Staging admission remains live during the additional CSR build admission.
    let result = PageRankPartition::build(
        operation,
        partition,
        &vertices,
        &edges,
        state.resources.clone(),
    )
    .map_err(error)?;
    state.audit("init", request, partition, result.adjacency_identity())?;
    Ok(result)
}

fn admit_rows(account: &mut MemoryAccount, rows: usize, bytes: usize) -> Result<()> {
    let bytes = rows
        .checked_mul(bytes)
        .and_then(|v| v.checked_add(64))
        .ok_or_else(|| error("input admission overflow"))?;
    account.charge(bytes).map_err(error)
}

/// An upstream owner can still be initializing while other producers have sent
/// updates. Buffer those early rows under the same native quota. The owner's
/// own producer marker proves its state must already exist; a missing/misplaced
/// state fails instead of waiting indefinitely for an impossible placement.
pub async fn consume(
    state: &Arc<WorkerState>,
    request: &Request,
    round: &Round,
    partition: usize,
    input: &Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Partition> {
    let mut admission = state.resources.execution.memory_account();
    admit_rows(&mut admission, request.partitions, 32)?;
    let mut origins = Vec::new();
    origins
        .try_reserve_exact(request.partitions)
        .map_err(error)?;
    origins.resize(request.partitions, None);
    let mut pending = Vec::new();
    let mut observed_vertices = 0u64;
    let mut ready = None;
    let expected = request.message_schema(round.number);
    let mut stream = input.execute(partition, context)?;
    while let Some(batch) = next(&mut stream, state).await? {
        let rows = Messages::new(&batch, &expected)?;
        for row in 0..batch.num_rows() {
            let message = rows.get(row);
            validate_message(&message, request, partition, &mut origins)?;
            if message.kind == 1 {
                observed_vertices = observed_vertices
                    .checked_add(message.target as u64)
                    .ok_or_else(|| error("global vertex count overflow"))?;
            }
            if ready.is_none() {
                ready = ready_partition(state, partition, round.number)?;
            }
            if message.producer == partition as i64 {
                if message.producer_worker != state.incarnation.worker_id as i64 {
                    return Err(error(
                        "state placement mismatch: own producer ran on another worker",
                    ));
                }
                let Some(part) = &ready else {
                    return Err(error(
                        "state placement mismatch: local producer has no ready native partition",
                    ));
                };
                if lock(part)?.adjacency_identity() != message.adjacency_id as u64 {
                    return Err(error(
                        "state placement mismatch: adjacency identity changed",
                    ));
                }
            }
            if let Some(part) = &ready {
                let mut part = lock(part)?;
                for previous in pending.drain(..) {
                    receive(&mut part, round, previous)?;
                }
                receive(&mut part, round, message)?;
            } else {
                if pending.len() == pending.capacity() {
                    // Admit a bounded chunk before growing. Reserving one row
                    // repeatedly would copy the entire early queue per row.
                    admit_rows(&mut admission, request.batch_rows, size_of::<Message>())?;
                    pending
                        .try_reserve_exact(request.batch_rows)
                        .map_err(error)?;
                }
                pending.push(message);
            }
        }
    }
    let part = ready
        .ok_or_else(|| error("input ended without the local partition's completion marker"))?;
    if !pending.is_empty() {
        return Err(error("undrained early updates"));
    }
    if observed_vertices != request.vertices {
        return Err(error("incomplete or inconsistent global vertex count"));
    }
    {
        let mut native = lock(&part)?;
        native.finish(round, request.damping).map_err(error)?;
        let mut receipt = request.clone();
        receipt.round = round.number;
        state.audit("consume", &receipt, partition, native.adjacency_identity())?;
    }
    Ok(part)
}

fn ready_partition(state: &WorkerState, partition: usize, round: u64) -> Result<Option<Partition>> {
    let Some(part) = state.partition(partition)? else {
        return Ok(None);
    };
    let (next, receiving) = {
        let part = lock(&part)?;
        (part.next_round(), part.receiving_round())
    };
    if next > round {
        return Err(error("partition advanced beyond the incoming round"));
    }
    Ok((receiving == Some(round)).then_some(part))
}

fn validate_message(
    message: &Message,
    request: &Request,
    partition: usize,
    origins: &mut [Option<(i64, i64)>],
) -> Result<()> {
    if message.owner != partition as i64
        || message.producer < 0
        || message.producer >= request.partitions as i64
        || message.sequence < 0
        || message.producer_worker < 0
        || message.adjacency_id <= 0
        || !message.value.is_finite()
        || message.value < 0.0
        || ![0, 1].contains(&message.kind)
        || (message.kind == 1 && message.target < 0)
    {
        return Err(error("invalid message coordinates, value or kind"));
    }
    let origin = (message.producer_worker, message.adjacency_id);
    let previous = &mut origins[message.producer as usize];
    if previous.is_some_and(|p| p != origin) {
        return Err(error("producer identity changed midstream"));
    }
    *previous = Some(origin);
    Ok(())
}

fn receive(part: &mut PageRankPartition, round: &Round, message: Message) -> Result<()> {
    if message.kind == 0 {
        part.receive_values(
            round,
            message.producer as usize,
            message.sequence as u64,
            message.target,
            message.value,
        )
    } else {
        part.finish_producer(
            round,
            message.producer as usize,
            message.sequence as u64,
            message.value,
        )
    }
    .map_err(error)
}
