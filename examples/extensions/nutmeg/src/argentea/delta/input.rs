//! Separate statistics and contribution barriers, each drained through EOF.
use super::{
    super::{batches::integers, error, input::next, state::lock},
    request::Request,
    state::{DeltaState, Partition},
    wire::{self, Message, Messages, Statistics},
};
use datafusion::{execution::TaskContext, physical_plan::ExecutionPlan};
use datafusion_common::Result;
use grust_procedures::MemoryAccount;
use sail_argentea_core::{DeltaPartition, Operation, Round};
use std::sync::Arc;

fn admit(account: &mut MemoryAccount, n: usize, width: usize) -> Result<()> {
    account
        .charge(
            n.checked_mul(width)
                .ok_or_else(|| error("v2 input admission overflow"))?,
        )
        .map_err(error)
}
pub async fn initialize(
    state: &Arc<DeltaState>,
    request: &Request,
    operation: Operation,
    p: usize,
    inputs: &[Arc<dyn ExecutionPlan>],
    context: Arc<TaskContext>,
) -> Result<DeltaPartition> {
    let mut admission = state.base.resources.execution.memory_account();
    let mut vertices = Vec::new();
    let mut edges = Vec::new();
    let mut stream = inputs[0].execute(p, context.clone())?;
    while let Some(batch) = next(&mut stream, &state.base).await? {
        let ids = integers(&batch, "id")?;
        let owners = integers(&batch, "owner")?;
        admit(&mut admission, batch.num_rows(), 8)?;
        vertices
            .try_reserve_exact(batch.num_rows())
            .map_err(error)?;
        for i in 0..batch.num_rows() {
            state
                .base
                .resources
                .execution
                .charge_work(1)
                .map_err(error)?;
            if owners.value(i) != p as i64 || operation.owner(ids.value(i)) != p {
                return Err(error("misrouted v2 vertex"));
            }
            vertices.push(ids.value(i));
        }
    }
    let mut stream = inputs[1].execute(p, context)?;
    while let Some(batch) = next(&mut stream, &state.base).await? {
        let sources = integers(&batch, "src")?;
        let targets = integers(&batch, "dst")?;
        let owners = integers(&batch, "owner")?;
        admit(&mut admission, batch.num_rows(), 16)?;
        edges.try_reserve_exact(batch.num_rows()).map_err(error)?;
        for i in 0..batch.num_rows() {
            state
                .base
                .resources
                .execution
                .charge_work(1)
                .map_err(error)?;
            if owners.value(i) != p as i64 || operation.owner(sources.value(i)) != p {
                return Err(error("misrouted v2 edge"));
            }
            edges.push((sources.value(i), targets.value(i)));
        }
    }
    DeltaPartition::build(
        operation,
        p,
        &vertices,
        &edges,
        request.options(),
        state.base.resources.clone(),
    )
    .map_err(error)
}
fn ready(state: &DeltaState, p: usize, phase: u64, stats: bool) -> Result<Option<Partition>> {
    let Some(part) = state.partition(p)? else {
        return Ok(None);
    };
    let native = lock(&part)?;
    if native.next_phase() > phase {
        return Err(error("native v2 state advanced beyond input phase"));
    }
    let waiting = if stats {
        native.collecting_phase()
    } else {
        native.receiving_phase()
    };
    drop(native);
    Ok((waiting == Some(phase)).then_some(part))
}
fn own_origin(
    state: &DeltaState,
    p: usize,
    message: &Message,
    part: Option<&Partition>,
) -> Result<()> {
    if message.producer == p as i64 {
        let part = part
            .ok_or_else(|| error("own v2 producer has no local state in the required barrier"))?;
        if message.worker != state.base.incarnation.worker_id as i64
            || lock(part)?.adjacency_identity() != message.adjacency as u64
        {
            return Err(error("v2 local state/producer placement mismatch"));
        }
    }
    Ok(())
}
pub async fn statistics(
    state: &Arc<DeltaState>,
    request: &Request,
    phase: &Round,
    p: usize,
    input: &Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Partition> {
    let _admission = state
        .base
        .resources
        .execution
        .reserve(request.partitions * (size_of::<Statistics>() + 32) + 1024)
        .map_err(error)?;
    let mut reports = Vec::new();
    reports
        .try_reserve_exact(request.partitions)
        .map_err(error)?;
    reports.resize_with(request.partitions, Statistics::default);
    let mut origins = Vec::new();
    origins
        .try_reserve_exact(request.partitions)
        .map_err(error)?;
    origins.resize(request.partitions, None);
    let expected = request.message_schema(phase.number, true);
    let mut stream = input.execute(p, context)?;
    while let Some(batch) = next(&mut stream, &state.base).await? {
        let rows = Messages::new(&batch, &expected)?;
        for i in 0..batch.num_rows() {
            state
                .base
                .resources
                .execution
                .charge_work(1)
                .map_err(error)?;
            let message = rows.get(i);
            wire::validate(&message, request, p, &mut origins, state)?;
            if message.producer == p as i64 {
                own_origin(
                    state,
                    p,
                    &message,
                    ready(state, p, phase.number, true)?.as_ref(),
                )?;
            }
            reports[message.producer as usize].receive(message)?;
        }
    }
    let part = ready(state, p, phase.number, true)?
        .ok_or_else(|| error("v2 statistics ended before local state became ready"))?;
    {
        let mut native = lock(&part)?;
        for (producer, report) in reports.iter().enumerate() {
            native
                .receive_statistics_values(phase, producer, report.values(request)?)
                .map_err(error)?;
        }
    }
    Ok(part)
}
pub async fn contributions(
    state: &Arc<DeltaState>,
    request: &Request,
    phase: &Round,
    p: usize,
    input: &Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Partition> {
    let mut admission = state.base.resources.execution.memory_account();
    admit(&mut admission, request.partitions, 32)?;
    let mut origins = Vec::new();
    origins
        .try_reserve_exact(request.partitions)
        .map_err(error)?;
    origins.resize(request.partitions, None);
    let mut pending = Vec::new();
    let mut part = None;
    let expected = request.message_schema(phase.number, false);
    let mut stream = input.execute(p, context)?;
    while let Some(batch) = next(&mut stream, &state.base).await? {
        let rows = Messages::new(&batch, &expected)?;
        for i in 0..batch.num_rows() {
            state
                .base
                .resources
                .execution
                .charge_work(1)
                .map_err(error)?;
            let message = rows.get(i);
            wire::validate(&message, request, p, &mut origins, state)?;
            if part.is_none() {
                part = ready(state, p, phase.number, false)?;
            }
            own_origin(state, p, &message, part.as_ref())?;
            if let Some(part) = &part {
                let mut native = lock(part)?;
                for buffered in pending.drain(..) {
                    receive(&mut native, phase, buffered)?;
                }
                receive(&mut native, phase, message)?;
            } else {
                if pending.len() == pending.capacity() {
                    admit(&mut admission, request.batch_rows, size_of::<Message>())?;
                    pending
                        .try_reserve_exact(request.batch_rows)
                        .map_err(error)?;
                }
                pending.push(message);
            }
        }
    }
    let part =
        part.ok_or_else(|| error("v2 contributions ended before local state became ready"))?;
    if !pending.is_empty() {
        return Err(error("undrained v2 early queue"));
    }
    lock(&part)?.finish(phase).map_err(error)?;
    Ok(part)
}
fn receive(native: &mut DeltaPartition, phase: &Round, message: Message) -> Result<()> {
    let mode = wire::mode(message.mode)?;
    match message.kind {
        wire::UPDATE if message.aux == 0 => native.receive_values(
            phase,
            mode,
            message.producer as usize,
            message.sequence as u64,
            message.target,
            message.value,
        ),
        wire::COMPLETE if message.target == 0 && message.aux >= 0 => native.finish_producer_values(
            phase,
            mode,
            message.producer as usize,
            message.sequence as u64,
            message.value,
            message.aux as u64,
        ),
        _ => return Err(error("invalid v2 contribution channel row")),
    }
    .map_err(error)
}
