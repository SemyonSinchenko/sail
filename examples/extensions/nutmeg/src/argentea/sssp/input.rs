//! Separate statistics and contribution barriers, each drained through EOF.
use super::{
    super::{batches::integers, error, input::next, state::lock},
    request::Request,
    state::{Partition, SsspState},
    wire::{self, Message, Messages, Statistics},
};
use datafusion::{execution::TaskContext, physical_plan::ExecutionPlan};
use datafusion_common::Result;
use grust_procedures::MemoryAccount;
use sail_argentea_core::{Operation, Round, SsspPartition};
use std::sync::Arc;

fn admit(account: &mut MemoryAccount, n: usize, width: usize) -> Result<()> {
    account
        .charge(
            n.checked_mul(width)
                .ok_or_else(|| error("v5 input admission overflow"))?,
        )
        .map_err(error)
}
pub async fn initialize(
    state: &Arc<SsspState>,
    request: &Request,
    operation: Operation,
    p: usize,
    inputs: &[Arc<dyn ExecutionPlan>],
    context: Arc<TaskContext>,
) -> Result<SsspPartition> {
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
                return Err(error("misrouted v5 vertex"));
            }
            vertices.push(ids.value(i));
        }
    }
    let mut stream = inputs[1].execute(p, context)?;
    while let Some(batch) = next(&mut stream, &state.base).await? {
        let sources = integers(&batch, "src")?;
        let targets = integers(&batch, "dst")?;
        let owners = integers(&batch, "owner")?;
        let weights = wire::floats(&batch, "weight")?;
        admit(&mut admission, batch.num_rows(), 24)?;
        edges.try_reserve_exact(batch.num_rows()).map_err(error)?;
        for i in 0..batch.num_rows() {
            state
                .base
                .resources
                .execution
                .charge_work(1)
                .map_err(error)?;
            if owners.value(i) != p as i64 || operation.owner(sources.value(i)) != p {
                return Err(error("misrouted v5 edge"));
            }
            edges.push((sources.value(i), targets.value(i), weights.value(i)));
        }
    }
    let prepared = SsspPartition::prepare(
        operation,
        p,
        state.base.incarnation.worker_id,
        &vertices,
        &edges,
        request.options(),
        state.base.resources.clone(),
    )
    .map_err(error)?;
    // CSR owns its storage; raw input and its charge need not overlap labels.
    drop(vertices);
    drop(edges);
    drop(admission);
    prepared.finish().map_err(error)
}
fn ready(state: &SsspState, p: usize, phase: u64, stats: bool) -> Result<Option<Partition>> {
    let Some(part) = state.partition(p)? else {
        return Ok(None);
    };
    let native = lock(&part)?;
    if native.next_phase() > phase {
        return Err(error("native v5 state advanced beyond input phase"));
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
    state: &SsspState,
    p: usize,
    message: &Message,
    part: Option<&Partition>,
) -> Result<()> {
    if message.producer == p as i64 {
        let part = part
            .ok_or_else(|| error("own v5 producer has no local state in the required barrier"))?;
        if message.worker != state.base.incarnation.worker_id as i64
            || lock(part)?.origin().adjacency_id != message.adjacency as u64
        {
            return Err(error("v5 local state/producer placement mismatch"));
        }
    }
    Ok(())
}
pub async fn statistics(
    state: &Arc<SsspState>,
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
        .ok_or_else(|| error("v5 statistics ended before local state became ready"))?;
    {
        let mut native = lock(&part)?;
        for (producer, report) in reports.iter().enumerate() {
            native
                .receive_statistics_values(
                    phase,
                    producer,
                    report.values(
                        request,
                        origins[producer].ok_or_else(|| error("missing v5 statistics origin"))?,
                    )?,
                )
                .map_err(error)?;
        }
        native.finish_statistics(phase).map_err(error)?;
    }
    Ok(part)
}
pub async fn contributions(
    state: &Arc<SsspState>,
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
        part.ok_or_else(|| error("v5 contributions ended before local state became ready"))?;
    if !pending.is_empty() {
        return Err(error("undrained v5 early queue"));
    }
    lock(&part)?.finish(phase).map_err(error)?;
    Ok(part)
}
fn receive(native: &mut SsspPartition, phase: &Round, m: Message) -> Result<()> {
    use sail_argentea_core::{
        SsspCompletionValues, SsspLabel, SsspMessageValues, SsspOrigin, SsspPayload,
    };
    let mode = wire::mode(m.mode)?;
    let bucket = wire::bucket(m.bucket)?;
    let origin = SsspOrigin {
        worker_id: m.worker as u64,
        adjacency_id: m.adjacency as u64,
    };
    if m.kind == wire::COMPLETE {
        if m.target != 0 || m.source != 0 || m.hops != 0 || m.distance != 0.0 || m.aux < 0 {
            return Err(error("invalid v5 completion fields"));
        }
        return native
            .finish_producer_values(
                phase,
                SsspCompletionValues {
                    origin,
                    producer: m.producer as usize,
                    mode,
                    bucket,
                    sequence: m.sequence as u64,
                    total_messages: m.aux as u64,
                },
            )
            .map_err(error);
    }
    let payload = match m.kind {
        wire::TOPOLOGY if m.hops == 0 && m.aux == 0 && m.distance == 0.0 => SsspPayload::Topology {
            source: m.source,
            target: m.target,
        },
        wire::CANDIDATE if m.hops > 0 && m.aux == 0 => SsspPayload::Candidate {
            target: m.target,
            label: SsspLabel::from_parts(m.distance, m.hops as u64, m.source).map_err(error)?,
        },
        _ => return Err(error("invalid v5 update fields")),
    };
    native
        .receive_values(
            phase,
            SsspMessageValues {
                origin,
                producer: m.producer as usize,
                recipient: m.owner as usize,
                sequence: m.sequence as u64,
                mode,
                bucket,
                payload,
            },
        )
        .map_err(error)
}
