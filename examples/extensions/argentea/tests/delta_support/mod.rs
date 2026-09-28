#![allow(dead_code)]
use grust_procedures::{ExecutionContext, ExecutionLimits};
use sail_argentea_core::{
    DeltaMode, DeltaOptions, DeltaPartition, Operation, Resources, Result, Round,
};
use sail_native_resource_ffi::MemoryLease;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct LeaseOwner(Arc<AtomicUsize>);
impl Drop for LeaseOwner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
pub fn resources(bytes: usize) -> (Resources, ExecutionContext, Arc<AtomicUsize>) {
    let context = ExecutionContext::new(ExecutionLimits {
        memory_bytes: bytes,
        work_units: usize::MAX,
        batch_rows: 16,
        deadline: None,
    })
    .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = MemoryLease::new(Arc::new(LeaseOwner(drops.clone())), bytes as u64);
    (
        Resources::new(context.clone(), lease).unwrap(),
        context,
        drops,
    )
}
pub fn operation(p: usize, n: u64) -> Operation {
    Operation {
        package: "test-package".into(),
        session: "session-1".into(),
        operation: "operation-1".into(),
        snapshot: "snapshot-1".into(),
        generation: 1,
        partitions: p,
        vertices: n,
    }
}
pub fn options() -> DeltaOptions {
    DeltaOptions {
        damping: 0.85,
        tolerance: 1e-11,
        max_pushes: 512,
    }
}
pub fn partitions(
    op: &Operation,
    ids: &[i64],
    edges: &[(i64, i64)],
    options: DeltaOptions,
    resources: &Resources,
) -> Vec<DeltaPartition> {
    (0..op.partitions)
        .map(|p| {
            DeltaPartition::build(
                op.clone(),
                p,
                &ids.iter()
                    .copied()
                    .filter(|id| op.owner(*id) == p)
                    .collect::<Vec<_>>(),
                &edges
                    .iter()
                    .copied()
                    .filter(|(id, _)| op.owner(*id) == p)
                    .collect::<Vec<_>>(),
                options,
                resources.clone(),
            )
            .unwrap()
        })
        .collect()
}
pub fn phase(op: &Operation, parts: &[DeltaPartition]) -> Round {
    Round {
        operation: op.clone(),
        number: parts[0].next_phase(),
    }
}
pub fn stats(parts: &mut [DeltaPartition]) -> Result<()> {
    let reports = parts
        .iter()
        .map(DeltaPartition::statistics)
        .collect::<Result<Vec<_>>>()?;
    for (receiver, part) in parts.iter_mut().enumerate() {
        // Different arrival order at every owner; global reduction is still by
        // producer index. This harness models no network or Sail placement.
        for i in 0..reports.len() {
            part.receive_statistics(&reports[(receiver + i) % reports.len()])?;
        }
    }
    Ok(())
}
#[derive(Default, Debug)]
pub struct Traffic {
    pub messages: usize,
    pub negative_updates: usize,
    pub negative_dangling: usize,
}
pub fn exchange(op: &Operation, parts: &mut [DeltaPartition]) -> Result<(DeltaMode, Traffic)> {
    let phase = phase(op, parts);
    let mut cursors = parts
        .iter_mut()
        .map(|p| p.start_emission(&phase))
        .collect::<Result<Vec<_>>>()?;
    let mode = cursors[0].mode();
    let mut traffic = Traffic::default();
    assert!(cursors.iter().all(|c| c.mode() == mode));
    // Round-robin producers interleave arrivals while preserving each stream.
    loop {
        let mut any = false;
        for c in &mut cursors {
            if let Some(m) = c.next_update()? {
                any = true;
                traffic.messages += 1;
                traffic.negative_updates += usize::from(m.value < 0.0);
                parts[op.owner(m.target)].receive(&m)?;
            }
        }
        if !any {
            break;
        }
    }
    let completions = cursors
        .into_iter()
        .map(|c| c.finish())
        .collect::<Result<Vec<_>>>()?;
    for completion in &completions {
        traffic.negative_dangling += usize::from(completion.dangling < 0.0);
        for part in parts.iter_mut() {
            part.finish_producer(completion)?;
        }
    }
    for part in parts {
        part.finish(&phase)?;
    }
    Ok((mode, traffic))
}
pub fn transition(ids: &[i64], edges: &[(i64, i64)], x: &[f64], d: f64) -> Vec<f64> {
    // Deliberately dense matrix with uniform dangling columns. Independent of
    // CSR construction, sparse cursors, owner routing and activation code.
    let n = ids.len();
    let mut matrix = vec![vec![0.0; n]; n];
    for (j, id) in ids.iter().enumerate() {
        let outgoing = edges.iter().filter(|(s, _)| s == id).count();
        if outgoing == 0 {
            for row in &mut matrix {
                row[j] = 1.0 / n as f64;
            }
        }
        for (_, target) in edges.iter().filter(|(s, _)| s == id) {
            matrix[ids.iter().position(|id| id == target).unwrap()][j] += 1.0 / outgoing as f64;
        }
    }
    matrix
        .iter()
        .map(|row| (1.0 - d) / n as f64 + d * row.iter().zip(x).map(|(p, x)| p * x).sum::<f64>())
        .collect()
}
pub fn collected(ids: &[i64], parts: &[DeltaPartition]) -> Vec<f64> {
    let mut result = vec![f64::NAN; ids.len()];
    for part in parts {
        for (id, x, _) in part.state_rows() {
            result[ids.iter().position(|v| *v == id).unwrap()] = x;
        }
    }
    result
}
