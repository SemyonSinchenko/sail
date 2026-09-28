use grust_procedures::{ExecutionContext, ExecutionLimits};
use sail_argentea_core::{Operation, PageRankPartition, Resources, Round};
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

fn resources(bytes: usize) -> (Resources, ExecutionContext, Arc<AtomicUsize>) {
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
fn operation(partitions: usize, vertices: u64) -> Operation {
    Operation {
        package: "argentea-test-package".into(),
        session: "session-a".into(),
        operation: "operation-a".into(),
        snapshot: "snapshot-1".into(),
        generation: 1,
        partitions,
        vertices,
    }
}

fn partitions(
    op: &Operation,
    vertices: &[i64],
    edges: &[(i64, i64)],
    resources: &Resources,
) -> Vec<PageRankPartition> {
    (0..op.partitions)
        .map(|p| {
            PageRankPartition::build(
                op.clone(),
                p,
                &vertices
                    .iter()
                    .copied()
                    .filter(|v| op.owner(*v) == p)
                    .collect::<Vec<_>>(),
                &edges
                    .iter()
                    .copied()
                    .filter(|(v, _)| op.owner(*v) == p)
                    .collect::<Vec<_>>(),
                resources.clone(),
            )
            .unwrap()
        })
        .collect()
}

#[test]
fn twenty_rounds_retain_adjacency_and_match_an_independent_dense_reference() {
    // Negative/sparse IDs, duplicate edges, self-loop, isolate, dangling target,
    // disconnected cycle and more partitions than some fixtures have vertices.
    for count in [1, 2, 3, 11] {
        let ids = [-9, -2, 0, 3, 11, 28, 100];
        let edges = [
            (-9, -2),
            (-9, -2),
            (-2, 0),
            (0, 0),
            (3, 11),
            (11, 3),
            (28, 0),
        ];
        let op = operation(count, ids.len() as u64);
        let (resources, usage, dropped) = resources(4 * 1024 * 1024);
        let mut parts = partitions(&op, &ids, &edges, &resources);
        let adjacency: Vec<_> = parts.iter().map(|p| p.adjacency_identity()).collect();
        let mut expected = vec![1.0 / ids.len() as f64; ids.len()];
        for number in 0..20 {
            let round = Round {
                operation: op.clone(),
                number,
            };
            for part in &mut parts {
                part.begin(&round).unwrap();
            }
            // This in-memory harness tests the partition protocol, not a Sail
            // transport. Production uses bounded Arrow streams and shuffles.
            let mut outputs = Vec::new();
            let mut emissions = Vec::new();
            for part in &mut parts {
                emissions.push(
                    part.emit(&round, |update| {
                        outputs.push(update);
                        Ok(())
                    })
                    .unwrap(),
                );
            }
            for update in outputs {
                parts[op.owner(update.target)].receive(update).unwrap();
            }
            for (producer, emission) in emissions.iter().enumerate() {
                for part in &mut parts {
                    part.finish_producer(
                        &round,
                        producer,
                        emission.sequences[part.partition()],
                        emission.dangling_mass,
                    )
                    .unwrap();
                }
            }
            let old = expected.clone();
            expected.fill(0.0);
            let mut dangling_reference = 0.0;
            for (u, id) in ids.iter().enumerate() {
                let outgoing: Vec<_> = edges.iter().filter(|(source, _)| source == id).collect();
                if outgoing.is_empty() {
                    dangling_reference += old[u];
                }
                for (_, target) in &outgoing {
                    let v = ids.iter().position(|id| id == target).unwrap();
                    expected[v] += old[u] / outgoing.len() as f64;
                }
            }
            for rank in &mut expected {
                *rank = 0.15 / ids.len() as f64
                    + 0.85 * (*rank + dangling_reference / ids.len() as f64);
            }
            let mut mass = 0.0;
            for (p, part) in parts.iter_mut().enumerate() {
                mass += part.finish(&round, 0.85).unwrap().rank_mass;
                assert_eq!(part.adjacency_identity(), adjacency[p]);
                assert_eq!(part.next_round(), number + 1);
                for (id, rank) in part.ranks() {
                    assert!(
                        (rank - expected[ids.iter().position(|v| *v == id).unwrap()]).abs() < 1e-14
                    );
                }
            }
            assert!((mass - 1.0).abs() < 1e-14);
        }
        drop(parts);
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(resources);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn incomplete_or_replayed_messages_never_publish_new_ranks() {
    for corruption in 0..8 {
        let op = operation(2, 2);
        let round = Round {
            operation: op.clone(),
            number: 0,
        };
        let (resources, _, _) = resources(64 * 1024);
        let mut parts = partitions(&op, &[0, 1], &[(0, 1)], &resources);
        for part in &mut parts {
            part.begin(&round).unwrap();
        }
        let mut messages = Vec::new();
        let emission = parts[0]
            .emit(&round, |m| {
                messages.push(m);
                Ok(())
            })
            .unwrap();
        let mut message = messages.pop().unwrap();
        let before: Vec<_> = parts[1].ranks().collect();
        match corruption {
            0 => {
                parts[1].receive(message.clone()).unwrap();
                assert!(parts[1].receive(message).is_err());
            }
            1 => {
                message.sequence += 1;
                assert!(parts[1].receive(message).is_err());
            }
            2 => {
                Arc::make_mut(&mut message.round).operation.generation += 1;
                assert!(parts[1].receive(message).is_err());
            }
            3 => {
                Arc::make_mut(&mut message.round).operation.package = "foreign".into();
                assert!(parts[1].receive(message).is_err());
            }
            4 => {
                message.target = 0;
                assert!(parts[1].receive(message).is_err());
            }
            5 => {
                message.value = f64::NAN;
                assert!(parts[1].receive(message).is_err());
            }
            6 => {
                assert!(
                    parts[1]
                        .finish_producer(&round, 0, emission.sequences[1], emission.dangling_mass)
                        .is_err()
                );
            }
            7 => {
                assert!(parts[1].finish(&round, 0.85).is_err());
            }
            _ => unreachable!(),
        }
        assert_eq!(parts[1].ranks().collect::<Vec<_>>(), before);
        assert!(parts[1].begin(&round).is_err());
    }
}

#[test]
fn output_lease_survives_partition_drop_and_releases_only_at_final_owner() {
    let op = operation(1, 1);
    let round = Round {
        operation: op.clone(),
        number: 0,
    };
    let (resources, usage, dropped) = resources(64 * 1024);
    let mut part = PageRankPartition::build(op, 0, &[0], &[], resources.clone()).unwrap();
    part.begin(&round).unwrap();
    let result = part.emit(&round, |_| Ok(())).unwrap();
    drop(part);
    drop(resources);
    assert!(usage.usage().unwrap().live_bytes > 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(result);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn quota_failure_cancellation_and_broken_output_do_not_commit_a_round() {
    let op = operation(1, 2);
    let (small, usage, _) = resources(1024);
    assert!(PageRankPartition::build(op.clone(), 0, &[0, 1], &[(0, 1)], small).is_err());
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    let round = Round {
        operation: op.clone(),
        number: 0,
    };
    let (resources, usage, _) = resources(64 * 1024);
    let mut part = PageRankPartition::build(op, 0, &[0, 1], &[(0, 1)], resources).unwrap();
    part.begin(&round).unwrap();
    assert!(
        part.emit(&round, |_| Err("shuffle output closed".into()))
            .is_err()
    );
    assert_eq!(part.next_round(), 0);
    assert!(part.begin(&round).is_err());
    part.abort().unwrap();
    assert!(part.begin(&round).is_err());
    drop(part);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
}

#[test]
fn queued_updates_keep_the_memory_lease_after_emission_and_partition_drop() {
    let op = operation(1, 2);
    let round = Round {
        operation: op.clone(),
        number: 0,
    };
    let (resources, usage, dropped) = resources(64 * 1024);
    let mut part = PageRankPartition::build(op, 0, &[0, 1], &[(0, 1)], resources).unwrap();
    part.begin(&round).unwrap();
    let mut queued = Vec::new();
    let emission = part
        .emit(&round, |update| {
            queued.push(update);
            Ok(())
        })
        .unwrap();
    drop(emission);
    drop(part);
    assert!(usage.usage().unwrap().live_bytes > 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(queued);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn cancellation_during_emission_and_stale_rounds_leave_previous_rank_state() {
    let op = operation(1, 3);
    let round = Round {
        operation: op.clone(),
        number: 0,
    };
    let (resources, usage, _) = resources(64 * 1024);
    let mut part =
        PageRankPartition::build(op.clone(), 0, &[0, 1, 2], &[(0, 1), (0, 2)], resources).unwrap();
    let before: Vec<_> = part.ranks().collect();
    part.begin(&round).unwrap();
    let mut callbacks = 0;
    assert!(
        part.emit(&round, |_| {
            callbacks += 1;
            usage.cancel().unwrap();
            Ok(())
        })
        .is_err()
    );
    assert_eq!(callbacks, 1);
    assert_eq!(part.ranks().collect::<Vec<_>>(), before);
    assert_eq!(part.next_round(), 0);
    drop(part);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);

    let (resources, _, _) = self::resources(64 * 1024);
    let mut part = PageRankPartition::build(op.clone(), 0, &[0, 1, 2], &[], resources).unwrap();
    let skipped = Round {
        operation: op,
        number: 1,
    };
    assert!(part.begin(&skipped).is_err());
    assert!(part.begin(&round).is_err());
}

#[test]
fn owned_emission_drains_a_one_message_channel_without_holding_the_partition_lock() {
    use std::sync::{Mutex, mpsc::sync_channel};
    let op = operation(1, 4);
    let round = Round {
        operation: op.clone(),
        number: 0,
    };
    let (resources, usage, _) = resources(64 * 1024);
    let part = Arc::new(Mutex::new(
        PageRankPartition::build(
            op,
            0,
            &[0, 1, 2, 3],
            &[(0, 1), (0, 2), (0, 3), (1, 3)],
            resources,
        )
        .unwrap(),
    ));
    let mut cursor = {
        let mut owned = part.lock().unwrap();
        owned.begin(&round).unwrap();
        owned.start_emission(&round).unwrap()
    };
    let (tx, rx) = sync_channel(1);
    let producer = std::thread::spawn(move || {
        while let Some(update) = cursor.next_update().unwrap() {
            tx.send(update).unwrap();
        }
        cursor.finish().unwrap()
    });
    // Every receive reacquires the native owner lock while the producer can be
    // blocked on the bounded channel. No sleep or timed race decides this test.
    for update in rx {
        part.lock().unwrap().receive(update).unwrap();
    }
    let emission = producer.join().unwrap();
    {
        let mut owned = part.lock().unwrap();
        owned
            .finish_producer(&round, 0, emission.sequences[0], emission.dangling_mass)
            .unwrap();
        let result = owned.finish(&round, 0.85).unwrap();
        assert!((result.rank_mass - 1.0).abs() < 1e-14);
    }
    drop(emission);
    drop(part);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
}

#[test]
fn abandoning_an_owned_emission_cancels_and_releases_its_snapshot() {
    let op = operation(1, 2);
    let round = Round {
        operation: op.clone(),
        number: 0,
    };
    let (resources, usage, dropped) = resources(64 * 1024);
    let mut part = PageRankPartition::build(op, 0, &[0, 1], &[(0, 1)], resources).unwrap();
    part.begin(&round).unwrap();
    let cursor = part.start_emission(&round).unwrap();
    drop(part);
    assert!(usage.usage().unwrap().live_bytes > 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(cursor);
    assert!(usage.checkpoint().is_err());
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
