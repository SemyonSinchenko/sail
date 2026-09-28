mod delta_support;
use delta_support::*;
use sail_argentea_core::{DeltaMode, DeltaPartition};
use std::sync::atomic::Ordering;

#[test]
fn signed_residual_and_dangling_pushes_match_dense_transitions_at_every_phase() {
    for p in [1, 2, 3, 11] {
        let ids = [0, 1, 2];
        let edges = [(0, 1), (1, 1)];
        let op = operation(p, 3);
        let opts = options();
        let (resources, usage, drops) = resources(4 * 1024 * 1024);
        let mut parts = partitions(&op, &ids, &edges, opts, &resources);
        let adjacency: Vec<_> = parts
            .iter()
            .map(DeltaPartition::adjacency_identity)
            .collect();
        let mut x = vec![1.0 / 3.0; 3];
        let mut r = vec![0.0; 3];
        let mut negative_edges = 0;
        let mut negative_dangling = 0;
        let mut done = None;
        for _ in 0..opts.max_pushes * 2 + 2 {
            stats(&mut parts).unwrap();
            let phase = phase(&op, &parts);
            let seals: Vec<_> = parts.iter_mut().map(|p| p.seal(&phase).unwrap()).collect();
            assert!(seals.iter().all(|s| s == &seals[0]));
            if let Some(seal) = seals[0] {
                done = Some(seal);
                break;
            }
            let (mode, traffic) = exchange(&op, &mut parts).unwrap();
            negative_edges += traffic.negative_updates;
            negative_dangling += traffic.negative_dangling;
            match mode {
                DeltaMode::Certify => {
                    let mass: f64 = x.iter().sum();
                    for value in &mut x {
                        *value /= mass;
                    }
                    r = transition(&ids, &edges, &x, opts.damping)
                        .iter()
                        .zip(&x)
                        .map(|(tx, x)| tx - x)
                        .collect();
                }
                DeltaMode::Push => {
                    let mass: f64 = x.iter().sum();
                    let norm: f64 = r.iter().map(|v| v.abs()).sum();
                    let cutoff = (norm / 6.0).min(opts.tolerance * mass / 12.0);
                    let push: Vec<_> = r
                        .iter()
                        .map(|&r| if r.abs() > cutoff { r } else { 0.0 })
                        .collect();
                    let transported = transition(&ids, &edges, &push, opts.damping);
                    for i in 0..3 {
                        x[i] += push[i];
                        r[i] += transported[i] - (1.0 - opts.damping) / 3.0 - push[i];
                    }
                }
                _ => panic!("unexpected mode"),
            }
            for (i, part) in parts.iter().enumerate() {
                assert_eq!(part.adjacency_identity(), adjacency[i]);
                for (id, actual, residual) in part.state_rows() {
                    assert!(
                        (actual - x[id as usize]).abs() < 2e-14,
                        "p={p} phase={} x",
                        phase.number
                    );
                    assert!(
                        (residual - r[id as usize]).abs() < 2e-14,
                        "p={p} phase={} residual",
                        phase.number
                    );
                }
            }
        }
        let done = done.expect("dense fixture converges");
        assert!(negative_edges > 0);
        assert!(negative_dangling > 0);
        assert!(done.residual_l1 <= opts.tolerance);
        assert!(done.pushes > 0);
        let scores = collected(&ids, &parts);
        let tx = transition(&ids, &edges, &scores, opts.damping);
        let residual: f64 = tx.iter().zip(&scores).map(|(t, x)| (t - x).abs()).sum();
        assert!((residual - done.residual_l1).abs() < 1e-14);
        drop(parts);
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
        drop(resources);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn banda_reactivation_fixture_and_settling_components_avoid_inactive_edges() {
    let ids = vec![-5, 0, 1, 2, 9, 10, 11, 12, 13];
    let indexed = [
        (6, 4),
        (3, 5),
        (0, 3),
        (0, 5),
        (6, 4),
        (1, 3),
        (5, 3),
        (7, 6),
        (7, 2),
        (4, 2),
        (3, 4),
        (6, 6),
        (5, 3),
        (2, 7),
        (1, 0),
        (1, 2),
    ];
    let edges = indexed
        .iter()
        .map(|&(a, b)| (ids[a], ids[b]))
        .collect::<Vec<_>>();
    let settle_ids = (0..128).collect::<Vec<i64>>();
    let mut settle_edges = (0..63).map(|i| (i, i + 1)).collect::<Vec<_>>();
    settle_edges.push((63, 63));
    for i in (64..128).step_by(2) {
        settle_edges.extend([(i, i + 1), (i + 1, i + 1)]);
    }
    for (fixture, ids, edges) in [(0, ids, edges), (1, settle_ids, settle_edges)] {
        for p in [1, 3, 11] {
            let op = operation(p, ids.len() as u64);
            let opts = options();
            let (resources, _, _) = resources(4 * 1024 * 1024);
            let mut parts = partitions(&op, &ids, &edges, opts, &resources);
            let mut avoided = false;
            let mut reactivated = false;
            let mut done = None;
            for _ in 0..1026 {
                let reports = parts
                    .iter()
                    .map(DeltaPartition::statistics)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                if reports[0].completed == DeltaMode::Push {
                    avoided |=
                        reports.iter().map(|s| s.active_edges).sum::<u64>() < edges.len() as u64;
                    reactivated |= reports.iter().any(|s| s.reactivated_vertices > 0);
                }
                drop(reports);
                stats(&mut parts).unwrap();
                let phase = phase(&op, &parts);
                let seals = parts
                    .iter_mut()
                    .map(|p| p.seal(&phase).unwrap())
                    .collect::<Vec<_>>();
                if let Some(s) = seals[0] {
                    done = Some(s);
                    break;
                }
                exchange(&op, &mut parts).unwrap();
            }
            let done = done.unwrap();
            assert!(avoided);
            if fixture == 0 {
                assert!(reactivated);
            }
            let scores = collected(&ids, &parts);
            let mut reference = vec![1.0 / ids.len() as f64; ids.len()];
            for _ in 0..1000 {
                reference = transition(&ids, &edges, &reference, opts.damping);
            }
            let error: f64 = scores
                .iter()
                .zip(&reference)
                .map(|(x, y)| (x - y).abs())
                .sum();
            // Independent dense power iteration stops far below the certified
            // tolerance; floating reductions need an explicit roundoff allowance.
            assert!(
                error <= done.stationary_error_bound + 2e-13,
                "fixture={fixture},p={p},error={error}"
            );
        }
    }
}

#[test]
fn stationary_zero_push_and_done_relays_keep_certified_state_and_counters() {
    for edges in [vec![], vec![(0, 1), (1, 2), (2, 0)]] {
        let op = operation(5, 3);
        let mut opts = options();
        opts.max_pushes = 0;
        let (resources, usage, drops) = resources(1024 * 1024);
        let mut parts = partitions(&op, &[0, 1, 2], &edges, opts, &resources);
        stats(&mut parts).unwrap();
        assert_eq!(exchange(&op, &mut parts).unwrap().0, DeltaMode::Certify);
        let scores = collected(&[0, 1, 2], &parts);
        for _ in 0..4 {
            stats(&mut parts).unwrap();
            let (mode, traffic) = exchange(&op, &mut parts).unwrap();
            assert_eq!(mode, DeltaMode::Done);
            assert_eq!(traffic.messages, 0);
            assert_eq!(collected(&[0, 1, 2], &parts), scores);
            assert!(
                parts
                    .iter()
                    .all(|p| p.pushes() == 0 && p.certificate_passes() == 1)
            );
        }
        stats(&mut parts).unwrap();
        let phase = phase(&op, &parts);
        for p in &mut parts {
            assert_eq!(p.seal(&phase).unwrap().unwrap().pushes, 0);
        }
        let mut cursors = parts
            .iter()
            .map(|p| p.rank_cursor().unwrap())
            .collect::<Vec<_>>();
        drop(parts);
        drop(resources);
        assert!(usage.usage().unwrap().live_bytes > 0);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let mut rows = 0;
        for c in &mut cursors {
            while c.next_rank().unwrap().is_some() {
                rows += 1;
            }
        }
        assert_eq!(rows, 3);
        drop(cursors);
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn cap_forces_a_fresh_certificate_and_never_publishes_uncertified_rows() {
    for max_pushes in [0, 1, 2] {
        let op = operation(2, 3);
        let opts = sail_argentea_core::DeltaOptions {
            max_pushes,
            tolerance: 1e-15,
            ..options()
        };
        let (resources, _, _) = resources(1024 * 1024);
        let mut parts = partitions(&op, &[0, 1, 2], &[(0, 1), (1, 1)], opts, &resources);
        for _ in 0..max_pushes + 2 {
            stats(&mut parts).unwrap();
            exchange(&op, &mut parts).unwrap();
            if parts[0].completed_mode() == DeltaMode::Certify && parts[0].pushes() == max_pushes {
                break;
            }
        }
        assert_eq!(parts[0].completed_mode(), DeltaMode::Certify);
        assert_eq!(parts[0].pushes(), max_pushes);
        assert_eq!(parts[0].certificate_passes(), 1 + u64::from(max_pushes > 0));
        stats(&mut parts).unwrap();
        let phase = phase(&op, &parts);
        for p in &mut parts {
            assert!(p.seal(&phase).unwrap_err().contains("push cap"));
            assert!(p.rank_cursor().is_err());
        }
    }
}
