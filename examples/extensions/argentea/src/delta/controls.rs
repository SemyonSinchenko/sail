//! White-box fault injection is limited to this module. In particular, a forged
//! maintained residual must not replace a freshly recomputed certificate.
use super::*;
use grust_procedures::{ExecutionContext, ExecutionLimits};
use sail_native_resource_ffi::MemoryLease;
fn part() -> DeltaPartition {
    let execution = ExecutionContext::new(ExecutionLimits {
        memory_bytes: 1024 * 1024,
        work_units: usize::MAX,
        batch_rows: 16,
        deadline: None,
    })
    .unwrap();
    let resources = Resources::new(execution, MemoryLease::new(Arc::new(()), 1024 * 1024)).unwrap();
    DeltaPartition::build(
        Operation {
            package: "pkg".into(),
            session: "s".into(),
            operation: "op".into(),
            snapshot: "snap".into(),
            generation: 1,
            partitions: 1,
            vertices: 3,
        },
        0,
        &[0, 1, 2],
        &[(0, 1), (1, 1)],
        DeltaOptions {
            damping: 0.85,
            tolerance: 1e-12,
            max_pushes: 100,
        },
        resources,
    )
    .unwrap()
}
fn round(p: &DeltaPartition) -> Round {
    Round {
        operation: p.operation.clone(),
        number: p.next_phase,
    }
}
fn barrier(p: &mut DeltaPartition) {
    p.receive_statistics(&p.statistics().unwrap()).unwrap();
}
fn exchange(p: &mut DeltaPartition) -> DeltaMode {
    let phase = round(p);
    let mut cursor = p.start_emission(&phase).unwrap();
    let mode = cursor.mode();
    while let Some(m) = cursor.next_update().unwrap() {
        p.receive(&m).unwrap();
    }
    p.finish_producer(&cursor.finish().unwrap()).unwrap();
    p.finish(&phase).unwrap();
    mode
}
#[test]
fn maintained_residual_cannot_forge_a_certificate_and_failure_rebases() {
    let mut p = part();
    barrier(&mut p);
    assert_eq!(exchange(&mut p), DeltaMode::Certify);
    barrier(&mut p);
    assert_eq!(exchange(&mut p), DeltaMode::Push);
    let score_before = p.values.scores.clone();
    // Simulate accumulated residual drift: cheap normalized bound appears zero,
    // but x is still far from the fixed point. This never enters production API.
    Arc::get_mut(&mut p.values).unwrap().residual.fill(0.0);
    barrier(&mut p);
    assert!(p.seal(&round(&p)).unwrap().is_none());
    assert_eq!(exchange(&mut p), DeltaMode::Certify);
    assert_eq!(p.certificate_passes(), 2);
    assert!(p.values.residual.iter().map(|r| r.abs()).sum::<f64>() > p.options.tolerance);
    let mass: f64 = score_before.iter().sum();
    for (x, old) in p.values.scores.iter().zip(score_before) {
        assert_eq!(*x, old / mass);
    }
    barrier(&mut p);
    assert!(p.seal(&round(&p)).unwrap().is_none());
    assert_eq!(exchange(&mut p), DeltaMode::Push);
    let mut converged = false;
    for _ in 0..202 {
        barrier(&mut p);
        if p.seal(&round(&p)).unwrap().is_some() {
            converged = true;
            break;
        }
        exchange(&mut p);
    }
    assert!(converged);
}
#[test]
fn nonfinite_and_negative_scores_or_residuals_are_rejected() {
    for (score, bad) in [
        (true, -0.1),
        (true, f64::NAN),
        (true, f64::INFINITY),
        (false, f64::NAN),
        (false, f64::INFINITY),
    ] {
        let mut p = part();
        let values = Arc::get_mut(&mut p.values).unwrap();
        if score {
            values.scores[0] = bad;
        } else {
            values.residual[0] = bad;
        }
        assert!(p.statistics().is_err());
        assert!(p.rank_cursor().is_err());
    }
}
