#[path = "delta_support/mod.rs"]
mod support;
use sail_argentea_core::{SsspLabel, WeightedAdjacency};
use std::sync::atomic::Ordering;

#[test]
fn weighted_csr_retains_exact_arc_weight_pairs_duplicates_and_signed_ids() {
    let (resources, usage, drops) = support::resources(1 << 20);
    let op = support::operation(1, 4);
    let graph = WeightedAdjacency::build(
        &op,
        0,
        &[i64::MAX, 0, i64::MIN, 7],
        &[
            (0, i64::MAX, 1.25),
            (i64::MIN, 0, 0.0),
            (0, i64::MIN, 9.0),
            (0, i64::MAX, 2.5),
            (0, 0, -0.0),
        ],
        &resources,
    )
    .unwrap();
    assert_eq!(graph.vertices(), &[i64::MIN, 0, 7, i64::MAX]);
    assert_eq!(graph.arc_count(), 5);
    assert_eq!(
        graph.outgoing(0).unwrap(),
        &[(i64::MAX, 1.25), (i64::MIN, 9.0), (i64::MAX, 2.5), (0, 0.0)]
    );
    assert_eq!(graph.outgoing(0).unwrap()[3].1.to_bits(), 0.0f64.to_bits());
    assert!(graph.outgoing(7).unwrap().is_empty());
    assert!(graph.outgoing(100).is_err());
    let retained = graph.clone();
    let identity = graph.identity();
    drop(graph);
    drop(resources);
    assert_eq!(retained.identity(), identity);
    assert!(usage.usage().unwrap().live_bytes > 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(retained);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn rejected_weights_sources_and_owners_release_all_build_admission() {
    let (r, usage, _) = support::resources(1 << 20);
    let op = support::operation(2, 3);
    for weight in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.5] {
        assert!(WeightedAdjacency::build(&op, 0, &[0, 2], &[(2, 1, weight)], &r).is_err());
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
    }
    for ids in [&[0, 0][..], &[0, 1][..]] {
        assert!(WeightedAdjacency::build(&op, 0, ids, &[], &r).is_err());
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
    }
    assert!(WeightedAdjacency::build(&op, 0, &[0], &[(2, 1, 1.0)], &r).is_err());
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    // Receiver topology validation, not this source-local constructor, proves
    // target membership. Empty owners and remote target IDs must be representable.
    let empty = WeightedAdjacency::build(&op, 1, &[], &[], &r).unwrap();
    assert!(empty.vertices().is_empty());
    drop(empty);
    let graph = WeightedAdjacency::build(&op, 0, &[0], &[(0, 1, 1.0)], &r).unwrap();
    assert_eq!(graph.outgoing(0).unwrap(), &[(1, 1.0)]);
    drop(graph);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
}

#[test]
fn weighted_build_respects_quota_and_preexisting_cancellation() {
    let (r, usage, _) = support::resources(4096);
    let op = support::operation(1, 1);
    assert!(
        WeightedAdjacency::build(&op, 0, &[0], &[], &r)
            .unwrap_err()
            .contains("memory")
    );
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    let (r, usage, _) = support::resources(1 << 20);
    r.execution.cancel().unwrap();
    assert!(
        WeightedAdjacency::build(&op, 0, &[0], &[], &r)
            .unwrap_err()
            .contains("cancel")
    );
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
}

#[test]
fn path_order_prioritizes_distance_hops_then_signed_parent() {
    let source = SsspLabel::source(i64::MIN);
    let a = source.extend(i64::MIN, 1.0).unwrap();
    assert!(a.precedes(SsspLabel::from_parts(1.0, 2, i64::MIN).unwrap()));
    assert!(a.precedes(SsspLabel::from_parts(1.0, 1, i64::MAX).unwrap()));
    assert!(
        SsspLabel::from_parts(0.5, 99, i64::MAX)
            .unwrap()
            .precedes(a)
    );
    let previous = SsspLabel::from_parts(1.0, 1, i64::MAX).unwrap();
    assert!(!a.changes_outgoing(previous));
    assert!(a.changes_outgoing(SsspLabel::from_parts(1.0, 2, i64::MIN).unwrap()));
    assert!(!source.extend(0, 0.0).unwrap().precedes(source));
    assert_eq!(a.distance(), 1.0);
    assert_eq!(a.hops(), 1);
    assert_eq!(a.parent(), i64::MIN);
}

#[test]
fn candidate_and_bucket_overflow_are_errors_without_saturation() {
    for distance in [f64::NAN, f64::INFINITY, -1.0] {
        assert!(SsspLabel::from_parts(distance, 1, 0).is_err());
    }
    for weight in [f64::NAN, f64::INFINITY, -1.0] {
        assert!(SsspLabel::source(0).extend(0, weight).is_err());
    }
    assert!(
        SsspLabel::from_parts(f64::MAX, 1, 0)
            .unwrap()
            .extend(0, f64::MAX)
            .unwrap_err()
            .contains("distance overflow")
    );
    assert!(
        SsspLabel::from_parts(0.0, u64::MAX, 0)
            .unwrap()
            .extend(0, 0.0)
            .unwrap_err()
            .contains("hop overflow")
    );
    let label = SsspLabel::from_parts(9.0, 1, 0).unwrap();
    assert_eq!(label.bucket(4.0).unwrap(), 2.0);
    for delta in [0.0, -1.0, f64::INFINITY, f64::NAN] {
        assert!(label.bucket(delta).is_err());
    }
    assert!(
        SsspLabel::from_parts(f64::MAX, 1, 0)
            .unwrap()
            .bucket(f64::MIN_POSITIVE)
            .unwrap_err()
            .contains("bucket overflow")
    );
    assert_eq!(
        SsspLabel::from_parts(-0.0, 0, 0)
            .unwrap()
            .distance()
            .to_bits(),
        0.0f64.to_bits()
    );
}
