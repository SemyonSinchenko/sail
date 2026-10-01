use super::*;

#[test]
fn tight_admission_accepts_transferred_storage_for_both_algorithms() {
    let mut failed = vec![];
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        let mut f = fixture(65536, algorithm, "active");
        let _watch = watch::start(65536 * size_of::<Option<SsspLabel>>(), &pointers(&f.part));
        let available = LIMIT - f.execution.usage().unwrap().live_bytes;
        // Pending mask + frontier fits, while one additional dense label
        // vector does not. Hold this blocker through next-barrier publication.
        let headroom = 1 << 20;
        let blocker = f.execution.reserve(available - headroom).unwrap();
        let result = f.part.finish(&f.phase);
        println!(
            "SSSP_REUSE_HEADROOM algorithm={algorithm:?} local_vertices=65536 headroom={headroom} result={result:?}"
        );
        if result.is_err() {
            failed.push(algorithm);
        } else {
            assert_eq!(f.part.next_phase(), f.phase.number + 1);
        }
        drop(blocker);
        check_release(f);
    }
    assert!(
        failed.is_empty(),
        "replacement label admission remained: {failed:?}"
    );
}

#[test]
fn old_row_cursor_retains_snapshot_and_last_lease_after_publication() {
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        let mut f = fixture(1024, algorithm, "active");
        let _watch = watch::start(1024 * size_of::<Option<SsspLabel>>(), &pointers(&f.part));
        // Exercise the actual row cursor's Arc/lease ownership, deliberately
        // retaining a pre-publication snapshot through this private test seam.
        // Public row_cursor() still requires a sealed result.
        let mut cursor = SsspRowCursor {
            adjacency: f.part.adjacency.clone(),
            values: f.part.values.clone(),
            resources: f.part.resources.clone(),
            position: 0,
        };
        let expected = f.part.values.labels.clone();
        f.part.finish(&f.phase).unwrap();
        assert_ne!(f.part.values.labels, expected);
        let observed = f.observed.clone();
        let execution = f.execution.clone();
        drop(f);
        assert_eq!(observed.callbacks.load(Ordering::SeqCst), 0);
        for label in expected {
            assert_eq!(cursor.next_row().unwrap().unwrap().label, label);
        }
        assert!(cursor.next_row().unwrap().is_none());
        drop(cursor);
        assert_eq!(observed.callbacks.load(Ordering::SeqCst), 1);
        assert_eq!(observed.admitted.load(Ordering::SeqCst), 0);
        assert_eq!(observed.watched_live.load(Ordering::SeqCst), 0);
        assert_eq!(execution.usage().unwrap().live_bytes, 0);
    }
}

#[test]
fn selection_preserves_ties_parent_only_updates_and_bucket_carryover() {
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        let mut f = fixture(6, algorithm, "active");
        let label = |d, h, p| Some(SsspLabel::from_parts(d, h, p).unwrap());
        // White-box merge operands isolate branches that a final-distance-only
        // oracle misses. End-to-end weighted protocol/oracle tests remain separate.
        let values = Arc::get_mut(&mut f.part.values).unwrap();
        values.labels = vec![
            label(0.0, 0, 0),
            label(2.0, 2, 9),
            label(8.0, 2, 9),
            label(8.0, 2, 3),
            None,
            None,
        ];
        values.active = vec![0, 1, 2, 3];
        values.reached = 4;
        values.reachable_edges = 5;
        let State::Receiving(inbox) = &mut f.part.state else {
            unreachable!()
        };
        inbox.candidates.copy_from_slice(&[
            label(1.0, 1, 0),
            label(2.0, 2, 6),
            label(3.0, 2, 3),
            label(8.0, 2, 3),
            label(4.0, 1, 0),
            None,
        ]);
        let _watch = watch::start(6 * size_of::<Option<SsspLabel>>(), &pointers(&f.part));
        let before_work = f.execution.usage().unwrap().counted_work().unwrap();
        f.part.finish(&f.phase).unwrap();
        assert_eq!(
            f.part.values.labels,
            vec![
                label(0.0, 0, 0),
                label(2.0, 2, 6),
                label(3.0, 2, 3),
                label(8.0, 2, 3),
                label(4.0, 1, 0),
                None
            ]
        );
        let expected = if algorithm == SsspAlgorithm::Reference {
            vec![2, 4]
        } else {
            vec![2, 3, 4]
        };
        assert_eq!(f.part.values.active, expected);
        assert_eq!(f.part.values.reached, 5);
        assert_eq!(f.part.values.reachable_edges, 5); // newly reached isolate
        assert_eq!(
            f.execution.usage().unwrap().counted_work().unwrap() - before_work,
            10
        );
        check_release(f);
    }
}

#[test]
fn failures_do_not_publish_mutated_candidates_or_release_live_storage() {
    let mut late_failure_before_merge = vec![];
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        for failure in [
            "early_quota",
            "late_quota",
            "work",
            "cancel",
            "final_checkpoint",
        ] {
            let mut f = fixture(6, algorithm, "active");
            let old = f.part.values.clone();
            let expected = old.labels.clone();
            let work = f.execution.usage().unwrap().counted_work().unwrap();
            let _watch = watch::start(6 * size_of::<Option<SsspLabel>>(), &pointers(&f.part));
            let mut blocker = None;
            let fired = Arc::new(AtomicUsize::new(0));
            let mut hook = None;
            match failure {
                "early_quota" | "late_quota" => {
                    let available = LIMIT - f.execution.usage().unwrap().live_bytes;
                    let headroom = if failure == "early_quota" { 0 } else { 500 };
                    blocker = Some(f.execution.reserve(available - headroom).unwrap());
                }
                "work" => {
                    f.execution
                        .charge_work(usize::MAX - work - old.active.len() - 2)
                        .unwrap();
                }
                "cancel" => f.execution.cancel().unwrap(),
                "final_checkpoint" => {
                    let execution = f.execution.clone();
                    let fired = fired.clone();
                    // StatisticsInbox::new allocates this vector after admitting
                    // its charge and after every merge work check. Cancel there;
                    // no wall-clock race or machine-speed assumption is involved.
                    hook = Some(watch::on_next_size(
                        3 * size_of::<Option<SsspStatisticsValues>>(),
                        move || {
                            fired.fetch_add(1, Ordering::SeqCst);
                            execution.cancel().unwrap();
                        },
                    ));
                }
                _ => unreachable!(),
            }
            let before_finish = f.execution.usage().unwrap().counted_work().unwrap();
            let error = f.part.finish(&f.phase).unwrap_err();
            println!(
                "SSSP_REUSE_FAILURE algorithm={algorithm:?} case={failure} finish_work={} error={error}",
                f.execution.usage().unwrap().counted_work().unwrap() - before_finish
            );
            let expected_error = match failure {
                "early_quota" | "late_quota" => "memory",
                "work" => "work",
                _ => "cancel",
            };
            assert!(error.contains(expected_error), "{error}");
            assert!(Arc::ptr_eq(&old, &f.part.values));
            assert_eq!(f.part.values.labels, expected);
            assert_eq!(f.part.next_phase(), f.phase.number);
            assert!(f.part.row_cursor().is_err());
            if failure != "cancel" {
                assert!(matches!(f.part.state, State::Failed));
            }
            if failure == "late_quota"
                && f.execution.usage().unwrap().counted_work().unwrap() - work
                    != old.active.len() + 6
            {
                late_failure_before_merge.push(algorithm);
            }
            if failure == "final_checkpoint" {
                assert_eq!(fired.load(Ordering::SeqCst), 1);
                assert_eq!(
                    f.execution.usage().unwrap().counted_work().unwrap() - work,
                    old.active.len() + 6
                );
            }
            drop(hook);
            drop(blocker);
            drop(old);
            drop(expected);
            check_release(f);
        }
    }
    assert!(
        late_failure_before_merge.is_empty(),
        "late quota failed before merge: {late_failure_before_merge:?}"
    );
}
