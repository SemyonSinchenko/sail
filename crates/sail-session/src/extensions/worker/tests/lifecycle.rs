use super::*;

#[test]
fn worker_binding_requires_callable_close_and_releases_rejected_quota() -> Result<()> {
    let (runtime, pool) = runtime(64)?;
    let bootstrap = python_factory(None)?;
    Python::attach(|py| bootstrap.bind(py)?.setattr("noncallable_close", true))
        .map_err(py_error)?;
    let factory = PythonWorkerFactory::new(
        "exact-package".into(),
        manifest(),
        bootstrap,
        runtime,
        Arc::new(NativeResourceTracker::default()),
    )?;
    let result = factory.owner(&scope(1), "operation");
    assert!(matches!(result, Err(error) if error.to_string().contains("callable close()")));
    assert_eq!(pool.reserved(), 0, "rejected binding must return its quota");
    Ok(())
}

#[test]
fn close_calls_every_operation_once_even_when_one_callback_fails() -> Result<()> {
    let (runtime, pool) = runtime(128)?;
    let bootstrap = python_factory(None)?;
    let factory = PythonWorkerFactory::new(
        "exact-package".into(),
        manifest(),
        bootstrap.clone(),
        runtime,
        Arc::new(NativeResourceTracker::default()),
    )?;
    let first = factory.owner(&scope(1), "first")?;
    let second = factory.owner(&scope(1), "second")?;
    Python::attach(|py| first.bind(py)?.setattr("fail_close", true)).map_err(py_error)?;
    factory.close_job(&scope(1).job);
    factory.close_job(&scope(1).job);
    assert!(factory.owner(&scope(1), "third").is_err());
    Python::attach(|py| -> Result<()> {
        let mut closed = bootstrap
            .bind(py)
            .map_err(py_error)?
            .getattr("closed_operations")
            .map_err(py_error)?
            .extract::<Vec<String>>()
            .map_err(py_error)?;
        closed.sort();
        assert_eq!(closed, ["first", "second"]);
        for owner in [&first, &second] {
            assert_eq!(
                owner
                    .bind(py)
                    .map_err(py_error)?
                    .getattr("close_calls")
                    .map_err(py_error)?
                    .extract::<usize>()
                    .map_err(py_error)?,
                1
            );
        }
        Ok(())
    })?;
    assert_eq!(
        pool.reserved(),
        128,
        "live owners retain valid resources after cancellation"
    );
    drop(first);
    drop(second);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}
