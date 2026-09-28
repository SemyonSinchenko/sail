use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContextProvider;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::collect_partitioned;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::prelude::SessionConfig;
use tokio::sync::Notify;

use super::*;
use crate::extensions::manifest::RelationType;

mod lifecycle;

fn manifest() -> Manifest {
    Manifest {
        name: "worker-fixture".into(),
        version: "1".into(),
        api_version: 1,
        datafusion_version: "55.1.0".into(),
        arrow_version: "59.3.0".into(),
        placement: "worker".into(),
        memory_bytes: Some(64),
        relation_types: vec![RelationType {
            type_url: "fixture.worker".into(),
            accepts_bare: true,
            min_inputs: 0,
            max_inputs: 2,
        }],
    }
}

fn scope(job_id: u64) -> WorkerTaskScope {
    WorkerTaskScope {
        job: WorkerJobIdentity {
            session_id: "host-session".into(),
            job_id,
        },
        worker_id: 7,
        stage: 0,
        partition: 0,
        attempt: 0,
    }
}

#[derive(Debug)]
struct TestContext(Arc<TaskContext>);
impl TaskContextProvider for TestContext {
    fn task_ctx(&self) -> Arc<TaskContext> {
        self.0.clone()
    }
}

fn python_factory(provider: Option<Arc<dyn TableProvider>>) -> Result<Arc<PythonOwner>> {
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::from_code(
            py,
            c"
class Bound:
    def __init__(self, resource, factory, operation):
        self.resource, self.provider = resource, factory.provider
        self.factory, self.operation = factory, operation
        self.close_calls = 0
        self.fail_close = False
    def scalar_udfs(self):
        return []
    def plan_relation(self, type_url, payload, inputs):
        return self.provider
    def close(self):
        self.close_calls += 1
        self.factory.closed_operations.append(self.operation)
        if self.fail_close:
            raise RuntimeError('fixture close failure')
class Factory:
    def __init__(self):
        self.provider = None
        self.calls = 0
        self.closed_operations = []
        self.noncallable_close = False
    def bind_with_resources(self, incarnation, quota, resource):
        import json
        self.calls += 1
        bound = Bound(resource, self, json.loads(incarnation)['operation_id'])
        if self.noncallable_close:
            bound.close = None
        return bound
",
            c"worker_fixture.py",
            c"worker_fixture",
        )
        .map_err(py_error)?;
        let factory = module
            .getattr("Factory")
            .and_then(|factory| factory.call0())
            .map_err(py_error)?;
        if let Some(provider) = provider {
            let context = Arc::new(TestContext(Arc::new(TaskContext::default())));
            let context_trait: Arc<dyn TaskContextProvider> = context.clone();
            let ffi = FFI_TableProvider::new(
                provider,
                false,
                tokio::runtime::Handle::try_current().ok(),
                &context_trait,
                None,
            );
            factory
                .setattr(
                    "provider",
                    PyCapsule::new_with_value(py, ffi, c"datafusion_table_provider")
                        .map_err(py_error)?,
                )
                .map_err(py_error)?;
            factory
                .setattr(
                    "context",
                    PyCapsule::new_with_value(py, context, c"test_context").map_err(py_error)?,
                )
                .map_err(py_error)?;
        }
        Ok(Arc::new(PythonOwner::new(factory.unbind())))
    })
}

fn runtime(bytes: usize) -> Result<(Arc<RuntimeEnv>, Arc<dyn MemoryPool>)> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
    let runtime = Arc::new(
        RuntimeEnvBuilder::default()
            .with_memory_pool(pool.clone())
            .build()?,
    );
    Ok((runtime, pool))
}

#[tokio::test]
async fn close_job_releases_state_only_after_inflight_owner_drops() -> Result<()> {
    let (runtime, pool) = runtime(64)?;
    let tracker = Arc::new(NativeResourceTracker::default());
    let factory = PythonWorkerFactory::new(
        "exact-package".into(),
        manifest(),
        python_factory(None)?,
        runtime,
        tracker.clone(),
    )?;
    let first = factory.owner(&scope(1), "operation")?;
    let second = factory.owner(&scope(1), "operation")?;
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(
        pool.reserved(),
        64,
        "one quota for all partitions/rounds of this worker operation"
    );
    factory.close_job(&scope(1).job);
    assert!(factory.owner(&scope(1), "operation").is_err());
    drop(first);
    assert_eq!(
        pool.reserved(),
        64,
        "inflight reader still owns the bound session"
    );
    let mut waiting = Box::pin(tracker.wait_for_release());
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    std::thread::spawn(move || drop(second))
        .join()
        .map_err(|_| py_error("owner release thread panicked"))?;
    waiting.await;
    assert_eq!(
        pool.reserved(),
        0,
        "no later Python request is required to release quota"
    );
    factory.close_job(&scope(1).job);
    assert!(factory.owner(&scope(1), "another-operation").is_err());
    Ok(())
}

#[test]
fn job_isolation_and_host_pool_contention_precede_binding() -> Result<()> {
    let (runtime, pool) = runtime(128)?;
    let factory = PythonWorkerFactory::new(
        "exact-package".into(),
        manifest(),
        python_factory(None)?,
        runtime,
        Arc::new(NativeResourceTracker::default()),
    )?;
    let first = factory.owner(&scope(1), "operation")?;
    let host = MemoryConsumer::new("DataFusion input").register(&pool);
    host.try_grow(65)
        .expect_err("native state already reserves half of the same pool");
    host.try_grow(1)?;
    assert!(factory.owner(&scope(2), "operation").is_err());
    drop(host);
    let second = factory.owner(&scope(2), "operation")?;
    assert!(
        !Arc::ptr_eq(&first, &second),
        "same opaque operation token cannot share state across jobs"
    );
    assert_eq!(pool.reserved(), 128);
    factory.close_job(&scope(1).job);
    drop(first);
    assert_eq!(pool.reserved(), 64);
    factory.close_job(&scope(2).job);
    drop(second);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[derive(Debug)]
struct PausedProvider {
    entered: Arc<Notify>,
    resume: Arc<Notify>,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for PausedProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        _: &dyn Session,
        _: Option<&Vec<usize>>,
        _: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.entered.notify_one();
        self.resume.notified().await;
        Ok(Arc::new(EmptyExec::new(self.schema())))
    }
}

fn descriptor() -> WorkerDescriptor {
    WorkerDescriptor {
        package_identity: "exact-package".into(),
        type_url: "fixture.worker".into(),
        operation_id: "operation".into(),
        payload: vec![],
        input_names: vec![],
        input_schemas: vec![],
        input_routing: vec![],
        output_schema: Schema::empty(),
        partitions: 1,
    }
}

#[tokio::test]
async fn closing_while_native_scan_is_pending_cannot_publish_or_resurrect_state() -> Result<()> {
    let (runtime, pool) = runtime(64)?;
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let provider = Arc::new(PausedProvider {
        entered: entered.clone(),
        resume: resume.clone(),
        schema: Arc::new(Schema::empty()),
    });
    let factory = Arc::new(PythonWorkerFactory::new(
        "exact-package".into(),
        manifest(),
        python_factory(Some(provider))?,
        runtime.clone(),
        Arc::new(NativeResourceTracker::default()),
    )?);
    let context = SessionContext::new_with_config_rt(SessionConfig::default(), runtime).task_ctx();
    let running_factory = factory.clone();
    let running = tokio::spawn(async move {
        running_factory
            .materialize(&descriptor(), vec![], Arc::new(scope(1)), context)
            .await
    });
    entered.notified().await;
    assert_eq!(pool.reserved(), 64);
    factory.close_job(&scope(1).job);
    assert_eq!(
        pool.reserved(),
        64,
        "pending scan remains a valid owner until it is dropped"
    );
    resume.notify_one();
    let result = running.await.map_err(py_error)?;
    assert!(
        matches!(result, Err(error) if error.to_string().contains("closed during preparation"))
    );
    assert_eq!(pool.reserved(), 0);
    assert!(factory.owner(&scope(1), "operation").is_err());
    Ok(())
}

#[tokio::test]
async fn package_and_relation_mismatch_are_rejected_before_native_binding() -> Result<()> {
    let (runtime, pool) = runtime(64)?;
    let context =
        SessionContext::new_with_config_rt(SessionConfig::default(), runtime.clone()).task_ctx();
    let factory = PythonWorkerFactory::new(
        "exact-package".into(),
        manifest(),
        python_factory(None)?,
        runtime,
        Arc::new(NativeResourceTracker::default()),
    )?;
    for (identity, url) in [
        ("other-package", "fixture.worker"),
        ("exact-package", "unregistered"),
    ] {
        let mut descriptor = descriptor();
        descriptor.package_identity = identity.into();
        descriptor.type_url = url.into();
        assert!(
            factory
                .materialize(&descriptor, vec![], Arc::new(scope(1)), context.clone())
                .await
                .is_err()
        );
        assert_eq!(pool.reserved(), 0);
    }
    assert!(
        factory
            .materialize(
                &descriptor(),
                vec![],
                Arc::new(scope(1)),
                Arc::new(TaskContext::default())
            )
            .await
            .is_err()
    );
    assert_eq!(
        pool.reserved(),
        0,
        "a foreign context cannot select another memory pool"
    );
    Ok(())
}

#[tokio::test]
async fn integer_owner_routes_to_exact_partition_and_keeps_empty_channels() -> Result<()> {
    use sail_physical_optimizer::{PhysicalOptimizerOptions, get_physical_optimizers};

    // The session width intentionally differs from the protocol's three owners.
    let context =
        SessionContext::new_with_config(SessionConfig::default().with_target_partitions(8));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "owner",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![0, 2, 0, 2]))])?;
    let input = context.read_batch(batch)?.create_physical_plan().await?;
    let routing = WorkerInputRouting::IntegerRange {
        column: "owner".into(),
        split_points: vec![1, 2],
    };
    let schema = input.schema().as_ref().clone();
    let routed = route_input(input, Some(&routing))?;
    let mut worker_descriptor = descriptor();
    worker_descriptor.input_names = vec!["owners".into()];
    worker_descriptor.input_schemas = vec![schema.clone()];
    worker_descriptor.input_routing = vec![Some(routing)];
    worker_descriptor.output_schema = schema;
    worker_descriptor.partitions = 3;
    let mut plan: Arc<dyn ExecutionPlan> =
        Arc::new(WorkerExtensionExec::new(worker_descriptor, vec![routed])?);
    for optimizer in get_physical_optimizers(PhysicalOptimizerOptions::default()) {
        plan = optimizer.optimize(plan, context.copied_config().options())?;
    }
    fn worker_input(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if let Some(worker) = plan.downcast_ref::<WorkerExtensionExec>() {
            return worker.children().first().map(|input| Arc::clone(input));
        }
        plan.children().into_iter().find_map(worker_input)
    }
    let input = worker_input(&plan).ok_or_else(|| py_error("worker node disappeared"))?;
    assert_eq!(input.properties().partitioning.partition_count(), 3);
    assert!(matches!(
        input.properties().partitioning,
        Partitioning::Range(_)
    ));
    let partitions = collect_partitioned(input, context.task_ctx()).await?;
    assert_eq!(partitions.len(), 3);
    let mut counts = [0; 3];
    for (partition, batches) in partitions.iter().enumerate() {
        for batch in batches {
            let owners = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| py_error("expected Int64 owners"))?;
            assert!(
                owners
                    .values()
                    .iter()
                    .all(|&owner| owner == partition as i64)
            );
            counts[partition] += owners.len();
        }
    }
    assert_eq!(counts, [2, 0, 2]);
    Ok(())
}

#[tokio::test]
async fn foreign_arrow_output_retains_quota_after_job_stream_and_plan_close() -> Result<()> {
    use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_ffi::execution_plan::ForeignExecutionPlan;
    use sail_common_datafusion::native_resource::MemoryLease;

    struct OutputAllocation {
        values: Vec<i64>,
        _lease: MemoryLease,
    }

    let (runtime, pool) = runtime(64)?;
    let factory = PythonWorkerFactory::new(
        "exact-package".into(),
        manifest(),
        python_factory(None)?,
        runtime,
        Arc::new(NativeResourceTracker::default()),
    )?;
    let owner = factory.owner(&scope(1), "operation")?;
    let observer = owner.clone();
    let lease = Python::attach(|py| -> Result<_> {
        let resource = owner
            .bind(py)
            .map_err(py_error)?
            .getattr("resource")
            .map_err(py_error)?;
        let capsule = resource.cast::<PyCapsule>().map_err(py_error)?;
        let pointer = capsule
            .pointer_checked(Some(MEMORY_LEASE_CAPSULE))
            .map_err(py_error)?;
        // SAFETY: this capsule is the real validated host lease created by owner().
        unsafe { MemoryLease::import(pointer, 64).map_err(py_error) }
    })?;
    let allocation = Arc::new(OutputAllocation {
        values: vec![17_i64],
        _lease: lease,
    });
    let pointer = std::ptr::NonNull::new(allocation.values.as_ptr() as *mut u8)
        .ok_or_else(|| py_error("output allocation pointer is null"))?;
    // SAFETY: the custom owner retains one immutable aligned i64 and the native
    // lease for exactly as long as the Arrow buffer or an FFI import references it.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, 8, allocation) };
    let array = Int64Array::new(ScalarBuffer::new(buffer, 0, 1), None);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?;
    let inner = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None)?;
    let plan = Arc::new(OwnedWorkerExec {
        inner,
        provider: Arc::new(EmptyTable::new(schema)),
        owner,
    });
    let ffi = FFI_ExecutionPlan::new(plan, Some(tokio::runtime::Handle::current()));
    // Force Arrow's foreign stream path, bypassing the same-library shortcut.
    let foreign = ForeignExecutionPlan::try_from(ffi)?;
    let mut stream = foreign.execute(0, Arc::new(TaskContext::default()))?;
    let retained = stream
        .next()
        .await
        .ok_or_else(|| py_error("missing output"))??;
    factory.close_job(&scope(1).job);
    Python::attach(|py| -> Result<()> {
        assert_eq!(
            observer
                .bind(py)
                .map_err(py_error)?
                .getattr("close_calls")
                .and_then(|value| value.extract::<usize>())
                .map_err(py_error)?,
            1,
            "CloseJob cancels the owner while the foreign plan and stream remain alive"
        );
        Ok(())
    })?;
    drop(observer);
    drop(stream);
    drop(foreign);
    assert_eq!(
        pool.reserved(),
        64,
        "retained Arrow output alone still owns the quota"
    );
    assert_eq!(
        retained
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| py_error("expected Int64 output"))?
            .value(0),
        17
    );
    drop(retained);
    assert_eq!(
        pool.reserved(),
        0,
        "final foreign Arrow release returns the worker quota"
    );
    Ok(())
}
