use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_schema::Field;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use tokio::sync::Notify;

use super::*;

#[derive(Debug)]
struct Factory {
    calls: AtomicUsize,
    closes: AtomicUsize,
    partitions: usize,
    started: Notify,
    release: Notify,
    wait: bool,
    wrong_schema: bool,
}

impl Factory {
    fn new(partitions: usize) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            partitions,
            started: Notify::new(),
            release: Notify::new(),
            wait: false,
            wrong_schema: false,
        }
    }
}

#[async_trait]
impl WorkerExtensionFactory for Factory {
    async fn materialize(
        &self,
        descriptor: &WorkerDescriptor,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        scope: Arc<WorkerTaskScope>,
        _context: Arc<TaskContext>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if self.wait {
            self.release.notified().await;
        }
        if scope.worker_id != 7 || inputs.len() != descriptor.input_names.len() {
            return plan_err!("unexpected worker identity or inputs");
        }
        let schema = if self.wrong_schema {
            Schema::empty()
        } else {
            descriptor.output_schema.clone()
        };
        Ok(Arc::new(
            EmptyExec::new(Arc::new(schema)).with_partitions(self.partitions),
        ))
    }
    fn close_job(&self, _job: &WorkerJobIdentity) {
        self.closes.fetch_add(1, Ordering::SeqCst);
    }
}

fn descriptor() -> WorkerDescriptor {
    let schema = Schema::new(vec![Field::new("owner", DataType::Int64, true)]);
    WorkerDescriptor {
        package_identity: "fixture@1:sha256:abcd".into(),
        type_url: "type.example/worker.v1".into(),
        operation_id: "operation".into(),
        payload: vec![1, 2, 3],
        input_names: vec!["input0".into()],
        input_schemas: vec![schema.clone()],
        input_routing: vec![Some(WorkerInputRouting::IntegerRange {
            column: "owner".into(),
            split_points: vec![1],
        })],
        output_schema: schema,
        partitions: 2,
    }
}
fn inputs() -> Vec<Arc<dyn ExecutionPlan>> {
    vec![Arc::new(
        EmptyExec::new(Arc::new(descriptor().input_schemas[0].clone())).with_partitions(2),
    )]
}
fn scope() -> WorkerTaskScope {
    WorkerTaskScope {
        job: WorkerJobIdentity {
            session_id: "sail-session".into(),
            job_id: 1,
        },
        worker_id: 7,
        stage: 2,
        partition: 1,
        attempt: 0,
    }
}
fn context(
    registry: Arc<WorkerExtensionRegistry>,
    scope: Option<WorkerTaskScope>,
) -> Arc<TaskContext> {
    let mut config = SessionConfig::new().with_extension(registry);
    if let Some(scope) = scope {
        config = config.with_extension(Arc::new(scope));
    }
    SessionContext::new_with_config(config).task_ctx()
}

#[tokio::test]
async fn descriptor_round_trips_without_driver_state_and_executes_on_authoritative_scope()
-> Result<()> {
    let registry = Arc::new(WorkerExtensionRegistry::default());
    let factory = Arc::new(Factory::new(2));
    registry.register(descriptor().package_identity, factory.clone())?;
    let context = context(registry.clone(), Some(scope()));
    let exec = WorkerExtensionExec::new(descriptor(), inputs())?;
    let mut bytes = vec![];
    exec.encode(&mut bytes)?;
    let decoded = WorkerExtensionExec::decode(&bytes, &inputs(), &context)?;
    assert!(contains_worker_extension(&decoded));
    let mut output = decoded.execute(1, context.clone())?;
    assert_eq!(
        factory.calls.load(Ordering::SeqCst),
        0,
        "materialization must be lazy"
    );
    assert!(output.next().await.is_none());
    assert_eq!(factory.calls.load(Ordering::SeqCst), 1);
    assert!(decoded.execute(0, context).is_err());
    assert!(WorkerExtensionExec::decode(&bytes, &[], &TaskContext::default()).is_err());
    let absent = self::context(Arc::new(WorkerExtensionRegistry::default()), Some(scope()));
    assert!(WorkerExtensionExec::decode(&bytes, &inputs(), &absent).is_err());
    assert!(
        registry
            .register(descriptor().package_identity, factory)
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn worker_scope_and_first_attempt_are_required() -> Result<()> {
    let registry = Arc::new(WorkerExtensionRegistry::default());
    registry.register(descriptor().package_identity, Arc::new(Factory::new(2)))?;
    let exec = WorkerExtensionExec::new(descriptor(), inputs())?;
    assert!(exec.execute(1, context(registry.clone(), None)).is_err());
    let mut attempted = scope();
    attempted.attempt = 1;
    assert!(exec.execute(1, context(registry, Some(attempted))).is_err());
    Ok(())
}

#[test]
fn descriptor_rejects_wrong_identity_schemas_routes_and_oversized_payloads() -> Result<()> {
    for corruption in 0..7 {
        let mut descriptor = descriptor();
        match corruption {
            0 => descriptor.partitions = 0,
            1 => descriptor.input_names.push("input0".into()),
            2 => {
                descriptor.input_routing[0] = Some(WorkerInputRouting::IntegerRange {
                    column: "absent".into(),
                    split_points: vec![1],
                })
            }
            3 => {
                descriptor.input_routing[0] = Some(WorkerInputRouting::IntegerRange {
                    column: "owner".into(),
                    split_points: vec![1, 1],
                })
            }
            4 => descriptor.package_identity.clear(),
            5 => descriptor.payload.resize(262_145, 0),
            6 => {
                descriptor.input_schemas[0] =
                    Schema::new(vec![Field::new("owner", DataType::Utf8, false)])
            }
            _ => unreachable!(),
        }
        assert!(WorkerExtensionExec::new(descriptor, inputs()).is_err());
    }
    let registry = Arc::new(WorkerExtensionRegistry::default());
    registry.register(descriptor().package_identity, Arc::new(Factory::new(2)))?;
    let context = context(registry, Some(scope()));
    assert!(
        WorkerExtensionExec::decode(
            &vec![0; MAX_WORKER_DESCRIPTOR_BYTES + 1],
            &inputs(),
            &context
        )
        .is_err()
    );
    let mut bytes = WORKER_CODEC_PREFIX.to_vec();
    bytes.extend_from_slice(br#"{"unexpected":true}"#);
    assert!(WorkerExtensionExec::decode(&bytes, &inputs(), &context).is_err());
    let mut wrong = descriptor();
    wrong.package_identity = "fixture@1:different-digest".into();
    let exec = WorkerExtensionExec::new(wrong, inputs())?;
    let mut bytes = vec![];
    exec.encode(&mut bytes)?;
    assert!(WorkerExtensionExec::decode(&bytes, &inputs(), &context).is_err());
    Ok(())
}

#[tokio::test]
async fn advertised_output_schema_and_width_are_verified() -> Result<()> {
    for wrong_schema in [false, true] {
        let registry = Arc::new(WorkerExtensionRegistry::default());
        let mut factory = Factory::new(if wrong_schema { 2 } else { 1 });
        factory.wrong_schema = wrong_schema;
        registry.register(descriptor().package_identity, Arc::new(factory))?;
        let exec = WorkerExtensionExec::new(descriptor(), inputs())?;
        let mut output = exec.execute(1, context(registry, Some(scope())))?;
        assert!(output.next().await.is_some_and(|item| item.is_err()));
    }
    Ok(())
}

#[tokio::test]
async fn close_job_is_idempotent_and_rejects_late_preparation() -> Result<()> {
    let registry = Arc::new(WorkerExtensionRegistry::default());
    let mut factory = Factory::new(2);
    factory.wait = true;
    let factory = Arc::new(factory);
    registry.register(descriptor().package_identity, factory.clone())?;
    let exec = WorkerExtensionExec::new(descriptor(), inputs())?;
    let mut output = exec.execute(1, context(registry.clone(), Some(scope())))?;
    let pending = tokio::spawn(async move { output.next().await });
    factory.started.notified().await;
    registry.close_job(&scope().job)?;
    registry.close_job(&scope().job)?;
    assert_eq!(factory.closes.load(Ordering::SeqCst), 1);
    factory.release.notify_one();
    let output = pending.await.map_err(|e| plan_datafusion_err!("{e}"))?;
    assert!(output.is_some_and(|item| item.is_err()));
    assert!(
        exec.execute(1, context(registry.clone(), Some(scope())))
            .is_err()
    );
    assert_eq!(factory.calls.load(Ordering::SeqCst), 1);
    let unrelated = WorkerJobIdentity {
        job_id: 2,
        ..scope().job
    };
    assert!(registry.ensure_open(&unrelated).is_ok());
    Ok(())
}
