//! One bounded native owner per host job and operation. No lock crosses await.
use std::collections::HashSet;
use std::fmt;
use std::io::Write;
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, Ordering},
};

use datafusion_common::Result;
use grust_procedures::{ExecutionContext, ExecutionLimits, MemoryAccount};
use sail_argentea_core::{Operation, PageRankPartition, Resources};
use sail_native_resource_ffi::MemoryLease;
use serde::Deserialize;

use super::{
    error,
    request::{Request, Verb},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Incarnation {
    pub session_id: String,
    pub job_id: u64,
    pub worker_id: u64,
    pub package_identity: String,
    pub operation_id: String,
}

pub type Partition = Arc<Mutex<PageRankPartition>>;

struct Registry {
    configured: Option<Request>,
    partitions: Vec<Option<Partition>>,
    claims: HashSet<(Verb, u64, usize)>,
    admission: MemoryAccount,
}

pub struct WorkerState {
    pub incarnation: Incarnation,
    pub resources: Resources,
    closed: AtomicBool,
    inner: Mutex<Registry>,
    audit: Mutex<Option<std::fs::File>>,
}

impl fmt::Debug for WorkerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArgenteaWorkerState")
            .field("incarnation", &self.incarnation)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

pub fn lock<T>(value: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    value.lock().map_err(|_| error("state lock poisoned"))
}

impl WorkerState {
    pub fn new(incarnation: Incarnation, bytes: usize, lease: MemoryLease) -> Result<Arc<Self>> {
        if incarnation.session_id.is_empty()
            || incarnation.session_id.len() > 200
            || incarnation.package_identity.is_empty()
            || incarnation.package_identity.len() > 256
            || incarnation.operation_id.is_empty()
            || incarnation.operation_id.len() > 128
            || incarnation.worker_id > i64::MAX as u64
        {
            return Err(error("invalid host incarnation"));
        }
        let execution = ExecutionContext::new(ExecutionLimits {
            memory_bytes: bytes,
            work_units: usize::MAX,
            batch_rows: 65_536,
            deadline: None,
        })
        .map_err(error)?;
        let resources = Resources::new(execution.clone(), lease).map_err(error)?;
        let mut admission = execution.memory_account();
        admission.charge(8192).map_err(error)?;
        let audit = std::env::var("SAIL_ARGENTEA_AUDIT_PATH")
            .ok()
            .map(|path| {
                let path = path.replace("{pid}", &std::process::id().to_string());
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .map_err(error)
            })
            .transpose()?;
        Ok(Arc::new(Self {
            incarnation,
            resources,
            closed: AtomicBool::new(false),
            inner: Mutex::new(Registry {
                configured: None,
                partitions: Vec::new(),
                claims: HashSet::new(),
                admission,
            }),
            audit: Mutex::new(audit),
        }))
    }

    pub fn check(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(error("operation is closed"));
        }
        self.resources.execution.checkpoint().map_err(error)
    }

    pub fn configure(&self, request: &Request) -> Result<Operation> {
        self.check()?;
        if request.operation_id != self.incarnation.operation_id {
            return Err(error("request operation does not match host scope"));
        }
        let mut registry = lock(&self.inner)?;
        self.check()?;
        if let Some(previous) = &registry.configured {
            if !request.compatible(previous) {
                return Err(error("operation parameters changed"));
            }
        } else {
            registry
                .admission
                .charge(request.partitions * 64 + 4096)
                .map_err(error)?;
            registry
                .partitions
                .try_reserve_exact(request.partitions)
                .map_err(error)?;
            registry.partitions.resize_with(request.partitions, || None);
            registry.configured = Some(request.clone());
        }
        let operation = Operation {
            package: self.incarnation.package_identity.clone(),
            session: format!(
                "{}:{}",
                self.incarnation.session_id, self.incarnation.job_id
            ),
            operation: request.operation_id.clone(),
            snapshot: request.snapshot_id.clone(),
            generation: request.generation,
            partitions: request.partitions,
            vertices: request.vertices,
        };
        operation.validate().map_err(error)?;
        Ok(operation)
    }

    pub fn claim(&self, request: &Request, partition: usize) -> Result<Operation> {
        let operation = self.configure(request)?;
        if partition >= operation.partitions {
            return Err(error("partition out of range"));
        }
        let mut registry = lock(&self.inner)?;
        self.check()?;
        let key = (request.verb, request.round, partition);
        if registry.claims.contains(&key) {
            return Err(error("partition stage replay is forbidden"));
        }
        registry.admission.charge(128).map_err(error)?;
        registry.claims.try_reserve(1).map_err(error)?;
        registry.claims.insert(key);
        Ok(operation)
    }

    pub fn partition(&self, partition: usize) -> Result<Option<Partition>> {
        self.check()?;
        Ok(lock(&self.inner)?
            .partitions
            .get(partition)
            .cloned()
            .flatten())
    }

    pub fn publish(&self, partition: PageRankPartition) -> Result<Partition> {
        let mut registry = lock(&self.inner)?;
        self.check()?;
        let index = partition.partition();
        let slot = registry
            .partitions
            .get_mut(index)
            .ok_or_else(|| error("unknown partition"))?;
        if slot.is_some() {
            return Err(error("native partition initialized twice"));
        }
        let partition = Arc::new(Mutex::new(partition));
        *slot = Some(partition.clone());
        Ok(partition)
    }

    pub fn audit(
        &self,
        event: &str,
        request: &Request,
        partition: usize,
        adjacency: u64,
    ) -> Result<()> {
        // A fixed small record, independent of graph cardinality. The owner's
        // conservative metadata reservation covers serialization scratch.
        let json = serde_json::to_string(&serde_json::json!({
            "event": event, "operation_id": request.operation_id,
            "snapshot_id": request.snapshot_id, "generation": request.generation,
            "round": request.round, "partition": partition,
            "worker_id": self.incarnation.worker_id, "pid": std::process::id(),
            "adjacency_id": adjacency, "job_id": self.incarnation.job_id,
            "session_id": self.incarnation.session_id,
        }))
        .map_err(error)?;
        self.audit_json(&json)
    }

    #[cfg(test)]
    pub fn test_audit_file(&self, file: std::fs::File) {
        *lock(&self.audit).unwrap() = Some(file);
    }

    pub fn audit_json(&self, json: &str) -> Result<()> {
        super::receipt::write_line(&mut std::io::stderr().lock(), "ARGENTEA_RECEIPT ", json)
            .map_err(error)?;
        if let Some(file) = lock(&self.audit)?.as_mut() {
            super::receipt::write_line(file, "", json).map_err(error)?;
            file.flush().map_err(error)?;
        }
        Ok(())
    }

    /// Tombstone before clearing state. Streams/Arrow arrays retain their own
    /// admitted owners. No in-flight publication can resurrect this operation.
    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let cancelled = self.resources.execution.cancel().map_err(error);
        let (request, partitions) = {
            let mut registry = lock(&self.inner)?;
            (
                registry.configured.clone(),
                std::mem::take(&mut registry.partitions),
            )
        };
        let mut first = cancelled.err();
        if let Some(mut request) = request {
            for partition in partitions.into_iter().flatten() {
                let result = lock(&partition).and_then(|part| {
                    request.round = part.next_round().saturating_sub(1);
                    self.audit(
                        "close",
                        &request,
                        part.partition(),
                        part.adjacency_identity(),
                    )
                });
                if first.is_none() {
                    first = result.err();
                }
            }
        }
        first.map_or(Ok(()), Err)
    }
}

pub struct ExecutionGuard {
    pub state: Arc<WorkerState>,
    pub complete: bool,
}
impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        if !self.complete {
            let _ = self.state.resources.execution.cancel();
        }
    }
}
