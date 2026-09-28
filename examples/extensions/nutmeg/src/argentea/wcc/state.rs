//! WCC partitions share the existing bound worker's resource domain.
use super::{
    super::{
        error,
        state::{WorkerState, lock},
    },
    request::{Request, Verb},
};
use datafusion_common::Result;
use grust_procedures::MemoryAccount;
use sail_argentea_core::{Operation, WccPartition};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};
pub type Partition = Arc<Mutex<WccPartition>>;
struct Registry {
    configured: Option<Request>,
    partitions: Vec<Option<Partition>>,
    claims: HashSet<(Verb, u64, usize)>,
    origins: Vec<Option<(i64, i64)>>,
    admission: MemoryAccount,
}
pub struct WccState {
    pub base: Arc<WorkerState>,
    inner: Mutex<Registry>,
}
impl std::fmt::Debug for WccState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArgenteaWccState")
            .field("incarnation", &self.base.incarnation)
            .finish_non_exhaustive()
    }
}
impl WccState {
    pub fn new(base: Arc<WorkerState>) -> Result<Arc<Self>> {
        base.check()?;
        let mut admission = base.resources.execution.memory_account();
        admission.charge(4096).map_err(error)?;
        Ok(Arc::new(Self {
            base,
            inner: Mutex::new(Registry {
                configured: None,
                partitions: vec![],
                claims: HashSet::new(),
                origins: vec![],
                admission,
            }),
        }))
    }
    pub fn configure(&self, request: &Request) -> Result<Operation> {
        self.base.check()?;
        if request.operation_id != self.base.incarnation.operation_id {
            return Err(error("v4 operation differs from host scope"));
        }
        let mut registry = lock(&self.inner)?;
        self.base.check()?;
        if let Some(previous) = &registry.configured {
            if !request.compatible(previous) {
                return Err(error("v4 operation parameters changed"));
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
            registry
                .origins
                .try_reserve_exact(request.partitions)
                .map_err(error)?;
            registry.origins.resize(request.partitions, None);
            registry.configured = Some(request.clone());
        }
        let operation = Operation {
            package: self.base.incarnation.package_identity.clone(),
            session: format!(
                "{}:{}",
                self.base.incarnation.session_id, self.base.incarnation.job_id
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
        if partition >= request.partitions {
            return Err(error("v4 partition out of range"));
        }
        let mut registry = lock(&self.inner)?;
        self.base.check()?;
        let key = (request.verb, request.phase, partition);
        if registry.claims.contains(&key) {
            return Err(error("v4 partition stage replay"));
        }
        registry.admission.charge(128).map_err(error)?;
        registry.claims.try_reserve(1).map_err(error)?;
        registry.claims.insert(key);
        Ok(operation)
    }
    // Tests inspect a retained snapshot after cancellation, when production
    // lookup correctly refuses further algorithm work.
    #[cfg(test)]
    pub(super) fn retained_partition_for_test(&self, p: usize) -> Option<Partition> {
        lock(&self.inner)
            .unwrap()
            .partitions
            .get(p)
            .cloned()
            .flatten()
    }
    pub fn partition(&self, p: usize) -> Result<Option<Partition>> {
        self.base.check()?;
        Ok(lock(&self.inner)?.partitions.get(p).cloned().flatten())
    }
    pub fn publish(&self, part: WccPartition) -> Result<Partition> {
        let mut registry = lock(&self.inner)?;
        self.base.check()?;
        let slot = registry
            .partitions
            .get_mut(part.partition())
            .ok_or_else(|| error("unknown v4 partition"))?;
        if slot.is_some() {
            return Err(error("v4 partition initialized twice"));
        }
        let part = Arc::new(Mutex::new(part));
        *slot = Some(part.clone());
        Ok(part)
    }
    pub fn origin(&self, producer: usize, origin: (i64, i64)) -> Result<()> {
        let mut registry = lock(&self.inner)?;
        self.base.check()?;
        let slot = registry
            .origins
            .get_mut(producer)
            .ok_or_else(|| error("unknown v4 producer"))?;
        if slot.is_some_and(|previous| previous != origin) {
            return Err(error("v4 producer origin changed across phases"));
        }
        *slot = Some(origin);
        Ok(())
    }
    pub fn audit(
        &self,
        event: &str,
        request: &Request,
        partition: usize,
        adjacency: u64,
        details: serde_json::Value,
    ) -> Result<()> {
        let mut value = serde_json::json!({"event":event,"protocol":4,"algorithm":request.algorithm,"operation_id":request.operation_id,"snapshot_id":request.snapshot_id,"generation":request.generation,"phase":request.phase,"partition":partition,"worker_id":self.base.incarnation.worker_id,"pid":std::process::id(),"adjacency_id":adjacency,"job_id":self.base.incarnation.job_id,"session_id":self.base.incarnation.session_id});
        if let Some(fields) = details.as_object() {
            value
                .as_object_mut()
                .expect("object")
                .extend(fields.clone());
        }
        self.base
            .audit_json(&serde_json::to_string(&value).map_err(error)?)
    }
    /// Best-effort causal logging must not replace the original execution error.
    /// Registry inspection remains available after the execution guard cancels.
    pub fn audit_resource_failure(
        &self,
        request: &Request,
        p: usize,
        failure: &datafusion_common::DataFusionError,
    ) -> Result<()> {
        let Some(mut details) =
            super::super::resource_failure::details(&self.base.resources, failure)?
        else {
            return Ok(());
        };
        let part = lock(&self.inner)?.partitions.get(p).cloned().flatten();
        let (adjacency, native_phase, rounds) = if let Some(part) = &part {
            let part = lock(part)?;
            (part.origin().adjacency_id, part.next_phase(), part.rounds())
        } else {
            (0, request.phase, 0)
        };
        details["initialized"] = part.is_some().into();
        details["native_phase"] = native_phase.into();
        details["rounds"] = rounds.into();
        self.audit("failure", request, p, adjacency, details)
    }

    // The bound owner closes/cancels the shared base first. Clearing this map
    // then cannot race a late configure/publication into resurrecting state.
    pub fn close(&self) -> Result<()> {
        let (request, parts) = {
            let mut registry = lock(&self.inner)?;
            (
                registry.configured.clone(),
                std::mem::take(&mut registry.partitions),
            )
        };
        let mut first = None;
        if let Some(mut request) = request {
            for part in parts.into_iter().flatten() {
                let result=lock(&part).and_then(|part|{request.phase=part.next_phase();self.audit("close",&request,part.partition(),part.origin().adjacency_id,serde_json::json!({"rounds":part.rounds(),"incoming_adjacency_id":part.incoming_identity().unwrap_or(0)}))});
                if first.is_none() {
                    first = result.err();
                }
            }
        }
        first.map_or(Ok(()), Err)
    }
}
