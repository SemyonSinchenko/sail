//! Experimental extensions share admission with all same-config runtime envs
//! in one process, including other sessions and local in-process workers.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryPool, UnboundedMemoryPool,
};
use datafusion_common::{Result, internal_datafusion_err};
use sail_common::config::{FairMemoryPoolConfig, GreedyMemoryPoolConfig, MemoryPoolConfig};

type PoolKey = (u8, usize);
type Pools = HashMap<PoolKey, Weak<dyn MemoryPool>>;

fn key(config: &MemoryPoolConfig) -> PoolKey {
    match config {
        MemoryPoolConfig::Unbounded => (0, 0),
        MemoryPoolConfig::Greedy(GreedyMemoryPoolConfig { max_size }) => (1, *max_size),
        MemoryPoolConfig::Fair(FairMemoryPoolConfig { max_size }) => (2, *max_size),
    }
}

fn new_pool(config: &MemoryPoolConfig) -> Arc<dyn MemoryPool> {
    match config {
        MemoryPoolConfig::Unbounded => Arc::new(UnboundedMemoryPool::default()),
        MemoryPoolConfig::Greedy(GreedyMemoryPoolConfig { max_size }) => {
            Arc::new(GreedyMemoryPool::new(*max_size))
        }
        MemoryPoolConfig::Fair(FairMemoryPoolConfig { max_size }) => {
            Arc::new(FairSpillPool::new(*max_size))
        }
    }
}

pub(super) fn create_memory_pool(
    config: &MemoryPoolConfig,
    share_process_pool: bool,
) -> Result<Arc<dyn MemoryPool>> {
    if !share_process_pool {
        return Ok(new_pool(config));
    }
    static POOLS: OnceLock<Mutex<Pools>> = OnceLock::new();
    let mut pools = POOLS
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| internal_datafusion_err!("process memory pool registry poisoned"))?;
    pools.retain(|_, pool| pool.strong_count() > 0);
    let key = key(config);
    if let Some(pool) = pools.get(&key).and_then(Weak::upgrade) {
        return Ok(pool);
    }
    let pool = new_pool(config);
    pools.insert(key, Arc::downgrade(&pool));
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use datafusion::execution::memory_pool::MemoryConsumer;
    use sail_common_datafusion::native_resource::reserve_native_quota;

    use super::*;

    #[test]
    fn sessions_and_worker_runtime_envs_share_admission_only_when_enabled() -> Result<()> {
        let config = MemoryPoolConfig::Greedy(GreedyMemoryPoolConfig { max_size: 113 });
        let driver = create_memory_pool(&config, true)?;
        let other_session = create_memory_pool(&config, true)?;
        let worker = create_memory_pool(&config, true)?;
        assert!(Arc::ptr_eq(&driver, &other_session));
        assert!(Arc::ptr_eq(&driver, &worker));
        let a = reserve_native_quota(&driver, "session-a", 64)?;
        let b = reserve_native_quota(&other_session, "session-b", 32)?;
        let query = MemoryConsumer::new("worker join").register(&worker);
        assert!(query.try_grow(18).is_err());
        query.try_grow(17)?;
        assert_eq!(driver.reserved(), 113);
        drop(a);
        assert_eq!(other_session.reserved(), 49);
        query.try_grow(64)?;
        drop(b);
        drop(query);
        assert_eq!(driver.reserved(), 0);
        assert!(!Arc::ptr_eq(&driver, &create_memory_pool(&config, false)?));
        Ok(())
    }
}
