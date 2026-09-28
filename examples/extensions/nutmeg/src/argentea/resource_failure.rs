//! Preserve local admission refusal before a peer's cancellation reaches the RPC.
use super::error;
use datafusion_common::{DataFusionError, Result};
use sail_argentea_core::Resources;

/// The core currently returns string errors. Match its exact, locally produced
/// memory-budget error for this domain; never infer refusal from a generic
/// cancellation, a nested remote error, or the configured limit alone.
pub(super) fn details(
    resources: &Resources,
    failure: &DataFusionError,
) -> Result<Option<serde_json::Value>> {
    let limit = resources.execution.limits().memory_bytes;
    let expected = format!("argentea: procedure memory budget exceeded (limit {limit})");
    if !matches!(failure, DataFusionError::Execution(message) if message == &expected) {
        return Ok(None);
    }
    let usage = resources.execution.usage().map_err(error)?;
    Ok(Some(serde_json::json!({
        "code":"native_memory_budget", "outcome":"resource_refused", "memory_limit":limit,
        "live_bytes":usage.live_bytes, "peak_bytes":usage.peak_bytes,
        "usage_boundary":"accounted native reservations after failed call unwinds; not RSS or refused allocation size",
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, atomic::AtomicUsize};
    #[test]
    fn only_exact_local_memory_refusal_produces_causal_evidence() {
        let drops = Arc::new(AtomicUsize::new(0));
        let state = super::super::tests::worker(10, 1 << 20, &drops);
        let limit = state.resources.execution.limits().memory_bytes;
        let refused = state.resources.execution.reserve(limit).unwrap_err();
        let failure = error(refused);
        state.resources.execution.cancel().unwrap();
        let evidence = details(&state.resources, &failure).unwrap().unwrap();
        assert_eq!(evidence["code"], "native_memory_budget");
        assert_eq!(evidence["memory_limit"], limit);
        assert!(evidence["peak_bytes"].as_u64().unwrap() > 0);
        for message in [
            "procedure execution cancelled".to_string(),
            "operation cancelled while reading input".to_string(),
            format!("procedure memory budget exceeded (limit {})", limit - 1),
            format!("procedure work budget exceeded (limit {limit})"),
            format!("Execution error: argentea: procedure memory budget exceeded (limit {limit})"),
        ] {
            assert!(
                details(&state.resources, &error(message))
                    .unwrap()
                    .is_none()
            );
        }
    }
}
