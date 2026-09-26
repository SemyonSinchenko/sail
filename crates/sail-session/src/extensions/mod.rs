//! Experimental, exact-build native packages for local Sail sessions.
//!
//! Python metadata is the bootstrap protocol; native objects use DataFusion's
//! named capsules. This is deliberately not a promise of a stable Sail C ABI.
mod manifest;
mod plan;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::catalog::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_common::{DataFusionError, Result, plan_err};
use datafusion_expr::ScalarUDF;
use datafusion_ffi::execution_plan::FFI_ExecutionPlan;
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_ffi::udf::FFI_ScalarUDF;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyCapsuleMethods, PyDict, PyList};
use sail_catalog::manager::CatalogManager;
use sail_common::config::ExecutionMode;
use sail_common_datafusion::connect_extension::{ConnectExtensionRegistry, ConnectRelationHandler};
use sail_plan::function::is_built_in_function_name;

use self::manifest::Manifest;
use self::plan::NativeTableProvider;
use sail_common_datafusion::native_scalar::{OwnedScalar, retain_scalar};

fn py_error(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Plan(format!("native extension: {error}"))
}

/// Loaded native code is process-lifetime; session state is never stored here.
/// This also protects release callbacks in Arrow arrays retained beyond a query.
fn retain_package(py: Python<'_>, identity: String, factory: &Bound<'_, PyAny>) -> Result<()> {
    static PACKAGES: OnceLock<Mutex<HashMap<String, Py<PyAny>>>> = OnceLock::new();
    let mut packages = PACKAGES
        .get_or_init(Mutex::default)
        .lock()
        .map_err(py_error)?;
    packages
        .entry(identity)
        .or_insert_with(|| factory.clone().unbind());
    let _ = py;
    Ok(())
}

struct PythonRelationHandler {
    owner: Arc<Py<PyAny>>,
    type_url: String,
    identity: String,
}

impl ConnectRelationHandler for PythonRelationHandler {
    fn plan(
        &self,
        payload: &[u8],
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn TableProvider>> {
        Python::attach(|py| {
            let capsules = PyList::empty(py);
            for input in inputs {
                let ffi = FFI_ExecutionPlan::new(input, tokio::runtime::Handle::try_current().ok());
                capsules
                    .append(
                        PyCapsule::new_with_value(py, ffi, c"datafusion_execution_plan")
                            .map_err(py_error)?,
                    )
                    .map_err(py_error)?;
            }
            let result = self
                .owner
                .bind(py)
                .call_method1(
                    "plan_relation",
                    (&self.type_url, PyBytes::new(py, payload), capsules),
                )
                .map_err(|e| py_error(format!("{}: {e}", self.identity)))?;
            let capsule = result.cast::<PyCapsule>().map_err(py_error)?;
            let pointer = capsule
                .pointer_checked(Some(c"datafusion_table_provider"))
                .map_err(py_error)?;
            // SAFETY: exact build metadata was checked before binding this handler;
            // the trusted package promises the named DataFusion capsule layout. No
            // Python code runs between obtaining the pointer and cloning its owner.
            let provider = unsafe { pointer.cast::<FFI_TableProvider>().as_ref().clone() };
            Ok(
                Arc::new(NativeTableProvider::new(Arc::<dyn TableProvider>::from(
                    &provider,
                ))) as Arc<dyn TableProvider>,
            )
        })
    }
}

/// All installation work happens on a fresh session. Validate every component
/// before mutating the session's catalog, so a failure cannot expose a partial
/// extension set to a client.
pub(crate) fn register_extensions(
    mut config: SessionConfig,
    mode: &ExecutionMode,
) -> Result<SessionConfig> {
    let mut registry = ConnectExtensionRegistry::new();
    if std::env::var("SAIL_EXPERIMENTAL_EXTENSIONS").as_deref() != Ok("1") {
        config.set_extension(Arc::new(registry));
        return Ok(config);
    }
    let distributed = !matches!(mode, ExecutionMode::Local);
    let catalog = config
        .get_extension::<CatalogManager>()
        .ok_or_else(|| py_error("session catalog is missing"))?;
    let scalars = Python::attach(|py| -> Result<Vec<ScalarUDF>> {
        let kwargs = PyDict::new(py);
        kwargs
            .set_item("group", "pysail.extensions")
            .map_err(py_error)?;
        let entries = py
            .import("importlib.metadata")
            .and_then(|m| m.getattr("entry_points"))
            .and_then(|f| f.call((), Some(&kwargs)))
            .map_err(py_error)?;
        let mut entries = entries
            .try_iter()
            .map_err(py_error)?
            .map(|item| {
                let item = item?;
                let name = item.getattr("name")?.extract::<String>()?;
                Ok((name, item))
            })
            .collect::<PyResult<Vec<_>>>()
            .map_err(py_error)?;
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let mut identities = HashSet::new();
        let mut names = HashSet::new();
        let mut scalars = Vec::new();
        // A fresh host-issued incarnation, not a client-selected graph namespace.
        let incarnation = uuid::Uuid::new_v4().to_string();
        for (entry_name, entry) in entries {
            let loaded = entry.call_method0("load").map_err(py_error)?;
            let factory = if loaded.is_callable() {
                loaded.call0().map_err(py_error)?
            } else {
                loaded
            };
            let metadata = factory.call_method0("manifest").map_err(py_error)?;
            let json = py
                .import("json")
                .and_then(|m| m.call_method1("dumps", (metadata,)))
                .and_then(|s| s.extract::<String>())
                .map_err(py_error)?;
            let manifest: Manifest = serde_json::from_str(&json).map_err(py_error)?;
            manifest.validate()?;
            if distributed && !manifest.relation_types.is_empty() {
                return plan_err!(
                    "extension {} exports relation handlers, which require distributed placement support",
                    manifest.name
                );
            }
            if !identities.insert(manifest.name.to_ascii_lowercase()) {
                return plan_err!(
                    "duplicate native extension name: {} (entry point {entry_name})",
                    manifest.name
                );
            }
            let identity = format!("{}@{}", manifest.name, manifest.version);
            retain_package(py, identity.clone(), &factory)?;
            let owner = Arc::new(
                factory
                    .call_method1("bind", (&incarnation,))
                    .map_err(py_error)?
                    .unbind(),
            );
            let functions = owner
                .bind(py)
                .call_method0("scalar_udfs")
                .map_err(py_error)?;
            for function in functions.try_iter().map_err(py_error)? {
                let function = function.map_err(py_error)?;
                if manifest.placement == "driver" {
                    return plan_err!(
                        "driver-only extension {} cannot export scalar functions",
                        manifest.name
                    );
                }
                let capsule = function
                    .call_method0("__datafusion_scalar_udf__")
                    .map_err(py_error)?;
                let capsule = capsule.cast::<PyCapsule>().map_err(py_error)?;
                let pointer = capsule
                    .pointer_checked(Some(c"datafusion_scalar_udf"))
                    .map_err(py_error)?;
                // SAFETY: see the provider import above; clone while capsule is live.
                let ffi = unsafe { pointer.cast::<FFI_ScalarUDF>().as_ref().clone() };
                let udf = ScalarUDF::new_from_shared_impl((&ffi).into());
                let mut aliases = vec![udf.name().to_ascii_lowercase()];
                aliases.extend(udf.aliases().iter().map(|s| s.to_ascii_lowercase()));
                aliases.sort();
                aliases.dedup();
                // Retain both the bound session and the exporting object.
                let owner: Arc<dyn std::any::Any + Send + Sync> = Arc::new(
                    (owner.bind(py), function)
                        .into_pyobject(py)
                        .map_err(py_error)?
                        .into_any()
                        .unbind(),
                );
                for name in aliases {
                    if is_built_in_function_name(&name)
                        || catalog.get_function(&name).map_err(py_error)?.is_some()
                        || !names.insert(name.clone())
                    {
                        return plan_err!(
                            "extension {} function name collision: {name}",
                            manifest.name
                        );
                    }
                    let scalar = ScalarUDF::new_from_impl(OwnedScalar {
                        name,
                        identity: identity.clone(),
                        udf: udf.clone(),
                        owner: Arc::clone(&owner),
                    });
                    retain_scalar(scalar.clone())?;
                    scalars.push(scalar);
                }
            }
            for relation in manifest.relation_types {
                registry.register(
                    relation.type_url.clone(),
                    relation.accepts_bare,
                    relation.min_inputs,
                    relation.max_inputs,
                    Arc::new(PythonRelationHandler {
                        owner: Arc::clone(&owner),
                        type_url: relation.type_url,
                        identity: identity.clone(),
                    }),
                )?;
            }
            log::info!("bound native extension {identity} to session incarnation {incarnation}");
        }
        Ok(scalars)
    })?;
    for udf in scalars {
        catalog.register_function(udf).map_err(py_error)?;
    }
    config.set_extension(Arc::new(registry));
    Ok(config)
}

/// Load scalar implementations on an execution worker before it decodes a
/// distributed physical plan. The worker does not install relation handlers or
/// mutate the driver catalog; the codec resolves native scalar descriptors from
/// the process-local registry populated here.
pub(crate) fn load_worker_extensions() -> Result<()> {
    Python::attach(|py| {
        let kwargs = PyDict::new(py);
        kwargs
            .set_item("group", "pysail.extensions")
            .map_err(py_error)?;
        let entries = py
            .import("importlib.metadata")
            .and_then(|m| m.getattr("entry_points"))
            .and_then(|f| f.call((), Some(&kwargs)))
            .map_err(py_error)?;
        for entry in entries.try_iter().map_err(py_error)? {
            let entry = entry.map_err(py_error)?;
            let loaded = entry.call_method0("load").map_err(py_error)?;
            let factory = if loaded.is_callable() {
                loaded.call0().map_err(py_error)?
            } else {
                loaded
            };
            let metadata = factory.call_method0("manifest").map_err(py_error)?;
            let json = py
                .import("json")
                .and_then(|m| m.call_method1("dumps", (metadata,)))
                .and_then(|s| s.extract::<String>())
                .map_err(py_error)?;
            let manifest: Manifest = serde_json::from_str(&json).map_err(py_error)?;
            manifest.validate()?;
            if manifest.placement == "driver" {
                continue;
            }
            let identity = format!("{}@{}", manifest.name, manifest.version);
            retain_package(py, identity.clone(), &factory)?;
            let owner = factory
                .call_method1("bind", (format!("worker-{identity}"),))
                .map_err(py_error)?;
            for function in owner
                .call_method0("scalar_udfs")
                .map_err(py_error)?
                .try_iter()
                .map_err(py_error)?
            {
                let function = function.map_err(py_error)?;
                let capsule = function
                    .call_method0("__datafusion_scalar_udf__")
                    .map_err(py_error)?;
                let capsule = capsule.cast::<PyCapsule>().map_err(py_error)?;
                let pointer = capsule
                    .pointer_checked(Some(c"datafusion_scalar_udf"))
                    .map_err(py_error)?;
                let ffi = unsafe { pointer.cast::<FFI_ScalarUDF>().as_ref().clone() };
                let udf = ScalarUDF::new_from_shared_impl((&ffi).into());
                let owner: Arc<dyn std::any::Any + Send + Sync> = Arc::new(
                    (owner.clone(), function)
                        .into_pyobject(py)
                        .map_err(py_error)?
                        .into_any()
                        .unbind(),
                );
                let mut names = vec![udf.name().to_string()];
                names.extend(udf.aliases().iter().cloned());
                names.sort_unstable();
                names.dedup();
                for name in names {
                    retain_scalar(ScalarUDF::new_from_impl(OwnedScalar {
                        name,
                        identity: identity.clone(),
                        udf: udf.clone(),
                        owner: Arc::clone(&owner),
                    }))?;
                }
            }
        }
        Ok(())
    })
}
