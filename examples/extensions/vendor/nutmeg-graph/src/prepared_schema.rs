//! Eager, bounded schema setup for native extension session binding.
use super::*;

/// Prepare the finite output-schema catalog before accepting user plans.
///
/// Grust owns the result types; Nutmeg observes each on its fixed three-node
/// probe graph. This setup runs once per library instance, under a separate
/// 16 MiB budget. Default schemas and the two supported rank precisions are
/// the entire key space. Session reads then use cache-only lookup.
pub fn prepare_output_schemas() -> Result<()> {
    static PREPARED: OnceCell<()> = OnceCell::new();
    PREPARED
        .get_or_try_init(|| {
            for definition in definitions() {
                let algorithm = short(definition);
                output_schema_at(algorithm, None)?;
                if definition
                    .options
                    .iter()
                    .any(|o| o.field.name == PRECISION_OPTION)
                {
                    for precision in ["f32", "f64"] {
                        output_schema_at(algorithm, Some(precision))?;
                    }
                }
            }
            Ok(())
        })
        .map(|_| ())
}

pub(super) fn cached_output_schema(
    algorithm: &str,
    args: &ValidatedArguments,
    names: ColumnNames,
) -> Result<SchemaRef> {
    let precision = precision_of(algorithm, args)?;
    let key = match precision.as_deref() {
        None => algorithm.to_owned(),
        Some(value @ ("f32" | "f64")) => format!("{algorithm}#{PRECISION_OPTION}={value}"),
        Some(other) => {
            return plan_err!(
                "nutmeg: unsupported output precision `{other}`; expected f32 or f64"
            );
        }
    };
    let schemas = SCHEMAS.read().map_err(|_| poisoned())?;
    let Some(schema) = schemas.get(&key) else {
        return plan_err!("nutmeg: schema `{key}` was not prepared during session binding");
    };
    names.rename_schema(schema)
}
