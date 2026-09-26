use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_schema::{DataType, FieldRef};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use pyo3::{Py, PyAny};

/// Retains the Python owner and gives each catalog alias its own name. The
/// underlying kernel still receives the original argument/return field metadata.
#[derive(Debug)]
pub(super) struct OwnedScalar {
    pub name: String,
    pub identity: String,
    pub udf: ScalarUDF,
    pub owner: Arc<Py<PyAny>>,
}

impl PartialEq for OwnedScalar {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.identity == other.identity && self.udf == other.udf
    }
}
impl Eq for OwnedScalar {}
impl Hash for OwnedScalar {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.identity.hash(state);
        self.udf.hash(state);
    }
}

impl ScalarUDFImpl for OwnedScalar {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        self.udf.signature()
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.udf.return_type(types)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.udf.return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // The owner is held for every call and for as long as this UDF is planned.
        let _owner = &self.owner;
        self.udf.invoke_with_args(args)
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        self.udf.coerce_types(types)
    }
    fn short_circuits(&self) -> bool {
        self.udf.short_circuits()
    }
}
