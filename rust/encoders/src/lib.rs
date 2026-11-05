// REQ-2194: provisa._encoders, Provisa's compiled encoders as one extension module. Each encoder is
// a submodule: `from provisa._encoders import pgwire` (or `import provisa._encoders.pgwire`).

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyModule;

// Linked for the module inits below.
use grpc_encoder as _;
use pgwire_encoder as _;

extern "C" {
    // The init the pgwire source's #[pymodule] exports: its functions are private to it, so the
    // module it builds is the one provisa._encoders.pgwire serves.
    fn PyInit_pgwire_encoder() -> *mut ffi::PyObject;
    // The gRPC result encoder's (rust/encoders/grpc), served as provisa._encoders.grpc.
    fn PyInit_grpc_encoder() -> *mut ffi::PyObject;
}

/// Adds `module` to `parent` as `name`, also under its qualified name in sys.modules: that is the
/// only place the import system finds an extension's submodules.
fn add_encoder(
    parent: &Bound<'_, PyModule>,
    name: &str,
    module: Bound<'_, PyModule>,
) -> PyResult<()> {
    let qualified = format!("{}.{}", parent.name()?, name);
    module.setattr("__name__", &qualified)?;
    parent.add(name, &module)?;
    parent
        .py()
        .import("sys")?
        .getattr("modules")?
        .set_item(&qualified, &module)?;
    Ok(())
}

#[pymodule]
#[pyo3(name = "_encoders")]
fn encoders(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    // SAFETY: a module init returns a new reference, or NULL with the error set.
    let pgwire = unsafe { Bound::from_owned_ptr_or_err(py, PyInit_pgwire_encoder()) }?;
    add_encoder(m, "pgwire", pgwire.downcast_into::<PyModule>()?)?;
    // REQ-2194: the JSON encoders (rust/encoders/json), provisa._encoders.json.
    let json = json_encoder::module(py, &format!("{}.json", m.name()?))?;
    m.add("json", &json)?;
    // SAFETY: as above.
    let grpc = unsafe { Bound::from_owned_ptr_or_err(py, PyInit_grpc_encoder()) }?;
    add_encoder(m, "grpc", grpc.downcast_into::<PyModule>()?)?;
    Ok(())
}
