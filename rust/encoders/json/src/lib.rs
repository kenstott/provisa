//! REQ-2194: the JSON encoders, provisa._encoders.json: GraphQL, the REST (OpenAPI) table endpoint
//! (JSON and CSV), JSON:API and OData, one submodule each. Each takes a read's Arrow batches
//! (zero-copy, the GIL released) and the plan its router compiled, and writes the response body:
//! the nesting (one-to-many and many-to-one), the values on REQ-1867's exact-values contract
//! (a DECIMAL as a string of its digits, `infinity`, JSON only from json/jsonb columns, integers
//! by engine type, JSON numbers with their digits) and the surface's envelope. They share one value
//! model (`pyval`), conversion (`convert`) and writer (`pywrite`).

mod convert;
mod fmt;
mod json;
mod nest;
mod odata;
mod printable;
mod pywrite;
mod pyval;
mod surfaces;
mod text;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::pyarrow::PyArrowType;
use pyo3::create_exception;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};

use convert::Mode;
use nest::Col;
use pyval::{Error, Res};

create_exception!(json, EncodeError, PyValueError, "The answer cannot be written (a 500).");
create_exception!(json, ValueRefused, EncodeError, "JsonValueRefused(field, reason).");
create_exception!(json, ResponseRefused, EncodeError, "JsonResponseRefused(reason).");
create_exception!(json, ODataRefused, EncodeError, "QueryOptionRefused(status, code, message).");

fn py_err(e: Error) -> PyErr {
    match e {
        Error::Value(m) => EncodeError::new_err(m),
        Error::ValueRefused(f, r) => ValueRefused::new_err((f, r)),
        Error::ResponseRefused(r) => ResponseRefused::new_err((r,)),
        Error::ODataRefused(s, c, m) => ODataRefused::new_err((s, c, m)),
    }
}

/// One column: `(field_name, nested_in, cardinality, is_agg, json, json_text, integer)`.
type ColumnTuple = (String, Option<String>, Option<String>, bool, bool, Vec<String>, bool);

fn spec_of(columns: Vec<ColumnTuple>) -> Vec<Col> {
    columns
        .into_iter()
        .map(|(field, nested_in, card, is_agg, json, json_text, integer)| Col {
            field,
            nested_in,
            one_to_many: card.as_deref() == Some("one-to-many"),
            is_agg,
            mode: if json {
                Mode::Json(Arc::new(json_text.into_iter().map(String::into_bytes).collect::<HashSet<_>>()))
            } else if integer {
                Mode::Integer
            } else {
                Mode::Convert
            },
        })
        .collect()
}

fn run<'py>(py: Python<'py>, f: impl FnOnce() -> Res<Vec<u8>> + Send) -> PyResult<Bound<'py, PyBytes>> {
    match py.allow_threads(f) {
        Ok(b) => Ok(PyBytes::new(py, &b)),
        Err(e) => Err(py_err(e)),
    }
}

fn unwrap(batches: Vec<PyArrowType<RecordBatch>>) -> Vec<RecordBatch> {
    batches.into_iter().map(|b| b.0).collect()
}

/// `(rows, row count, {relationship: matched rows}, [truncated to-one path])`.
type FieldAnswer<'py> = (Bound<'py, PyBytes>, usize, HashMap<String, u64>, Vec<String>);

/// `graphql.encode_field(batches, columns, result_limit=None)`: a root field's rows, the JSON
/// array its answer holds (`{"data": {root_field: rows}}`), with what the rows said: how many,
/// each relationship's matched rows, and the to-one paths that matched several rows for a parent.
#[pyfunction]
#[pyo3(signature = (batches, columns, result_limit=None))]
fn encode_field<'py>(
    py: Python<'py>,
    batches: Vec<PyArrowType<RecordBatch>>,
    columns: Vec<ColumnTuple>,
    result_limit: Option<usize>,
) -> PyResult<FieldAnswer<'py>> {
    let batches = unwrap(batches);
    let spec = spec_of(columns);
    let r = py.allow_threads(|| {
        let b = nest::prepare(&batches, &spec)?;
        surfaces::graphql_field(&b, &spec, result_limit)
    });
    let (body, w) = r.map_err(py_err)?;
    Ok((PyBytes::new(py, &body), w.rows, w.joined.into_iter().collect(), w.truncated))
}

/// `graphql.encode_aggregate(batches, columns, aggregate_alias) -> bytes`: an aggregate root
/// field's answer, `{aggregate_alias: {...}, "nodes": [...]}`.
#[pyfunction]
fn encode_aggregate<'py>(
    py: Python<'py>,
    batches: Vec<PyArrowType<RecordBatch>>,
    columns: Vec<ColumnTuple>,
    aggregate_alias: String,
) -> PyResult<Bound<'py, PyBytes>> {
    let batches = unwrap(batches);
    let spec = spec_of(columns);
    run(py, || {
        let b = nest::prepare(&batches, &spec)?;
        surfaces::graphql_aggregate(&b, &spec, &aggregate_alias)
    })
}

/// `rest.encode(batches, columns, aggregate_alias=None) -> (bytes, [truncated path])`: the REST
/// table endpoint's `{"data": rows}` (a read or a group-by), or `{"data": aggregate}`.
#[pyfunction]
#[pyo3(signature = (batches, columns, aggregate_alias=None))]
fn encode_rest<'py>(
    py: Python<'py>,
    batches: Vec<PyArrowType<RecordBatch>>,
    columns: Vec<ColumnTuple>,
    aggregate_alias: Option<String>,
) -> PyResult<(Bound<'py, PyBytes>, Vec<String>)> {
    let batches = unwrap(batches);
    let spec = spec_of(columns);
    let r = py.allow_threads(|| {
        let b = nest::prepare(&batches, &spec)?;
        surfaces::rest(&b, &spec, aggregate_alias.as_deref())
    });
    let (body, w) = r.map_err(py_err)?;
    Ok((PyBytes::new(py, &body), w.truncated))
}

/// `rest.encode_csv(batches, columns) -> (bytes, [truncated path])`: the REST table endpoint's
/// `text/csv` answer.
#[pyfunction]
fn encode_rest_csv<'py>(
    py: Python<'py>,
    batches: Vec<PyArrowType<RecordBatch>>,
    columns: Vec<ColumnTuple>,
) -> PyResult<(Bound<'py, PyBytes>, Vec<String>)> {
    let batches = unwrap(batches);
    let spec = spec_of(columns);
    let r = py.allow_threads(|| {
        let b = nest::prepare(&batches, &spec)?;
        surfaces::rest_csv(&b, &spec)
    });
    let (body, truncated) = r.map_err(py_err)?;
    Ok((PyBytes::new(py, &body), truncated))
}

/// `jsonapi.encode(...) -> (bytes, [truncated path])`: a JSON:API table read's compound document;
/// `truncation` is `(code, message)` of the warning a truncated to-one path raises, `{path}` in
/// the message where its path goes.
#[pyfunction]
#[pyo3(signature = (
    batches, columns, *, resource_type, page_size, page_number, probe, total, id_columns,
    scalar_names, rel_names, rel_scalar_names, includes, include_keys, base_path, link_params,
    truncation, warnings=None
))]
#[allow(clippy::too_many_arguments)]
fn encode_jsonapi<'py>(
    py: Python<'py>,
    batches: Vec<PyArrowType<RecordBatch>>,
    columns: Vec<ColumnTuple>,
    resource_type: String,
    page_size: usize,
    page_number: usize,
    probe: bool,
    total: Option<i64>,
    id_columns: Vec<String>,
    scalar_names: HashMap<String, String>,
    rel_names: Vec<(String, String)>,
    rel_scalar_names: HashMap<String, HashMap<String, String>>,
    includes: Vec<String>,
    include_keys: HashMap<String, Vec<String>>,
    base_path: String,
    link_params: Vec<(String, String)>,
    truncation: (String, String),
    warnings: Option<Vec<u8>>,
) -> PyResult<(Bound<'py, PyBytes>, Vec<String>)> {
    let batches = unwrap(batches);
    let spec = spec_of(columns);
    let a = surfaces::JsonApi {
        resource_type,
        page_size,
        page_number,
        probe,
        total,
        id_columns,
        scalar_names,
        rel_names,
        rel_scalar_names,
        includes,
        include_keys,
        base_path,
        link_params,
        warnings,
        truncation,
    };
    let r = py.allow_threads(|| {
        let b = nest::prepare(&batches, &spec)?;
        surfaces::jsonapi(&b, &spec, &a)
    });
    let (body, truncated) = r.map_err(py_err)?;
    Ok((PyBytes::new(py, &body), truncated))
}

/// `jsonapi.encode_group_by(...) -> bytes`: JSON:API `?groupBy=`.
#[pyfunction]
#[pyo3(signature = (batches, columns, *, resource_type, scalar_names, rel_names, rel_scalar_names))]
fn encode_jsonapi_group_by<'py>(
    py: Python<'py>,
    batches: Vec<PyArrowType<RecordBatch>>,
    columns: Vec<ColumnTuple>,
    resource_type: String,
    scalar_names: HashMap<String, String>,
    rel_names: Vec<(String, String)>,
    rel_scalar_names: HashMap<String, HashMap<String, String>>,
) -> PyResult<Bound<'py, PyBytes>> {
    let batches = unwrap(batches);
    let spec = spec_of(columns);
    run(py, || {
        let b = nest::prepare(&batches, &spec)?;
        surfaces::jsonapi_group_by(&b, &spec, &resource_type, &scalar_names, &rel_names, &rel_scalar_names)
    })
}

/// `jsonapi.encode_aggregate(batches, columns, aggregate_alias) -> bytes`: JSON:API `?aggregate`.
#[pyfunction]
fn encode_jsonapi_aggregate<'py>(
    py: Python<'py>,
    batches: Vec<PyArrowType<RecordBatch>>,
    columns: Vec<ColumnTuple>,
    aggregate_alias: String,
) -> PyResult<Bound<'py, PyBytes>> {
    let batches = unwrap(batches);
    let spec = spec_of(columns);
    run(py, || {
        let b = nest::prepare(&batches, &spec)?;
        surfaces::jsonapi_aggregate(&b, &spec, &aggregate_alias)
    })
}

/// An OData entity level: `(set name, {property: edm name}, fields, [navigation])`; a
/// navigation: `(name, collection, page, parent_key, link_query, target entity)`.

fn entity_of(t: &Bound<'_, PyAny>) -> PyResult<odata::Entity> {
    let (name, types, fields, plans): (String, HashMap<String, String>, Vec<String>, Vec<Bound<'_, PyAny>>) = t.extract()?;
    let mut tm = HashMap::new();
    for (k, v) in types {
        tm.insert(k, odata::Edm::parse(&v).map_err(py_err)?);
    }
    let mut navs = Vec::new();
    for p in plans {
        let (n, collection, page, parent_key, link_query, target): (String, bool, usize, Vec<String>, String, Bound<'_, PyAny>) = p.extract()?;
        navs.push(odata::Nav { name: n, collection, page, parent_key, link_query, target: entity_of(&target)? });
    }
    Ok(odata::Entity::new(name, tm, fields, navs))
}

/// `odata.encode(batches, names, entity, *, context, count, page, next_link, root, warnings=None)`:
/// an entity set read's document.
#[pyfunction]
#[pyo3(signature = (batches, names, entity, *, context, count, page, next_link, root, warnings=None))]
#[allow(clippy::too_many_arguments)]
fn encode_odata<'py>(
    py: Python<'py>,
    batches: Vec<PyArrowType<RecordBatch>>,
    names: Vec<String>,
    entity: Bound<'py, PyAny>,
    context: String,
    count: Option<i64>,
    page: usize,
    next_link: String,
    root: String,
    warnings: Option<Vec<u8>>,
) -> PyResult<Bound<'py, PyBytes>> {
    let batches = unwrap(batches);
    let r = odata::Read { context, count, warnings, page, next_link, root, entity: entity_of(&entity)? };
    let spec: Vec<Col> = names
        .iter()
        .map(|n| Col { field: n.clone(), nested_in: None, one_to_many: false, is_agg: false, mode: Mode::Convert })
        .collect();
    run(py, || {
        let b = nest::prepare(&batches, &spec)?;
        odata::read(&b, &names, &r)
    })
}

/// `f` in `m` as `name`.
fn add_as<'py>(m: &Bound<'py, PyModule>, f: Bound<'py, pyo3::types::PyCFunction>, name: &str) -> PyResult<()> {
    m.add(name, f)
}

/// `odata.encode_navigation(batches, names, entity, *, context, name, root, warnings=None)`: a
/// navigation read's document, the first row's `name` list as its value.
#[pyfunction]
#[pyo3(signature = (batches, names, entity, *, context, name, root, warnings=None))]
#[allow(clippy::too_many_arguments)]
fn encode_odata_navigation<'py>(
    py: Python<'py>,
    batches: Vec<PyArrowType<RecordBatch>>,
    names: Vec<String>,
    entity: Bound<'py, PyAny>,
    context: String,
    name: String,
    root: String,
    warnings: Option<Vec<u8>>,
) -> PyResult<Bound<'py, PyBytes>> {
    let batches = unwrap(batches);
    let n = odata::Navigate { context, warnings, root, entity: entity_of(&entity)?, name };
    let spec: Vec<Col> = names
        .iter()
        .map(|n| Col { field: n.clone(), nested_in: None, one_to_many: false, is_agg: false, mode: Mode::Convert })
        .collect();
    run(py, || {
        let b = nest::prepare(&batches, &spec)?;
        odata::navigation(&b, &names, &n)
    })
}

/// A submodule of `parent` named `name`, registered in sys.modules under its qualified name: the
/// only place the import system finds an extension's submodules.
fn submodule<'py>(parent: &Bound<'py, PyModule>, name: &str) -> PyResult<Bound<'py, PyModule>> {
    let py = parent.py();
    let qualified = format!("{}.{}", parent.name()?, name);
    let m = PyModule::new(py, name)?;
    m.setattr("__name__", &qualified)?;
    parent.add(name, &m)?;
    py.import("sys")?.getattr("modules")?.set_item(&qualified, &m)?;
    Ok(m)
}

/// provisa._encoders.json, built under `qualified` (its name in sys.modules), with its submodules
/// graphql, rest, jsonapi and odata; rust/encoders registers it.
pub fn module<'py>(py: Python<'py>, qualified: &str) -> PyResult<Bound<'py, PyModule>> {
    let m = PyModule::new(py, "json")?;
    m.setattr("__name__", qualified)?;
    m.add("EncodeError", py.get_type::<EncodeError>())?;
    m.add("ValueRefused", py.get_type::<ValueRefused>())?;
    m.add("ResponseRefused", py.get_type::<ResponseRefused>())?;
    m.add("ODataRefused", py.get_type::<ODataRefused>())?;
    // Registered first, so each submodule's qualified name is the json module's.
    py.import("sys")?.getattr("modules")?.set_item(qualified, &m)?;
    let graphql = submodule(&m, "graphql")?;
    add_as(&graphql, wrap_pyfunction!(encode_field, &graphql)?, "encode_field")?;
    add_as(&graphql, wrap_pyfunction!(encode_aggregate, &graphql)?, "encode_aggregate")?;
    let rest = submodule(&m, "rest")?;
    add_as(&rest, wrap_pyfunction!(encode_rest, &rest)?, "encode")?;
    add_as(&rest, wrap_pyfunction!(encode_rest_csv, &rest)?, "encode_csv")?;
    let jsonapi = submodule(&m, "jsonapi")?;
    add_as(&jsonapi, wrap_pyfunction!(encode_jsonapi, &jsonapi)?, "encode")?;
    add_as(&jsonapi, wrap_pyfunction!(encode_jsonapi_group_by, &jsonapi)?, "encode_group_by")?;
    add_as(&jsonapi, wrap_pyfunction!(encode_jsonapi_aggregate, &jsonapi)?, "encode_aggregate")?;
    let odata = submodule(&m, "odata")?;
    add_as(&odata, wrap_pyfunction!(encode_odata, &odata)?, "encode")?;
    add_as(&odata, wrap_pyfunction!(encode_odata_navigation, &odata)?, "encode_navigation")?;
    Ok(m)
}
