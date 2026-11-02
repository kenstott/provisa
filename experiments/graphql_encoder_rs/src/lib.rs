//! Rust encoders for the JSON surfaces: GraphQL, the REST (OpenAPI) rows endpoint, JSON:API and
//! OData. Each takes a read's Arrow batches (zero-copy, the GIL released) and the plan its router
//! compiled, and returns the response body the product's Python path writes for it: the same
//! nesting (`serialize_rows`, `serialize_aggregate`, OData's `expand.write`), the same values
//! (REQ-1867: exact decimals, `infinity`, JSON only from json/jsonb columns, integers by type) and
//! the same envelope. They share one value model (`pyval`), conversion (`convert`) and writer
//! (`pywrite`).

mod convert;
mod fmt;
mod json;
mod nest;
mod odata;
mod printable;
mod pytz_data;
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
use pyo3::types::PyBytes;

use convert::Mode;
use nest::Col;
use pyval::{Error, Res};

create_exception!(graphql_encoder_rs, EncodeError, PyValueError, "The product's path raises for this answer.");
create_exception!(graphql_encoder_rs, ValueRefused, EncodeError, "JsonValueRefused(field, reason).");
create_exception!(graphql_encoder_rs, ResponseRefused, EncodeError, "JsonResponseRefused(reason).");
create_exception!(graphql_encoder_rs, ODataRefused, EncodeError, "QueryOptionRefused(status, code, message).");

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

/// `encode_response(batches, columns, root_field, result_limit=None) -> bytes`: GraphQL's
/// `{"data": {root_field: serialize_rows(...)}}`.
#[pyfunction]
#[pyo3(signature = (batches, columns, root_field, result_limit=None))]
fn encode_response<'py>(
    py: Python<'py>,
    batches: Vec<PyArrowType<RecordBatch>>,
    columns: Vec<ColumnTuple>,
    root_field: String,
    result_limit: Option<usize>,
) -> PyResult<Bound<'py, PyBytes>> {
    let batches = unwrap(batches);
    let spec = spec_of(columns);
    run(py, || {
        let b = nest::prepare(&batches, &spec)?;
        surfaces::graphql(&b, &spec, &root_field, result_limit)
    })
}

/// `encode_rest(batches, columns, aggregate_alias=None) -> bytes`: the REST table endpoint's
/// `{"data": rows}` (a read or a group-by), or `{"data": aggregate}` for `?aggregate`.
#[pyfunction]
#[pyo3(signature = (batches, columns, aggregate_alias=None))]
fn encode_rest<'py>(
    py: Python<'py>,
    batches: Vec<PyArrowType<RecordBatch>>,
    columns: Vec<ColumnTuple>,
    aggregate_alias: Option<String>,
) -> PyResult<Bound<'py, PyBytes>> {
    let batches = unwrap(batches);
    let spec = spec_of(columns);
    run(py, || {
        let b = nest::prepare(&batches, &spec)?;
        surfaces::rest(&b, &spec, aggregate_alias.as_deref())
    })
}

/// `encode_jsonapi(...) -> bytes`: a JSON:API table read's compound document.
#[pyfunction]
#[pyo3(signature = (
    batches, columns, *, resource_type, page_size, page_number, probe, total, id_columns,
    scalar_names, rel_names, rel_scalar_names, includes, include_keys, base_path, link_params,
    warnings=None
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
    warnings: Option<Vec<u8>>,
) -> PyResult<Bound<'py, PyBytes>> {
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
    };
    run(py, || {
        let b = nest::prepare(&batches, &spec)?;
        surfaces::jsonapi(&b, &spec, &a)
    })
}

/// `encode_jsonapi_group_by(...) -> bytes`: JSON:API `?groupBy=`.
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

/// `encode_jsonapi_aggregate(batches, columns, aggregate_alias) -> bytes`: JSON:API `?aggregate`.
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

/// `encode_odata(batches, names, entity, *, context, count, warnings, page, next_link, root)`:
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

#[pymodule]
fn graphql_encoder_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_response, m)?)?;
    m.add_function(wrap_pyfunction!(encode_rest, m)?)?;
    m.add_function(wrap_pyfunction!(encode_jsonapi, m)?)?;
    m.add_function(wrap_pyfunction!(encode_jsonapi_group_by, m)?)?;
    m.add_function(wrap_pyfunction!(encode_jsonapi_aggregate, m)?)?;
    m.add_function(wrap_pyfunction!(encode_odata, m)?)?;
    // The pytz release whose zone tables localize aware timestamps (tools/gen_pytz.py).
    m.add("PYTZ_VERSION", pytz_data::VERSION)?;
    m.add("EncodeError", m.py().get_type::<EncodeError>())?;
    m.add("ValueRefused", m.py().get_type::<ValueRefused>())?;
    m.add("ResponseRefused", m.py().get_type::<ResponseRefused>())?;
    m.add("ODataRefused", m.py().get_type::<ODataRefused>())?;
    Ok(())
}
