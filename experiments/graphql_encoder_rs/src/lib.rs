//! The GraphQL JSON response encoder: a root field's Arrow result to the bytes of
//! `{"data": {root_field: rows}}`, as `serialize_rows` nests the rows and `OrjsonResponse` encodes
//! them. An experiment; nothing in the product calls it.

mod cells;
mod fmt;
mod jtext;

use std::collections::HashMap;

use arrow::array::RecordBatch;
use arrow::pyarrow::PyArrowType;
use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use cells::{Cell, Keyer, Out, Res};

/// orjson refuses a container nested deeper than this.
const MAX_DEPTH: usize = 254;
const RECURSION: &str = "Recursion limit reached";

// ---------------------------------------------------------------- the selection plan

/// One column of the compiled query: `ColumnRef`'s field_name, nested_in, cardinality, is_agg.
#[derive(Clone)]
struct Col {
    field: String,
    nested_in: Option<String>,
    one_to_many: bool,
    is_agg: bool,
}

/// A dict built from columns, `{col.field_name: value ...}`: each distinct name at its first
/// column's position, holding its last column's value.
type Fields = Vec<(u32, usize)>;

struct Path {
    /// The path's own names relative to its parent's object, and its leaf last.
    parts: Vec<u32>,
    /// The nearest enclosing path (index into `paths`), None for the root object.
    parent: Option<usize>,
    cols: Vec<usize>,
    fields: Fields,
    one_to_many: bool,
    /// The full path split on dots (the flat path walks it from the root).
    full_parts: Vec<u32>,
}

struct Plan {
    names: Vec<Vec<u8>>, // each name's JSON key bytes, `"name":`
    root_cols: Vec<usize>,
    root_fields: Fields,
    /// In first-appearance order (`_group_columns`).
    paths: Vec<Path>,
    /// `paths` indices in sorted path order (`_serialize_with_one_to_many`).
    sorted: Vec<usize>,
    with_one_to_many: bool,
}

struct Interner {
    ids: HashMap<String, u32>,
    names: Vec<Vec<u8>>,
}

impl Interner {
    fn id(&mut self, s: &str) -> u32 {
        if let Some(&i) = self.ids.get(s) {
            return i;
        }
        let mut k = Vec::new();
        fmt::write_str(&mut k, s.as_bytes());
        k.push(b':');
        self.names.push(k);
        self.ids.insert(s.to_string(), self.names.len() as u32 - 1);
        self.names.len() as u32 - 1
    }
}

fn fields_of(cols: &[usize], spec: &[Col], it: &mut Interner) -> Fields {
    let mut f: Fields = Vec::new();
    for &c in cols {
        let id = it.id(&spec[c].field);
        match f.iter_mut().find(|e| e.0 == id) {
            Some(e) => e.1 = c,
            None => f.push((id, c)),
        }
    }
    f
}

fn plan(spec: &[Col]) -> Plan {
    let mut it = Interner { ids: HashMap::new(), names: Vec::new() };
    let mut root_cols = Vec::new();
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, c) in spec.iter().enumerate() {
        match &c.nested_in {
            None => root_cols.push(i),
            Some(p) => {
                if !groups.contains_key(p) {
                    order.push(p.clone());
                }
                groups.entry(p.clone()).or_default().push(i);
            }
        }
    }
    let root_fields = fields_of(&root_cols, spec, &mut it);
    let index: HashMap<&str, usize> = order.iter().enumerate().map(|(i, p)| (p.as_str(), i)).collect();
    let mut paths = Vec::new();
    for p in &order {
        let cols = groups[p].clone();
        // _parent_path: the nearest enclosing path placed in this row. Every path is placed in
        // every row, in sorted order, and a dotted prefix sorts before its extensions, so the
        // parent is the longest dotted prefix that is itself a path.
        let segs: Vec<&str> = p.split('.').collect();
        let mut parent = None;
        for n in (1..segs.len()).rev() {
            if let Some(&pi) = index.get(segs[..n].join(".").as_str()) {
                parent = Some((pi, n));
                break;
            }
        }
        let rel = match parent {
            Some((_, n)) => &segs[n..],
            None => &segs[..],
        };
        paths.push(Path {
            parts: rel.iter().map(|s| it.id(s)).collect(),
            parent: parent.map(|(pi, _)| pi),
            fields: fields_of(&cols, spec, &mut it),
            one_to_many: cols.iter().any(|&c| spec[c].one_to_many),
            cols,
            full_parts: segs.iter().map(|s| it.id(s)).collect(),
        });
    }
    let mut sorted: Vec<usize> = (0..order.len()).collect();
    sorted.sort_by(|&a, &b| order[a].as_bytes().cmp(order[b].as_bytes()));
    let with_one_to_many = paths.iter().any(|p| p.one_to_many) && !root_cols.is_empty();
    Plan { names: it.names, root_cols, root_fields, paths, sorted, with_one_to_many }
}

// ---------------------------------------------------------------- the response tree

#[derive(Clone, Copy)]
enum V {
    Null,
    Leaf { start: u64, len: u32, depth: u8 },
    Poison(&'static str),
    Obj(u32),
    Arr(u32),
}

/// Python dicts and lists as the serializer builds them: insertion-ordered objects.
struct Tree {
    buf: Vec<u8>,
    objs: Vec<Vec<(u32, V)>>,
    arrs: Vec<Vec<u32>>,
}

impl Tree {
    fn new() -> Self {
        Tree { buf: Vec::new(), objs: Vec::new(), arrs: Vec::new() }
    }
    fn clear(&mut self) {
        self.buf.clear();
        self.objs.clear();
        self.arrs.clear();
    }
    fn obj(&mut self) -> u32 {
        self.objs.push(Vec::new());
        self.objs.len() as u32 - 1
    }
    fn get(&self, o: u32, k: u32) -> Option<V> {
        self.objs[o as usize].iter().find(|e| e.0 == k).map(|e| e.1)
    }
    /// `d[k] = v`: a key already there keeps its position.
    fn set(&mut self, o: u32, k: u32, v: V) {
        let entries = &mut self.objs[o as usize];
        match entries.iter_mut().find(|e| e.0 == k) {
            Some(e) => e.1 = v,
            None => entries.push((k, v)),
        }
    }
    fn cell(&mut self, c: &dyn Cell, row: usize, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<V> {
        let start = self.buf.len();
        Ok(match c.write(row, &mut self.buf, key, k)? {
            Out::Null => V::Null,
            Out::Poison(p) => V::Poison(p),
            Out::Leaf(depth) => V::Leaf { start: start as u64, len: (self.buf.len() - start) as u32, depth },
        })
    }

    fn write_value(&self, v: V, names: &[Vec<u8>], out: &mut Vec<u8>, depth: usize) -> Res<()> {
        // `depth`: the nesting level of the container holding this value.
        match v {
            V::Null => out.extend_from_slice(b"null"),
            V::Poison(p) => return Err(p.to_string()),
            V::Leaf { start, len, depth: d } => {
                if depth + d as usize > MAX_DEPTH {
                    return Err(RECURSION.into());
                }
                out.extend_from_slice(&self.buf[start as usize..start as usize + len as usize]);
            }
            V::Obj(o) => self.write_obj(o, names, out, depth + 1)?,
            V::Arr(a) => {
                if depth + 1 > MAX_DEPTH {
                    return Err(RECURSION.into());
                }
                out.push(b'[');
                for (i, &o) in self.arrs[a as usize].iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    self.write_obj(o, names, out, depth + 2)?;
                }
                out.push(b']');
            }
        }
        Ok(())
    }

    fn write_obj(&self, o: u32, names: &[Vec<u8>], out: &mut Vec<u8>, depth: usize) -> Res<()> {
        if depth > MAX_DEPTH {
            return Err(RECURSION.into());
        }
        out.push(b'{');
        for (i, &(k, v)) in self.objs[o as usize].iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(&names[k as usize]);
            self.write_value(v, names, out, depth)?;
        }
        out.push(b'}');
        Ok(())
    }
}

/// The level of a row object in `{"data": {root: [row]}}`.
const ROW_DEPTH: usize = 4;

type Seen = hashbrown::HashMap<Box<[u8]>, u32>;

fn nesting_error(what: &str) -> String {
    format!("nesting failed: {what}")
}

// ---------------------------------------------------------------- the encoder

struct Batch {
    cells: Vec<Box<dyn Cell>>,
    rows: usize,
}

struct Encoder<'p> {
    p: &'p Plan,
    spec: &'p [Col],
    k: Keyer,
    key: Vec<u8>,
}

impl<'p> Encoder<'p> {
    /// `_serialize_flat`: each row whose key is new becomes a result row, built once and never
    /// changed, so it is written as soon as it is built.
    fn flat(&mut self, batches: &[Batch], limit: Option<usize>, out: &mut Vec<u8>) -> Res<()> {
        let p = self.p;
        let keyed: Vec<usize> = p.root_cols.iter().copied().filter(|&c| !self.spec[c].is_agg).collect();
        let key_cols: Vec<usize> = if keyed.is_empty() { (0..self.spec.len()).collect() } else { keyed };
        // root key -> whether `_detect_truncated_paths` fails on that row (a repeat raises)
        let mut seen: Seen = Seen::default();
        let mut t = Tree::new();
        let mut written = 0usize;
        for b in batches {
            for r in 0..b.rows {
                self.key.clear();
                for &c in &key_cols {
                    let mut scratch = std::mem::take(&mut t.buf);
                    b.cells[c].write(r, &mut scratch, Some(&mut self.key), &mut self.k)?;
                    scratch.clear();
                    t.buf = scratch;
                }
                if let Some(&fails) = seen.get(self.key.as_slice()) {
                    if fails != 0 {
                        return Err(nesting_error("a repeated row's relationship path is not an object"));
                    }
                    continue;
                }
                t.clear();
                let root = t.obj();
                for &(name, c) in &p.root_fields {
                    let v = t.cell(b.cells[c].as_ref(), r, None, &mut self.k)?;
                    t.set(root, name, v);
                }
                self.build_nested(&mut t, root, b, r)?;
                let fails = self.detect_fails(&t, root);
                seen.insert(self.key.as_slice().into(), fails as u32);
                if limit.is_none_or(|l| written < l) {
                    if written > 0 {
                        out.push(b',');
                    }
                    t.write_obj(root, &p.names, out, ROW_DEPTH)?;
                    written += 1;
                }
            }
        }
        Ok(())
    }

    /// `_build_nested_obj`.
    fn build_nested(&mut self, t: &mut Tree, root: u32, b: &Batch, r: usize) -> Res<()> {
        for path in &self.p.paths {
            let all_none = path.cols.iter().all(|&c| b.cells[c].is_null(r));
            let mut target = root;
            let mut skip = false;
            let (leaf, inner) = path.full_parts.split_last().unwrap();
            for &part in inner {
                let next = match t.get(target, part) {
                    None => {
                        let o = t.obj();
                        t.set(target, part, V::Obj(o));
                        V::Obj(o)
                    }
                    Some(v) => v,
                };
                match next {
                    V::Obj(o) => target = o,
                    V::Null => {
                        skip = true;
                        break;
                    }
                    _ => return Err(nesting_error("a relationship path runs through a value")),
                }
            }
            if skip {
                continue;
            }
            if all_none {
                t.set(target, *leaf, V::Null);
            } else {
                let o = t.obj();
                for &(name, c) in &path.fields {
                    let v = t.cell(self.cells_of(b, c), r, None, &mut self.k)?;
                    t.set(o, name, v);
                }
                t.set(target, *leaf, V::Obj(o));
            }
        }
        Ok(())
    }

    fn cells_of<'b>(&self, b: &'b Batch, c: usize) -> &'b dyn Cell {
        b.cells[c].as_ref()
    }

    /// Whether `_detect_truncated_paths` raises on a repeat of this row: it walks each path's
    /// parents with `[]` and reads its leaf with `.get`.
    fn detect_fails(&self, t: &Tree, root: u32) -> bool {
        for path in &self.p.paths {
            let (_, inner) = path.full_parts.split_last().unwrap();
            let mut target = root;
            for &part in inner {
                match t.get(target, part) {
                    Some(V::Obj(o)) => target = o,
                    _ => return true,
                }
            }
        }
        false
    }

    /// `_serialize_with_one_to_many`: rows merge into the result as they come, so the whole tree is
    /// kept and written at the end.
    fn one_to_many(&mut self, batches: &[Batch], limit: Option<usize>, out: &mut Vec<u8>) -> Res<()> {
        let p = self.p;
        let mut t = Tree::new();
        let mut seen: Seen = Seen::default();
        let mut rows: Vec<u32> = Vec::new();
        let mut elements: Seen = Seen::default();
        let mut placed: Vec<Option<u32>> = vec![None; p.paths.len()];
        let mut root_vals: Vec<V> = Vec::with_capacity(p.root_cols.len());
        for b in batches {
            for r in 0..b.rows {
                self.key.clear();
                root_vals.clear();
                let mark = t.buf.len();
                for &c in &p.root_cols {
                    let v = t.cell(b.cells[c].as_ref(), r, Some(&mut self.key), &mut self.k)?;
                    root_vals.push(v);
                }
                let root = match seen.get(self.key.as_slice()) {
                    Some(&o) => {
                        t.buf.truncate(mark);
                        o
                    }
                    None => {
                        let o = t.obj();
                        for &(name, c) in &p.root_fields {
                            let at = p.root_cols.iter().position(|&x| x == c).unwrap();
                            t.set(o, name, root_vals[at]);
                        }
                        seen.insert(self.key.as_slice().into(), o);
                        rows.push(o);
                        o
                    }
                };
                for &pi in &p.sorted {
                    let path = &p.paths[pi];
                    let holder = match path.parent {
                        None => Some(root),
                        Some(pp) => placed[pp],
                    };
                    let Some(mut holder) = holder else {
                        placed[pi] = None;
                        continue;
                    };
                    let (&leaf, inner) = path.parts.split_last().unwrap();
                    for &part in inner {
                        // holder.setdefault(part, {}), then a dict method on what it returns
                        match t.get(holder, part) {
                            None => {
                                let o = t.obj();
                                t.set(holder, part, V::Obj(o));
                                holder = o;
                            }
                            Some(V::Obj(o)) => holder = o,
                            Some(_) => return Err(nesting_error("a relationship path runs through a value")),
                        }
                    }
                    let empty = path.cols.iter().all(|&c| b.cells[c].is_null(r));
                    if path.one_to_many {
                        let arr = match t.get(holder, leaf) {
                            Some(V::Arr(a)) => a,
                            _ => {
                                t.arrs.push(Vec::new());
                                let a = t.arrs.len() as u32 - 1;
                                t.set(holder, leaf, V::Arr(a));
                                a
                            }
                        };
                        if empty {
                            placed[pi] = None;
                            continue;
                        }
                        self.key.clear();
                        self.key.extend_from_slice(&arr.to_le_bytes());
                        let mark = t.buf.len();
                        let mut vals = Vec::with_capacity(path.fields.len());
                        for &(name, c) in &path.fields {
                            let v = t.cell(b.cells[c].as_ref(), r, Some(&mut self.key), &mut self.k)?;
                            vals.push((name, v));
                        }
                        let el = match elements.get(self.key.as_slice()) {
                            Some(&o) => {
                                t.buf.truncate(mark);
                                o
                            }
                            None => {
                                let o = t.obj();
                                for (name, v) in vals {
                                    t.set(o, name, v);
                                }
                                elements.insert(self.key.as_slice().into(), o);
                                t.arrs[arr as usize].push(o);
                                o
                            }
                        };
                        placed[pi] = Some(el);
                    } else {
                        let current = t.get(holder, leaf);
                        if empty {
                            if current.is_none() {
                                t.set(holder, leaf, V::Null);
                            }
                            placed[pi] = None;
                            continue;
                        }
                        let o = match current {
                            Some(V::Obj(o)) => o,
                            _ => {
                                let o = t.obj();
                                t.set(holder, leaf, V::Obj(o));
                                o
                            }
                        };
                        for &(name, c) in &path.fields {
                            let v = t.cell(b.cells[c].as_ref(), r, None, &mut self.k)?;
                            t.set(o, name, v);
                        }
                        placed[pi] = Some(o);
                    }
                }
            }
        }
        let n = limit.map_or(rows.len(), |l| l.min(rows.len()));
        for (i, &o) in rows[..n].iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            t.write_obj(o, &p.names, out, ROW_DEPTH)?;
        }
        Ok(())
    }
}

fn encode(batches: &[RecordBatch], spec: &[Col], root_field: &str, limit: Option<usize>) -> Res<Vec<u8>> {
    let p = plan(spec);
    let mut prepared = Vec::with_capacity(batches.len());
    for rb in batches {
        if rb.num_columns() != spec.len() {
            return Err(format!("the batch has {} columns, the plan {}", rb.num_columns(), spec.len()));
        }
        let mut cells = Vec::with_capacity(spec.len());
        for (f, a) in rb.schema().fields().iter().zip(rb.columns()) {
            let c = cells::cell_for(f, a)?;
            c.validate()?;
            cells.push(c);
        }
        prepared.push(Batch { cells, rows: rb.num_rows() });
    }
    let mut out = Vec::new();
    out.extend_from_slice(b"{\"data\":{");
    fmt::write_str(&mut out, root_field.as_bytes());
    out.extend_from_slice(b":[");
    let mut e = Encoder { p: &p, spec, k: Keyer::new(), key: Vec::new() };
    if p.with_one_to_many {
        e.one_to_many(&prepared, limit, &mut out)?;
    } else {
        e.flat(&prepared, limit, &mut out)?;
    }
    out.extend_from_slice(b"]}}");
    Ok(out)
}

/// `encode_response(batches, columns, root_field, result_limit=None) -> bytes`: the body
/// `OrjsonResponse` renders for `{"data": {root_field: serialize_rows(...)}}`. `columns` holds one
/// `(field_name, nested_in, cardinality, is_agg)` per Arrow column, in order. The GIL is released
/// while encoding.
#[pyfunction]
#[pyo3(signature = (batches, columns, root_field, result_limit=None))]
fn encode_response<'py>(
    py: Python<'py>,
    batches: Vec<PyArrowType<RecordBatch>>,
    columns: Vec<(String, Option<String>, Option<String>, bool)>,
    root_field: String,
    result_limit: Option<usize>,
) -> PyResult<Bound<'py, PyBytes>> {
    let batches: Vec<RecordBatch> = batches.into_iter().map(|b| b.0).collect();
    let spec: Vec<Col> = columns
        .into_iter()
        .map(|(field, nested_in, card, is_agg)| Col {
            field,
            nested_in,
            one_to_many: card.as_deref() == Some("one-to-many"),
            is_agg,
        })
        .collect();
    let res = py.allow_threads(|| encode(&batches, &spec, &root_field, result_limit));
    match res {
        Ok(b) => Ok(PyBytes::new(py, &b)),
        Err(e) if e.starts_with("NotImplemented") => Err(PyNotImplementedError::new_err(e)),
        Err(e) => Err(PyValueError::new_err(e)),
    }
}

#[pymodule]
fn graphql_encoder_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_response, m)?)?;
    Ok(())
}
