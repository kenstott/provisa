//! `serialize_rows` and `serialize_aggregate`: flat JOIN rows nested into GraphQL objects, kept as
//! a tree of Python-like dicts and lists until it is written.

use std::collections::HashMap;

use arrow::array::RecordBatch;

use crate::convert::{column_value, Mode};
use crate::fmt;
use crate::pyval::{err, Error, Reader, Res, P};
use crate::pywrite::{self, Keyer, MAX_DEPTH};

// ---------------------------------------------------------------- the selection plan

/// One column of the compiled query: `ColumnRef`'s field_name, nested_in, cardinality, is_agg,
/// and how its value is written (json / json_text, integer).
#[derive(Clone)]
pub struct Col {
    pub field: String,
    pub nested_in: Option<String>,
    pub one_to_many: bool,
    pub is_agg: bool,
    pub mode: Mode,
}

/// A dict built from columns, `{col.field_name: value ...}`: each distinct name at its first
/// column's position, holding its last column's value.
type Fields = Vec<(u32, usize)>;

pub struct Path {
    /// The path as the columns name it (`nested_in`).
    pub name: String,
    /// The path's own names relative to its parent's object, its leaf last.
    pub parts: Vec<u32>,
    /// The nearest enclosing path (index into `paths`), None for the root object.
    pub parent: Option<usize>,
    pub cols: Vec<usize>,
    pub fields: Fields,
    pub one_to_many: bool,
    /// The full path split on dots (the flat path walks it from the root).
    pub full_parts: Vec<u32>,
}

pub struct Plan {
    pub names: Interner,
    pub root_cols: Vec<usize>,
    /// In first-appearance order (`_group_columns`).
    pub paths: Vec<Path>,
    /// `paths` indices in sorted path order (`_serialize_with_one_to_many`).
    pub sorted: Vec<usize>,
    pub with_one_to_many: bool,
    /// Each column's field name.
    pub col_name: Vec<u32>,
}

/// Object keys: each name's JSON bytes, `"name":`.
#[derive(Clone, Default)]
pub struct Interner {
    ids: HashMap<Vec<u8>, u32>,
    pub raw: Vec<Vec<u8>>,
    pub names: Vec<Vec<u8>>,
    pub bad: Vec<bool>,
}

impl Interner {
    pub fn id(&mut self, s: &str) -> u32 {
        self.id_bytes(s.as_bytes())
    }
    pub fn id_bytes(&mut self, s: &[u8]) -> u32 {
        if let Some(&i) = self.ids.get(s) {
            return i;
        }
        let mut k = Vec::new();
        fmt::write_str(&mut k, s);
        k.push(b':');
        self.names.push(k);
        self.raw.push(s.to_vec());
        self.bad.push(crate::json::has_surrogate(s));
        self.ids.insert(s.to_vec(), self.names.len() as u32 - 1);
        self.names.len() as u32 - 1
    }
    pub fn get(&self, s: &[u8]) -> Option<u32> {
        self.ids.get(s).copied()
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

pub fn plan(spec: &[Col]) -> Plan {
    let mut it = Interner::default();
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
    let index: HashMap<&str, usize> = order.iter().enumerate().map(|(i, p)| (p.as_str(), i)).collect();
    let mut paths = Vec::new();
    for p in &order {
        let cols = groups[p].clone();
        // _parent_path: every path is placed in every row, in sorted order, and a dotted prefix
        // sorts before its extensions, so the parent is the longest dotted prefix that is a path.
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
            name: p.clone(),
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
    let col_name = spec.iter().map(|c| it.id(&c.field)).collect();
    Plan { names: it, root_cols, paths, sorted, with_one_to_many, col_name }
}

// ---------------------------------------------------------------- the response tree

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum LeafKind {
    Str,
    Int,
    Bool,
}

#[derive(Clone, Copy, Debug)]
pub enum V {
    Null,
    /// A scalar's JSON bytes in `Tree::buf`.
    Leaf { start: u64, len: u32, kind: LeafKind },
    Float(f64),
    Obj(u32),
    Arr(u32),
    /// A value held as Python holds it (`Tree::jv`): a dict or list becomes an `Obj` or `Arr` the
    /// moment the serializer reaches into it.
    Json(u32),
}

/// Python dicts and lists as the serializer builds them: insertion-ordered objects.
pub struct Tree {
    pub buf: Vec<u8>,
    scratch: Vec<u8>,
    pub objs: Vec<Vec<(u32, V)>>,
    pub arrs: Vec<Vec<V>>,
    pub jv: Vec<P<'static>>,
    pub names: Interner,
}

impl Tree {
    pub fn new(names: &Interner) -> Self {
        Tree { buf: Vec::new(), scratch: Vec::new(), objs: Vec::new(), arrs: Vec::new(), jv: Vec::new(), names: names.clone() }
    }
    pub fn clear(&mut self) {
        self.buf.clear();
        self.objs.clear();
        self.arrs.clear();
        self.jv.clear();
    }
    pub fn obj(&mut self) -> u32 {
        self.objs.push(Vec::new());
        self.objs.len() as u32 - 1
    }
    pub fn arr(&mut self) -> u32 {
        self.arrs.push(Vec::new());
        self.arrs.len() as u32 - 1
    }
    pub fn json(&mut self, j: P<'static>) -> V {
        self.jv.push(j);
        V::Json(self.jv.len() as u32 - 1)
    }
    /// A V for a value of `p`'s kind (a scalar's JSON bytes, a container held as it is).
    pub fn value(&mut self, p: P<'_>) -> Res<V> {
        Ok(match p {
            P::None => V::Null,
            P::Float(f) => V::Float(f),
            P::Str(s) => self.leaf(LeafKind::Str, |b| pywrite::write_text(b, &s))?,
            P::Int(i) => self.leaf(LeafKind::Int, |b| {
                b.extend_from_slice(itoa::Buffer::new().format(i).as_bytes());
                Ok(())
            })?,
            P::Bool(x) => self.leaf(LeafKind::Bool, |b| {
                b.extend_from_slice(if x { b"true" } else { b"false" });
                Ok(())
            })?,
            other => self.json(other.into_owned()),
        })
    }
    fn leaf(&mut self, kind: LeafKind, f: impl FnOnce(&mut Vec<u8>) -> Res<()>) -> Res<V> {
        let start = self.buf.len();
        f(&mut self.buf)?;
        Ok(V::Leaf { start: start as u64, len: (self.buf.len() - start) as u32, kind })
    }
    /// `d.get(k)`, a dict or list held there made a node first (it is the same object).
    pub fn get(&mut self, o: u32, k: u32) -> Option<V> {
        let pos = self.objs[o as usize].iter().position(|e| e.0 == k)?;
        let v = self.objs[o as usize][pos].1;
        let v = self.live(v);
        self.objs[o as usize][pos].1 = v;
        Some(v)
    }
    /// A held dict or list as a node.
    pub fn live(&mut self, v: V) -> V {
        if let V::Json(i) = v {
            if matches!(self.jv[i as usize], P::Dict(_) | P::List(_)) {
                let j = std::mem::replace(&mut self.jv[i as usize], P::None);
                return self.node(j);
            }
        }
        v
    }
    fn node(&mut self, j: P<'static>) -> V {
        match j {
            P::Dict(entries) => {
                let o = self.obj();
                for (k, v) in entries {
                    let id = self.names.id_bytes(&k);
                    let v = self.json(v);
                    self.objs[o as usize].push((id, v));
                }
                V::Obj(o)
            }
            P::List(items) => {
                let a = self.arr();
                for v in items {
                    let v = self.json(v);
                    self.arrs[a as usize].push(v);
                }
                V::Arr(a)
            }
            j => self.json(j),
        }
    }
    /// `d[k] = v`: a key already there keeps its position.
    pub fn set(&mut self, o: u32, k: u32, v: V) {
        let entries = &mut self.objs[o as usize];
        match entries.iter_mut().find(|e| e.0 == k) {
            Some(e) => e.1 = v,
            None => entries.push((k, v)),
        }
    }
    pub fn cell(&mut self, c: &Cell, row: usize, k: &mut Keyer) -> Res<V> {
        let _ = k;
        let mut scratch = std::mem::take(&mut self.scratch);
        let fast = c.fast(row, &mut scratch);
        let v = match fast {
            Some(f) => match f? {
                Fast::Null => V::Null,
                Fast::Float(x) => V::Float(x),
                Fast::Int(i) => self.leaf(LeafKind::Int, |b| {
                    b.extend_from_slice(itoa::Buffer::new().format(i).as_bytes());
                    Ok(())
                })?,
                Fast::Bool(x) => self.leaf(LeafKind::Bool, |b| {
                    b.extend_from_slice(if x { b"true" } else { b"false" });
                    Ok(())
                })?,
                Fast::Text(t) => self.leaf(LeafKind::Str, |b| pywrite::write_text(b, t))?,
                Fast::Scratch => self.leaf(LeafKind::Str, |b| pywrite::write_text(b, &scratch))?,
            },
            None => {
                let p = c.value(row)?;
                self.value(p)?
            }
        };
        self.scratch = scratch;
        Ok(v)
    }

    /// The JSON of `v`; `depth` is the nesting level of the container holding it.
    pub fn write_value(&self, v: V, out: &mut Vec<u8>, depth: usize) -> Res<()> {
        match v {
            V::Null => out.extend_from_slice(b"null"),
            V::Leaf { start, len, .. } => out.extend_from_slice(&self.buf[start as usize..start as usize + len as usize]),
            V::Float(f) => fmt::write_f64(out, f),
            V::Json(i) => pywrite::write(out, &self.jv[i as usize], depth)?,
            V::Obj(o) => self.write_obj(o, out, depth + 1)?,
            V::Arr(a) => {
                if depth + 1 > MAX_DEPTH && !self.arrs[a as usize].is_empty() {
                    return Err(pywrite::recursion());
                }
                out.push(b'[');
                for (i, &e) in self.arrs[a as usize].iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    self.write_value(e, out, depth + 1)?;
                }
                out.push(b']');
            }
        }
        Ok(())
    }

    /// An object at nesting level `depth`.
    pub fn write_obj(&self, o: u32, out: &mut Vec<u8>, depth: usize) -> Res<()> {
        self.write_entries(&self.objs[o as usize], out, depth)
    }

    pub fn write_entries(&self, entries: &[(u32, V)], out: &mut Vec<u8>, depth: usize) -> Res<()> {
        if depth > MAX_DEPTH && !entries.is_empty() {
            return Err(pywrite::recursion());
        }
        out.push(b'{');
        for (i, &(k, v)) in entries.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            if self.names.bad[k as usize] {
                return Err(pywrite::surrogate());
            }
            out.extend_from_slice(&self.names.names[k as usize]);
            self.write_value(v, out, depth)?;
        }
        out.push(b'}');
        Ok(())
    }

    /// `str(v)`.
    pub fn py_str(&self, v: V, out: &mut Vec<u8>) {
        match v {
            V::Null => out.extend_from_slice(b"None"),
            V::Leaf { start, len, kind } => {
                let b = &self.buf[start as usize..start as usize + len as usize];
                match kind {
                    LeafKind::Int => out.extend_from_slice(b),
                    LeafKind::Bool => out.extend_from_slice(if b == b"true" { b"True" } else { b"False" }),
                    LeafKind::Str => unescape(b, out),
                }
            }
            V::Float(f) => crate::text::float_repr(out, f),
            V::Json(i) => pywrite::py_str(out, &self.jv[i as usize]),
            V::Obj(_) | V::Arr(_) => pywrite::py_repr(out, &self.to_p(v)),
        }
    }

    /// The Python value a node holds (for its repr).
    pub fn to_p(&self, v: V) -> P<'static> {
        match v {
            V::Null => P::None,
            V::Leaf { start, len, kind } => {
                let b = &self.buf[start as usize..start as usize + len as usize];
                match kind {
                    LeafKind::Int => P::Int(std::str::from_utf8(b).unwrap().parse().unwrap()),
                    LeafKind::Bool => P::Bool(b == b"true"),
                    LeafKind::Str => {
                        let mut t = Vec::new();
                        unescape(b, &mut t);
                        P::Str(t.into())
                    }
                }
            }
            V::Float(f) => P::Float(f),
            V::Json(i) => self.jv[i as usize].clone(),
            V::Obj(o) => P::Dict(self.objs[o as usize].iter().map(|&(k, x)| (self.names.raw[k as usize].clone().into(), self.to_p(x))).collect()),
            V::Arr(a) => P::List(self.arrs[a as usize].iter().map(|&x| self.to_p(x)).collect()),
        }
    }
}

/// A JSON string literal's text (orjson's escapes).
pub fn unescape(b: &[u8], out: &mut Vec<u8>) {
    let s = &b[1..b.len() - 1];
    let mut i = 0;
    while i < s.len() {
        if s[i] == b'\\' {
            match s[i + 1] {
                b'"' => out.push(b'"'),
                b'\\' => out.push(b'\\'),
                b'b' => out.push(8),
                b'f' => out.push(12),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                b'u' => {
                    let h = std::str::from_utf8(&s[i + 2..i + 6]).unwrap();
                    out.push(u8::from_str_radix(h, 16).unwrap());
                    i += 4;
                }
                c => out.push(c),
            }
            i += 2;
        } else {
            out.push(s[i]);
            i += 1;
        }
    }
}

type Seen = hashbrown::HashMap<Box<[u8]>, u32>;

fn nesting_error(what: &str) -> Error {
    Error::Value(format!("nesting failed: {what}"))
}

// ---------------------------------------------------------------- a result column

/// A result column: its reader and how its values are written.
pub struct Cell {
    pub r: Reader,
    pub mode: Mode,
    pub field: String,
    scratch: std::cell::RefCell<Vec<u8>>,
    /// The last JSON text keyed and its key atom: equal text, equal parse, equal key.
    last: std::cell::RefCell<(Vec<u8>, Vec<u8>)>,
}

impl Cell {
    pub fn new(r: Reader, mode: Mode, field: String) -> Cell {
        Cell { r, mode, field, scratch: Default::default(), last: Default::default() }
    }
    pub fn is_null(&self, row: usize) -> bool {
        self.r.is_null(row)
    }
    /// `column_value(row[idx], col)`.
    pub fn value(&self, row: usize) -> Res<P<'_>> {
        column_value(self.r.get(row)?, &self.mode, &self.field)
    }
    /// A scalar of a common column type converted without building its Python value (text in
    /// `scratch` for `Fast::Scratch`); None for any other column.
    pub fn fast<'s>(&'s self, row: usize, scratch: &mut Vec<u8>) -> Option<Res<Fast<'s>>> {
        if matches!(self.mode, Mode::Json(_)) {
            return None;
        }
        let convert = matches!(self.mode, Mode::Convert);
        if self.r.is_null(row) {
            return Some(Ok(Fast::Null));
        }
        Some(Ok(match &self.r {
            Reader::Int(a) => Fast::Int(a.value(row) as i128),
            Reader::UInt(a) => Fast::Int(a.value(row) as i128),
            Reader::Bool(a) => Fast::Bool(a.value(row)),
            Reader::Float(a) => Fast::Float(a.value(row)),
            Reader::Utf8(a) => Fast::Text(a.value(row).as_bytes()),
            Reader::LUtf8(a) => Fast::Text(a.value(row).as_bytes()),
            Reader::VUtf8(a) => Fast::Text(a.value(row).as_bytes()),
            Reader::Dec128(a, sc) if convert => {
                let c = a.value(row);
                scratch.clear();
                let mut b = itoa::Buffer::new();
                let d = crate::pyval::Dec { neg: c < 0, coef: std::borrow::Cow::Borrowed(b.format(c.unsigned_abs())), exp: -*sc };
                crate::text::decimal_f(scratch, &d);
                Fast::Scratch
            }
            Reader::Date32(..) | Reader::Ts(_, crate::pyval::Unit::S | crate::pyval::Unit::Ms | crate::pyval::Unit::Us, _, _) if convert => {
                let p = match self.r.get(row) {
                    Ok(p) => p,
                    Err(e) => return Some(Err(e)),
                };
                scratch.clear();
                pywrite::temporal_text(scratch, &p);
                Fast::Scratch
            }
            _ => return None,
        }))
    }

    /// `_to_hashable(column_value(...))`, appended to `key`.
    pub fn key(&self, row: usize, key: &mut Vec<u8>, k: &mut Keyer) -> Res<()> {
        let mut scratch = std::mem::take(&mut *self.scratch.borrow_mut());
        let fast = self.fast(row, &mut scratch);
        if let Some(f) = fast {
            match f? {
                Fast::Null => key.push(pywrite::K_NULL),
                Fast::Int(i) => k.int_atom(key, i),
                Fast::Bool(b) => k.bool_atom(key, b),
                Fast::Float(x) => k.float_atom(key, x),
                Fast::Text(t) => k.str_atom(key, t),
                Fast::Scratch => k.str_atom(key, &scratch),
            }
            *self.scratch.borrow_mut() = scratch;
            return Ok(());
        }
        *self.scratch.borrow_mut() = scratch;
        if let Mode::Json(_) = self.mode {
            if let P::Str(s) = self.r.get(row)? {
                let mut last = self.last.borrow_mut();
                if last.1.is_empty() || last.0 != *s {
                    let mut atom = Vec::new();
                    let v = column_value(P::Str(s.clone()), &self.mode, &self.field)?;
                    k.atom(&mut atom, &v);
                    last.0.clear();
                    last.0.extend_from_slice(&s);
                    last.1 = atom;
                }
                key.extend_from_slice(&last.1);
                return Ok(());
            }
        }
        let v = self.value(row)?;
        k.atom(key, &v);
        Ok(())
    }
}

/// A scalar converted without a Python value.
pub enum Fast<'a> {
    Null,
    Text(&'a [u8]),
    /// Text left in the caller's scratch buffer.
    Scratch,
    Int(i128),
    Bool(bool),
    Float(f64),
}

pub struct Batch {
    pub cells: Vec<Cell>,
    pub rows: usize,
}

/// The batches' cells, each column read as `column_values` reads it (its eager errors raised).
pub fn prepare(batches: &[RecordBatch], spec: &[Col]) -> Res<Vec<Batch>> {
    let mut prepared = Vec::with_capacity(batches.len());
    for rb in batches {
        // The plan's columns are the statement's first (a column the statement carries after
        // them, as the serializer read rows by position, is not the answer's).
        if rb.num_columns() < spec.len() {
            return err(format!("the batch has {} columns, the plan {}", rb.num_columns(), spec.len()));
        }
        if rb.num_rows() == 0 {
            continue;
        }
        let mut cells = Vec::with_capacity(spec.len());
        for ((f, a), c) in rb.schema().fields().iter().zip(rb.columns()).zip(spec) {
            let r = Reader::new(f, a, true)?;
            r.validate(rb.num_rows())?;
            cells.push(Cell::new(r, c.mode.clone(), c.field.clone()));
        }
        prepared.push(Batch { cells, rows: rb.num_rows() });
    }
    Ok(prepared)
}

/// A row result's batches (REQ-2195), each its columns' cells as pyrows read them: cells as
/// `prepare` makes of Arrow batches.
pub fn prepare_rows(batches: Vec<Vec<Vec<P<'static>>>>, spec: &[Col]) -> Res<Vec<Batch>> {
    let mut prepared = Vec::with_capacity(batches.len());
    for columns in batches {
        let rows = columns.first().map_or(0, Vec::len);
        if rows == 0 {
            continue;
        }
        let cells = columns
            .into_iter()
            .zip(spec)
            .map(|(cells, c)| Cell::new(Reader::Rows(cells), c.mode.clone(), c.field.clone()))
            .collect();
        prepared.push(Batch { cells, rows });
    }
    Ok(prepared)
}

// ---------------------------------------------------------------- serialize_rows

/// What `serialize_rows` built: the tree and its result rows, in order (all of them; the caller
/// applies a result limit). A streaming caller is handed each row as it completes instead.
pub struct Rows {
    pub tree: Tree,
    pub rows: Vec<u32>,
    /// The many-to-one paths that matched several rows for one parent, sorted: only the first
    /// row's object was answered (`to_one_truncated`, REQ-1350).
    pub truncated: Vec<String>,
}

/// Where completed rows go.
pub trait Sink {
    /// A row of the flat path, complete when it is built: `true` to keep the tree (the row is
    /// retained), `false` when the sink wrote it.
    fn flat_row(&mut self, t: &Tree, row: u32) -> Res<bool>;
}

/// Keeps every row.
pub struct Keep;
impl Sink for Keep {
    fn flat_row(&mut self, _: &Tree, _: u32) -> Res<bool> {
        Ok(true)
    }
}

pub struct Encoder<'p> {
    pub p: &'p Plan,
    pub spec: &'p [Col],
    pub k: Keyer,
    key: Vec<u8>,
    /// A to-one object's key (the row's own key stays in `key`).
    object: Vec<u8>,
}

impl<'p> Encoder<'p> {
    pub fn new(p: &'p Plan, spec: &'p [Col]) -> Self {
        Encoder { p, spec, k: Keyer::new(), key: Vec::new(), object: Vec::new() }
    }

    /// `serialize_rows`.
    pub fn rows(&mut self, batches: &[Batch], sink: &mut dyn Sink) -> Res<Rows> {
        if self.p.with_one_to_many {
            self.one_to_many(batches)
        } else {
            self.flat(batches, sink)
        }
    }

    /// `_serialize_flat`: each row whose key is new becomes a result row, built once and never
    /// changed, so a streaming sink writes it as soon as it is built.
    fn flat(&mut self, batches: &[Batch], sink: &mut dyn Sink) -> Res<Rows> {
        let p = self.p;
        let keyed: Vec<usize> = p.root_cols.iter().copied().filter(|&c| !self.spec[c].is_agg).collect();
        let key_cols: Vec<usize> = if keyed.is_empty() { (0..self.spec.len()).collect() } else { keyed };
        // root key -> the result row (for `_detect_truncated_paths` on a repeat)
        let mut seen: Seen = Seen::default();
        let mut t = Tree::new(&p.names);
        let mut kept: Vec<u32> = Vec::new();
        // a streamed row's walk verdict, by its index in `fails`
        let mut fails: Vec<bool> = Vec::new();
        // each result row's to-one objects as keys (`built`), by its index in `fails`
        let mut objects: Vec<Box<[u8]>> = Vec::new();
        let mut truncated: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
        let mut cur: Vec<u8> = Vec::new();
        for b in batches {
            for r in 0..b.rows {
                self.key.clear();
                for &c in &key_cols {
                    b.cells[c].key(r, &mut self.key, &mut self.k)?;
                }
                if let Some(&at) = seen.get(self.key.as_slice()) {
                    // _detect_truncated_paths: this row's to-one objects (their values converted)
                    // against the first row's, by exact typed value.
                    cur.clear();
                    for path in &p.paths {
                        self.object_key(b, r, path, &mut cur)?;
                    }
                    if fails[at as usize] {
                        return Err(nesting_error("a repeated row's relationship path is not an object"));
                    }
                    if !p.paths.is_empty() {
                        for pi in differing(&objects[at as usize], &cur) {
                            truncated.insert(pi);
                        }
                    }
                    continue;
                }
                let root = t.obj();
                for &c in &p.root_cols {
                    let v = t.cell(&b.cells[c], r, &mut self.k)?;
                    t.set(root, p.col_name[c], v);
                }
                let skipped = self.build_nested(&mut t, root, b, r)?;
                fails.push(self.detect_fails(&mut t, root));
                if !p.paths.is_empty() {
                    cur.clear();
                    for (pi, path) in p.paths.iter().enumerate() {
                        if skipped[pi] {
                            // A parent this row had null: it built nothing here.
                            object_atom(&mut cur, &[pywrite::K_NULL]);
                        } else {
                            self.object_key(b, r, path, &mut cur)?;
                        }
                    }
                    objects.push(cur.as_slice().into());
                }
                seen.insert(self.key.as_slice().into(), fails.len() as u32 - 1);
                if sink.flat_row(&t, root)? {
                    kept.push(root);
                } else {
                    t.clear();
                }
            }
        }
        let mut names: Vec<String> = truncated.into_iter().map(|pi| p.paths[pi].name.clone()).collect();
        names.sort();
        Ok(Rows { tree: t, rows: kept, truncated: names })
    }

    /// The to-one object `path` builds of row `r`, as one length-prefixed key in `out`: null when
    /// its columns are all null, else `{field: value}` of its own fields.
    fn object_key(&mut self, b: &Batch, r: usize, path: &Path, out: &mut Vec<u8>) -> Res<()> {
        let mut key = std::mem::take(&mut self.object);
        key.clear();
        if path.cols.iter().all(|&c| b.cells[c].is_null(r)) {
            key.push(pywrite::K_NULL);
        } else {
            for &(name, c) in &path.fields {
                key.extend_from_slice(&name.to_le_bytes());
                b.cells[c].key(r, &mut key, &mut self.k)?;
            }
        }
        object_atom(out, &key);
        self.object = key;
        Ok(())
    }

    /// `_build_nested_obj`; for each path, whether a null parent skipped it.
    fn build_nested(&mut self, t: &mut Tree, root: u32, b: &Batch, r: usize) -> Res<Vec<bool>> {
        let mut skipped = vec![false; self.p.paths.len()];
        for (pi, path) in self.p.paths.iter().enumerate() {
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
                skipped[pi] = true;
                continue;
            }
            if all_none {
                t.set(target, *leaf, V::Null);
            } else {
                let o = t.obj();
                for &c in &path.cols {
                    let v = t.cell(&b.cells[c], r, &mut self.k)?;
                    t.set(o, self.p.col_name[c], v);
                }
                t.set(target, *leaf, V::Obj(o));
            }
        }
        Ok(skipped)
    }

    /// Whether `_detect_truncated_paths` raises on a repeat of this row: it walks each path's
    /// parents with `[]`, stopping at a null, and reads its leaf with `.get`.
    fn detect_fails(&self, t: &mut Tree, root: u32) -> bool {
        for path in &self.p.paths {
            let (_, inner) = path.full_parts.split_last().unwrap();
            let mut target = Some(root);
            for &part in inner {
                let Some(o) = target else { break };
                match t.get(o, part) {
                    Some(V::Obj(n)) => target = Some(n),
                    Some(V::Null) => target = None,
                    _ => return true,
                }
            }
        }
        false
    }

    /// `_serialize_with_one_to_many`: rows merge into the result as they come, so the whole tree is
    /// kept and written at the end.
    fn one_to_many(&mut self, batches: &[Batch]) -> Res<Rows> {
        let p = self.p;
        let mut t = Tree::new(&p.names);
        // root key -> (root object, its place)
        let mut seen: hashbrown::HashMap<Box<[u8]>, (u32, u64)> = hashbrown::HashMap::new();
        let mut rows: Vec<u32> = Vec::new();
        let mut elements: hashbrown::HashMap<Box<[u8]>, (u32, u64)> = hashbrown::HashMap::new();
        // a list's place (its parent's place, its path) -> (the list, its number at that place)
        let mut lists: HashMap<(u64, usize), (u32, u32)> = HashMap::new();
        // a to-one object's place (its parent's place, its path)
        let mut to_one: HashMap<(u64, usize), u64> = HashMap::new();
        let mut places: u64 = 0;
        let mut placed: Vec<Option<(u32, u64)>> = vec![None; p.paths.len()];
        for b in batches {
            for r in 0..b.rows {
                self.key.clear();
                for &c in &p.root_cols {
                    b.cells[c].key(r, &mut self.key, &mut self.k)?;
                }
                let (root, root_place) = match seen.get(self.key.as_slice()) {
                    Some(&x) => x,
                    None => {
                        let o = t.obj();
                        for &c in &p.root_cols {
                            let v = t.cell(&b.cells[c], r, &mut self.k)?;
                            t.set(o, p.col_name[c], v);
                        }
                        places += 1;
                        seen.insert(self.key.as_slice().into(), (o, places));
                        rows.push(o);
                        (o, places)
                    }
                };
                for &pi in &p.sorted {
                    let path = &p.paths[pi];
                    let holder = match path.parent {
                        None => Some((root, root_place)),
                        Some(pp) => placed[pp],
                    };
                    let Some((mut holder, parent_place)) = holder else {
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
                        let at = (parent_place, pi);
                        let arr = t.get(holder, leaf);
                        let known = lists.get(&at).copied();
                        let current = match (known, arr) {
                            (Some((l, n)), Some(V::Arr(a))) if a == l => (l, n),
                            _ => {
                                let a = match arr {
                                    Some(V::Arr(a)) => a,
                                    _ => {
                                        let a = t.arr();
                                        t.set(holder, leaf, V::Arr(a));
                                        a
                                    }
                                };
                                let n = known.map_or(0, |k| k.1 + 1);
                                lists.insert(at, (a, n));
                                (a, n)
                            }
                        };
                        if empty {
                            placed[pi] = None;
                            continue;
                        }
                        self.key.clear();
                        self.key.extend_from_slice(&parent_place.to_le_bytes());
                        self.key.extend_from_slice(&(pi as u64).to_le_bytes());
                        self.key.extend_from_slice(&current.1.to_le_bytes());
                        for &(_, c) in &path.fields {
                            b.cells[c].key(r, &mut self.key, &mut self.k)?;
                        }
                        let el = match elements.get(self.key.as_slice()) {
                            Some(&x) => x,
                            None => {
                                let o = t.obj();
                                for &c in &path.cols {
                                    let v = t.cell(&b.cells[c], r, &mut self.k)?;
                                    t.set(o, p.col_name[c], v);
                                }
                                places += 1;
                                elements.insert(self.key.as_slice().into(), (o, places));
                                t.arrs[current.0 as usize].push(V::Obj(o));
                                (o, places)
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
                        for &c in &path.cols {
                            let v = t.cell(&b.cells[c], r, &mut self.k)?;
                            t.set(o, p.col_name[c], v);
                        }
                        let place = *to_one.entry((parent_place, pi)).or_insert_with(|| {
                            places += 1;
                            places
                        });
                        placed[pi] = Some((o, place));
                    }
                }
            }
        }
        Ok(Rows { tree: t, rows, truncated: Vec::new() })
    }
}

/// One object's key, length-prefixed, appended to a row's keys.
fn object_atom(out: &mut Vec<u8>, key: &[u8]) {
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
}

/// The paths (by index) whose object keys differ between two rows' keys.
fn differing(a: &[u8], b: &[u8]) -> Vec<usize> {
    fn split(mut s: &[u8]) -> Vec<&[u8]> {
        let mut v = Vec::new();
        while !s.is_empty() {
            let n = u32::from_le_bytes(s[..4].try_into().unwrap()) as usize;
            v.push(&s[4..4 + n]);
            s = &s[4 + n..];
        }
        v
    }
    split(a).into_iter().zip(split(b)).enumerate().filter(|(_, (x, y))| x != y).map(|(i, _)| i).collect()
}

// ---------------------------------------------------------------- serialize_aggregate

/// `serialize_aggregate`'s payload, `{agg_alias: {...}, nodes...}`, as one object of a tree.
pub fn aggregate(batches: &[Batch], spec: &[Col], agg_alias: &str) -> Res<(Tree, u32, u32)> {
    let mut t = Tree::new(&Interner::default());
    let alias = t.names.id(agg_alias);
    let inner = t.obj();
    let payload = t.obj();
    t.set(payload, alias, V::Obj(inner));
    let mut k = Keyer::new();
    // `agg_rows[0]`: the first row, which an empty first batch does not hold.
    if let Some(b) = batches.iter().find(|b| b.rows > 0) {
        let prefix = format!("{agg_alias}.");
        for (i, col) in spec.iter().enumerate() {
            let name = t.names.id(&col.field);
            if col.nested_in.is_none() && col.is_agg {
                let v = t.cell(&b.cells[i], 0, &mut k)?;
                t.set(payload, name, v);
                continue;
            }
            let path = col.nested_in.clone().unwrap_or_else(|| agg_alias.to_string());
            let sub: Vec<String> = if path == agg_alias {
                Vec::new()
            } else if let Some(rest) = path.strip_prefix(&prefix) {
                rest.split('.').map(str::to_string).collect()
            } else {
                path.split('.').map(str::to_string).collect()
            };
            let mut target = inner;
            for part in &sub {
                let id = t.names.id(part);
                target = match t.get(target, id) {
                    Some(V::Obj(o)) => o,
                    _ => {
                        let o = t.obj();
                        t.set(target, id, V::Obj(o));
                        o
                    }
                };
            }
            let v = t.cell(&b.cells[i], 0, &mut k)?;
            t.set(target, name, v);
        }
    }
    Ok((t, payload, inner))
}
