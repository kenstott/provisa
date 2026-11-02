//! The documents each JSON surface answers, over the same nesting and writer: GraphQL's
//! `{"data": {root: rows}}`, the REST rows endpoint's `{"data": rows}`, and JSON:API's compound
//! document (data, relationships, included, meta, links).

use std::collections::HashMap;

use crate::fmt;
use crate::nest::{self, Batch, Col, Encoder, LeafKind, Plan, Sink, Tree, V};
use crate::pyval::{err, Res};

/// What the rows written said about themselves: how many, each relationship's matched rows (a
/// to-many's elements, a to-one's objects, by the root key holding it), and the to-one paths that
/// matched several rows for one parent.
#[derive(Default)]
pub struct Written {
    pub rows: usize,
    pub joined: Vec<(String, u64)>,
    pub truncated: Vec<String>,
}

/// The root keys that hold relationships: each path's first name.
fn relationship_keys(p: &Plan) -> Vec<u32> {
    let mut keys = Vec::new();
    for path in &p.paths {
        if !keys.contains(&path.full_parts[0]) {
            keys.push(path.full_parts[0]);
        }
    }
    keys
}

fn count_joined(t: &Tree, o: u32, keys: &[u32], counts: &mut [u64]) {
    for (i, k) in keys.iter().enumerate() {
        if let Some(&(_, v)) = t.objs[o as usize].iter().find(|e| e.0 == *k) {
            counts[i] += match v {
                V::Arr(a) => t.arrs[a as usize].len() as u64,
                V::Obj(_) => 1,
                V::Json(j) => match &t.jv[j as usize] {
                    crate::pyval::P::List(l) => l.len() as u64,
                    crate::pyval::P::None => 0,
                    _ => 1,
                },
                V::Null => 0,
                _ => 1,
            };
        }
    }
}

/// Writes each flat row as soon as it is built, up to a limit.
struct Stream<'o> {
    out: &'o mut Vec<u8>,
    limit: Option<usize>,
    written: usize,
    depth: usize,
    keys: &'o [u32],
    counts: Vec<u64>,
}

impl Sink for Stream<'_> {
    fn flat_row(&mut self, t: &Tree, row: u32) -> Res<bool> {
        if self.limit.is_none_or(|l| self.written < l) {
            if self.written > 0 {
                self.out.push(b',');
            }
            t.write_obj(row, self.out, self.depth)?;
            count_joined(t, row, self.keys, &mut self.counts);
            self.written += 1;
        }
        Ok(false)
    }
}

/// `serialize_rows`' rows written as a JSON array's items at `depth`, the tree's rows after a
/// streamed flat answer (none) or the one-to-many answer's (written here).
fn rows_into(batches: &[Batch], p: &Plan, spec: &[Col], limit: Option<usize>, depth: usize, out: &mut Vec<u8>) -> Res<Written> {
    let keys = relationship_keys(p);
    let mut e = Encoder::new(p, spec);
    let mut s = Stream { out, limit, written: 0, depth, keys: &keys, counts: vec![0; keys.len()] };
    let rows = e.rows(batches, &mut s)?;
    let (mut written, mut counts) = (s.written, s.counts);
    let n = limit.map_or(rows.rows.len(), |l| l.min(rows.rows.len()));
    for (i, &o) in rows.rows[..n].iter().enumerate() {
        if i > 0 || written > 0 {
            out.push(b',');
        }
        rows.tree.write_obj(o, out, depth)?;
        count_joined(&rows.tree, o, &keys, &mut counts);
    }
    written += n;
    let joined = keys.iter().zip(counts).map(|(k, c)| (String::from_utf8_lossy(&p.names.raw[*k as usize]).into_owned(), c)).collect();
    Ok(Written { rows: written, joined, truncated: rows.truncated })
}

/// GraphQL: a root field's rows, the JSON array `{"data": {root_field: ...}}` holds.
pub fn graphql_field(batches: &[Batch], spec: &[Col], limit: Option<usize>) -> Res<(Vec<u8>, Written)> {
    let p = nest::plan(spec);
    let mut out = Vec::new();
    out.push(b'[');
    let w = rows_into(batches, &p, spec, limit, 4, &mut out)?;
    out.push(b']');
    Ok((out, w))
}

/// GraphQL: an aggregate root field's answer, `{agg_alias: {...}, nodes...}`.
pub fn graphql_aggregate(batches: &[Batch], spec: &[Col], agg_alias: &str) -> Res<Vec<u8>> {
    let (t, payload, _) = nest::aggregate(batches, spec, agg_alias)?;
    let mut out = Vec::new();
    t.write_obj(payload, &mut out, 2)?;
    Ok(out)
}

/// REST: `{"data": rows}` (rows and group-by), or `{"data": aggregate}`.
pub fn rest(batches: &[Batch], spec: &[Col], agg_alias: Option<&str>) -> Res<(Vec<u8>, Written)> {
    let mut out = Vec::new();
    let mut w = Written::default();
    out.extend_from_slice(b"{\"data\":");
    match agg_alias {
        Some(alias) => {
            let (t, _, inner) = nest::aggregate(batches, spec, alias)?;
            t.write_obj(inner, &mut out, 2)?;
        }
        None => {
            let p = nest::plan(spec);
            out.push(b'[');
            w = rows_into(batches, &p, spec, None, 3, &mut out)?;
            out.push(b']');
        }
    }
    out.push(b'}');
    Ok((out, w))
}

/// REST with `Accept: text/csv`: `csv.writer` (the excel dialect, QUOTE_MINIMAL) over the rows
/// `serialize_rows` built, the first row's keys as the header, each value as `str()` writes it
/// (`repr` for a float, nothing for None).
pub fn rest_csv(batches: &[Batch], spec: &[Col]) -> Res<(Vec<u8>, Vec<String>)> {
    let p = nest::plan(spec);
    let mut e = Encoder::new(&p, spec);
    let rows = e.rows(batches, &mut nest::Keep)?;
    let t = &rows.tree;
    let mut out = Vec::new();
    let Some(&first) = rows.rows.first() else { return Ok((out, rows.truncated)) };
    let header: Vec<Vec<u8>> = t.objs[first as usize].iter().map(|(k, _)| t.names.raw[*k as usize].clone()).collect();
    csv_row(&mut out, header.iter().map(|h| Some(h.clone())).collect());
    for &o in &rows.rows {
        let fields = t.objs[o as usize]
            .iter()
            .map(|&(_, v)| {
                if matches!(v, V::Null) {
                    return None;
                }
                let mut s = Vec::new();
                t.py_str(v, &mut s);
                Some(s)
            })
            .collect();
        csv_row(&mut out, fields);
    }
    Ok((out, rows.truncated))
}

fn csv_row(out: &mut Vec<u8>, fields: Vec<Option<Vec<u8>>>) {
    let only = fields.len() == 1;
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        let f = f.as_deref().unwrap_or(b"");
        let quote = (only && f.is_empty()) || f.iter().any(|&c| matches!(c, b',' | b'"' | b'\r' | b'\n'));
        if quote {
            out.push(b'"');
            for &c in f {
                if c == b'"' {
                    out.push(b'"');
                }
                out.push(c);
            }
            out.push(b'"');
        } else {
            out.extend_from_slice(f);
        }
    }
    out.extend_from_slice(b"\r\n");
}

// ---------------------------------------------------------------- JSON:API

/// What the router hands the document: the names (REQ-1417), the keys, the page.
pub struct JsonApi {
    pub resource_type: String,
    pub page_size: usize,
    pub page_number: usize,
    /// A probe row was fetched (no total asked for).
    pub probe: bool,
    pub total: Option<i64>,
    pub id_columns: Vec<String>,
    pub scalar_names: HashMap<String, String>,
    pub rel_names: Vec<(String, String)>,
    pub rel_scalar_names: HashMap<String, HashMap<String, String>>,
    /// `?include=` names, physical, as given (a repeat is popped twice).
    pub includes: Vec<String>,
    pub include_keys: HashMap<String, Vec<String>>,
    pub base_path: String,
    pub link_params: Vec<(String, String)>,
    pub warnings: Option<Vec<u8>>,
    /// The message of the warning a to-one path that matched several rows for one parent raises,
    /// `{path}` where its path goes (serialize.TO_ONE_TRUNCATED_MESSAGE), and its code.
    pub truncation: (String, String),
}

/// `rename_row_keys` / `_rename_nested`: new objects with keys renamed.
struct Renamer<'a> {
    t: &'a mut Tree,
}

impl Renamer<'_> {
    fn id(&mut self, s: &str) -> u32 {
        self.t.names.id(s)
    }
    fn raw(&self, k: u32) -> String {
        String::from_utf8(self.t.names.raw[k as usize].clone()).unwrap_or_default()
    }

    /// An object's entries renamed by `map` (absent names kept).
    fn rename_obj(&mut self, o: u32, map: &HashMap<String, String>) -> u32 {
        let entries = self.t.objs[o as usize].clone();
        let n = self.t.obj();
        for (k, v) in entries {
            let name = self.raw(k);
            let nk = match map.get(&name) {
                Some(p) => self.id(p),
                None => k,
            };
            self.t.set(n, nk, v);
        }
        n
    }

    /// `_rename_nested(value, map)`.
    fn nested(&mut self, v: V, map: &HashMap<String, String>) -> V {
        match self.t.live(v) {
            V::Obj(o) => V::Obj(self.rename_obj(o, map)),
            V::Arr(a) => {
                let items = self.t.arrs[a as usize].clone();
                let n = self.t.arr();
                for x in items {
                    let x = self.nested(x, map);
                    self.t.arrs[n as usize].push(x);
                }
                V::Arr(n)
            }
            other => other,
        }
    }

    /// `rename_row_keys(row, gql_to_physical, rel_gql_to_physical, rel_scalar_maps)`.
    fn row(&mut self, o: u32, scalars: &HashMap<String, String>, rels: &[(String, String)], rel_scalars: &HashMap<String, HashMap<String, String>>) -> u32 {
        let entries = self.t.objs[o as usize].clone();
        let n = self.t.obj();
        let empty = HashMap::new();
        for (k, v) in entries {
            let name = self.raw(k);
            if let Some((_, phys)) = rels.iter().find(|(g, _)| *g == name) {
                let nk = self.id(phys);
                let map = rel_scalars.get(&name).unwrap_or(&empty);
                let v = self.nested(v, map);
                self.t.set(n, nk, v);
            } else {
                let nk = match scalars.get(&name) {
                    Some(p) => self.id(p),
                    None => k,
                };
                self.t.set(n, nk, v);
            }
        }
        n
    }
}

fn find(t: &Tree, o: u32, name: &str) -> Option<usize> {
    let id = t.names.get(name.as_bytes())?;
    t.objs[o as usize].iter().position(|e| e.0 == id)
}

/// A resource id's text of a value: a string as itself, any other value as its JSON (a number's
/// digits, `true`, a JSON object's text), as the answer writes it.
fn id_text(t: &Tree, v: V, out: &mut Vec<u8>) -> Res<()> {
    let mut w = Vec::new();
    t.write_value(v, &mut w, 0)?;
    match w.first() {
        Some(b'"') => nest::unescape(&w, out),
        _ => out.extend_from_slice(&w),
    }
    Ok(())
}

/// `resource_id(row, id_columns)`: one key's text, or the keys' texts joined by `:` (each `:` and
/// `\` in them escaped).
fn resource_id(t: &Tree, o: u32, cols: &[String]) -> Res<Vec<u8>> {
    let mut out = Vec::new();
    for (i, c) in cols.iter().enumerate() {
        let Some(pos) = find(t, o, c) else { return err(format!("KeyError: '{c}'")) };
        let v = t.objs[o as usize][pos].1;
        let mut s = Vec::new();
        id_text(t, v, &mut s)?;
        if cols.len() == 1 {
            return Ok(s);
        }
        if i > 0 {
            out.push(b':');
        }
        for b in s {
            if b == b'\\' || b == b':' {
                out.push(b'\\');
            }
            out.push(b);
        }
    }
    Ok(out)
}

/// The value's truthiness, for `[nested] if nested else []`.
fn truthy(t: &Tree, v: V) -> bool {
    match v {
        V::Null => false,
        V::Leaf { start, len, kind } => {
            let b = &t.buf[start as usize..start as usize + len as usize];
            match kind {
                LeafKind::Str => b != b"\"\"",
                LeafKind::Int => b != b"0",
                LeafKind::Bool => b == b"true",
            }
        }
        V::Float(f) => f != 0.0,
        V::Obj(o) => !t.objs[o as usize].is_empty(),
        V::Arr(a) => !t.arrs[a as usize].is_empty(),
        V::Json(_) => true,
    }
}

fn write_resource(t: &Tree, out: &mut Vec<u8>, ty: &[u8], id: &[u8], attrs: &[(u32, V)], rels: &[(Vec<u8>, Vec<u8>)]) -> Res<()> {
    out.extend_from_slice(b"{\"type\":");
    fmt::write_str(out, ty);
    out.extend_from_slice(b",\"id\":");
    crate::pywrite::write_text(out, id)?;
    out.extend_from_slice(b",\"attributes\":");
    t.write_entries(attrs, out, 4)?;
    if !rels.is_empty() {
        out.extend_from_slice(b",\"relationships\":{");
        for (i, (k, v)) in rels.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            fmt::write_str(out, k);
            out.push(b':');
            out.extend_from_slice(v);
        }
        out.push(b'}');
    }
    out.push(b'}');
    Ok(())
}

fn identifier(out: &mut Vec<u8>, ty: &[u8], id: &[u8]) -> Res<()> {
    out.extend_from_slice(b"{\"type\":");
    fmt::write_str(out, ty);
    out.extend_from_slice(b",\"id\":");
    crate::pywrite::write_text(out, id)
}

/// `urlencode`'s `quote_plus`.
pub fn quote_plus(out: &mut String, s: &str) {
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => out.push(b as char),
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
}

fn page_url(a: &JsonApi, pn: usize) -> String {
    let mut params: Vec<(String, String)> = vec![("page[number]".into(), pn.to_string()), ("page[size]".into(), a.page_size.to_string())];
    for (k, v) in &a.link_params {
        match params.iter_mut().find(|e| e.0 == *k) {
            Some(e) => e.1 = v.clone(),
            None => params.push((k.clone(), v.clone())),
        }
    }
    let mut s = format!("{}?", a.base_path);
    for (i, (k, v)) in params.iter().enumerate() {
        if i > 0 {
            s.push('&');
        }
        quote_plus(&mut s, k);
        s.push('=');
        quote_plus(&mut s, v);
    }
    s
}

/// `build_pagination_links`.
fn links(out: &mut Vec<u8>, a: &JsonApi, has_next: bool) {
    let mut entries: Vec<(&str, Option<String>)> = vec![("self", Some(page_url(a, a.page_number))), ("first", Some(page_url(a, 1)))];
    let mut has_next = has_next;
    if let Some(total) = a.total {
        let ps = a.page_size as i64;
        let last = 1.max((total + ps - 1).div_euclid(ps)) as usize;
        entries.push(("last", Some(page_url(a, last))));
        has_next = (a.page_number as i64) < last as i64;
    }
    entries.push(("prev", (a.page_number > 1).then(|| page_url(a, a.page_number - 1))));
    entries.push(("next", has_next.then(|| page_url(a, a.page_number + 1))));
    out.push(b'{');
    for (i, (k, v)) in entries.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        fmt::write_str(out, k.as_bytes());
        out.push(b':');
        match v {
            Some(s) => fmt::write_str(out, s.as_bytes()),
            None => out.extend_from_slice(b"null"),
        }
    }
    out.push(b'}');
}

/// The JSON:API document of a table read: the rows `serialize_rows` builds, cut to the page,
/// renamed to physical names, their included resources taken out (`_extract_included`), as
/// `rows_to_jsonapi` writes them, with the router's meta and links.
pub fn jsonapi(batches: &[Batch], spec: &[Col], a: &JsonApi) -> Res<(Vec<u8>, Vec<String>)> {
    let p = nest::plan(spec);
    let mut e = Encoder::new(&p, spec);
    let rows = e.rows(batches, &mut nest::Keep)?;
    let mut t = rows.tree;
    let truncated = rows.truncated;
    let n = rows.rows.len();
    let has_next = if a.probe { n > a.page_size } else { n == a.page_size };
    let page: Vec<u32> = rows.rows[..n.min(a.page_size)].to_vec();

    let mut ren = Renamer { t: &mut t };
    let page: Vec<u32> = page.into_iter().map(|o| ren.row(o, &a.scalar_names, &a.rel_names, &a.rel_scalar_names)).collect();

    // An include named twice is the one include (`include=a,b,a` is `include=a,b`).
    let mut includes: Vec<&String> = Vec::new();
    for inc in &a.includes {
        if !includes.contains(&inc) {
            includes.push(inc);
        }
    }
    // _extract_included
    let mut buckets: Vec<(String, Vec<V>, std::collections::HashSet<Vec<u8>>)> = Vec::new();
    let mut row_links: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();
    for &o in &page {
        let mut rl: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for &inc in &includes {
            let nested = match find(&t, o, inc) {
                Some(pos) => Some(t.objs[o as usize].remove(pos).1),
                None => None,
            };
            let nested = nested.map(|v| t.live(v));
            let is_list = matches!(nested, Some(V::Arr(_)));
            let items: Vec<V> = match nested {
                Some(V::Arr(x)) => t.arrs[x as usize].clone(),
                Some(v) if truthy(&t, v) => vec![v],
                _ => Vec::new(),
            };
            if !buckets.iter().any(|b| b.0 == *inc) {
                buckets.push((inc.clone(), Vec::new(), Default::default()));
            }
            let keys = a.include_keys.get(inc).cloned().unwrap_or_default();
            let mut idents: Vec<Vec<u8>> = Vec::new();
            for item in items {
                let item = t.live(item);
                let V::Obj(io) = item else { return err("TypeError: an included value is not an object") };
                let rid = resource_id(&t, io, &keys)?;
                let mut ident = Vec::new();
                identifier(&mut ident, inc.as_bytes(), &rid)?;
                ident.push(b'}');
                idents.push(ident);
                let b = buckets.iter_mut().find(|b| b.0 == *inc).unwrap();
                if b.2.insert(rid) {
                    b.1.push(item);
                }
            }
            let mut link = b"{\"data\":".to_vec();
            if is_list {
                link.push(b'[');
                link.extend_from_slice(&idents.join(&b","[..]));
                link.push(b']');
            } else {
                match idents.first() {
                    Some(i) => link.extend_from_slice(i),
                    None => link.extend_from_slice(b"null"),
                }
            }
            link.push(b'}');
            match rl.iter_mut().find(|e| e.0 == inc.as_bytes()) {
                Some(e) => e.1 = link,
                None => rl.push((inc.as_bytes().to_vec(), link)),
            }
        }
        row_links.push(rl);
    }

    let fks: Vec<(String, String)> = a.rel_names.iter().map(|(_, p)| (format!("{p}_id"), p.clone())).collect();
    let ty = a.resource_type.as_bytes();
    let mut out = Vec::new();
    out.extend_from_slice(b"{\"data\":[");
    for (i, &o) in page.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        let rid = resource_id(&t, o, &a.id_columns)?;
        let mut attrs: Vec<(u32, V)> = Vec::new();
        let mut rels: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        // A column named id is the resource's id only when the id is that column; an id built
        // from other keys leaves it an attribute.
        let id_is_column = a.id_columns.len() == 1 && a.id_columns[0] == "id";
        for &(k, v) in &t.objs[o as usize] {
            let name = &t.names.raw[k as usize];
            if id_is_column && name == b"id" {
                continue;
            }
            if let Some((_, rel)) = fks.iter().find(|(fk, _)| fk.as_bytes() == name.as_slice()) {
                let rel_name = &name[..name.len() - 3];
                let mut d = b"{\"data\":".to_vec();
                if matches!(v, V::Null) {
                    d.extend_from_slice(b"null");
                } else {
                    let mut s = Vec::new();
                    id_text(&t, v, &mut s)?;
                    identifier(&mut d, rel.as_bytes(), &s)?;
                    d.push(b'}');
                }
                d.push(b'}');
                match rels.iter_mut().find(|e| e.0 == rel_name) {
                    Some(e) => e.1 = d,
                    None => rels.push((rel_name.to_vec(), d)),
                }
            } else {
                attrs.push((k, v));
            }
        }
        if !a.includes.is_empty() {
            for (k, v) in &row_links[i] {
                match rels.iter_mut().find(|e| e.0 == *k) {
                    Some(e) => e.1 = v.clone(),
                    None => rels.push((k.clone(), v.clone())),
                }
            }
        }
        write_resource(&t, &mut out, ty, &rid, &attrs, &rels)?;
    }
    out.push(b']');
    let filled: Vec<_> = buckets.iter().filter(|b| !b.1.is_empty()).collect();
    if !filled.is_empty() {
        out.extend_from_slice(b",\"included\":[");
        let mut first = true;
        for (inc, items, _) in filled {
            let keys = a.include_keys.get(inc);
            let id_is_column = keys.is_none_or(|k| k.len() == 1 && k[0] == "id");
            for &item in items {
                let V::Obj(io) = item else { unreachable!() };
                let rid = match keys {
                    Some(k) => resource_id(&t, io, k)?,
                    None => {
                        let mut s = Vec::new();
                        if let Some(pos) = find(&t, io, "id") {
                            id_text(&t, t.objs[io as usize][pos].1, &mut s)?;
                        }
                        s
                    }
                };
                let attrs: Vec<(u32, V)> =
                    t.objs[io as usize].iter().filter(|(k, _)| !(id_is_column && t.names.raw[*k as usize] == b"id")).copied().collect();
                if !first {
                    out.push(b',');
                }
                first = false;
                write_resource(&t, &mut out, inc.as_bytes(), &rid, &attrs, &[])?;
            }
        }
        out.push(b']');
    }
    let mut meta: Vec<(&[u8], Vec<u8>)> = Vec::new();
    if let Some(total) = a.total {
        meta.push((b"total", total.to_string().into_bytes()));
    }
    // The request's warnings, then each truncated to-one path's (REQ-1350).
    let mut entries: Vec<u8> = match &a.warnings {
        Some(w) if w.len() > 2 => w[..w.len() - 1].to_vec(),
        _ => b"[".to_vec(),
    };
    for path in &truncated {
        if entries.len() > 1 {
            entries.push(b',');
        }
        let (code, template) = &a.truncation;
        entries.extend_from_slice(b"{\"code\":");
        fmt::write_str(&mut entries, code.as_bytes());
        entries.extend_from_slice(b",\"params\":{\"path\":");
        crate::pywrite::write_text(&mut entries, path.as_bytes())?;
        entries.extend_from_slice(b"},\"message\":");
        crate::pywrite::write_text(&mut entries, template.replace("{path}", path).as_bytes())?;
        entries.push(b'}');
    }
    entries.push(b']');
    if entries.len() > 2 {
        meta.push((b"warnings", entries));
    }
    if !meta.is_empty() {
        out.extend_from_slice(b",\"meta\":{");
        for (i, (k, v)) in meta.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            fmt::write_str(&mut out, k);
            out.push(b':');
            out.extend_from_slice(v);
        }
        out.push(b'}');
    }
    out.extend_from_slice(b",\"links\":");
    links(&mut out, a, has_next);
    out.push(b'}');
    Ok((out, truncated))
}

/// JSON:API `?groupBy=`: each group row, its groupKey and nodes renamed, as a resource.
pub fn jsonapi_group_by(batches: &[Batch], spec: &[Col], resource_type: &str, scalars: &HashMap<String, String>, rels: &[(String, String)], rel_scalars: &HashMap<String, HashMap<String, String>>) -> Res<Vec<u8>> {
    let p = nest::plan(spec);
    let mut e = Encoder::new(&p, spec);
    let rows = e.rows(batches, &mut nest::Keep)?;
    let mut t = rows.tree;
    // The group key's own fields: a group whose every key is NULL is that key, each field null.
    let key_fields: Vec<u32> = {
        let mut f = Vec::new();
        for c in spec.iter().filter(|c| c.nested_in.as_deref() == Some("groupKey")) {
            let id = t.names.id(&c.field);
            if !f.contains(&id) {
                f.push(id);
            }
        }
        f
    };
    for &o in &rows.rows {
        if let Some(pos) = find(&t, o, "groupKey") {
            let v = match t.live(t.objs[o as usize][pos].1) {
                V::Null => {
                    let g = t.obj();
                    for &k in &key_fields {
                        t.set(g, k, V::Null);
                    }
                    V::Obj(g)
                }
                v => v,
            };
            let V::Obj(g) = v else { return err("groupKey is not an object") };
            let mut ren = Renamer { t: &mut t };
            let n = ren.rename_obj(g, scalars);
            t.objs[o as usize][pos].1 = V::Obj(n);
        }
        if let Some(pos) = find(&t, o, "nodes") {
            let v = t.live(t.objs[o as usize][pos].1);
            let items: Vec<V> = match v {
                V::Arr(a) => t.arrs[a as usize].clone(),
                V::Obj(x) if t.objs[x as usize].is_empty() => Vec::new(),
                V::Leaf { kind: LeafKind::Str, .. } if !truthy(&t, v) => Vec::new(),
                _ => return err("TypeError: nodes is not a list of objects"),
            };
            let n = t.arr();
            for item in items {
                let V::Obj(io) = t.live(item) else { return err("AttributeError: a node has no items()") };
                let mut ren = Renamer { t: &mut t };
                let r = ren.row(io, scalars, rels, rel_scalars);
                t.arrs[n as usize].push(V::Obj(r));
            }
            t.objs[o as usize][pos].1 = V::Arr(n);
        }
    }
    let ty = format!("{resource_type}GroupBy");
    let mut out = Vec::new();
    out.extend_from_slice(b"{\"data\":[");
    for (i, &o) in rows.rows.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        let entries = t.objs[o as usize].clone();
        write_resource(&t, &mut out, ty.as_bytes(), i.to_string().as_bytes(), &entries, &[])?;
    }
    out.extend_from_slice(b"]}");
    Ok(out)
}

/// JSON:API `?aggregate`: `{"data": null, "meta": {"aggregate": {...}}}`.
pub fn jsonapi_aggregate(batches: &[Batch], spec: &[Col], agg_alias: &str) -> Res<Vec<u8>> {
    let (t, _, inner) = nest::aggregate(batches, spec, agg_alias)?;
    let mut out = Vec::new();
    out.extend_from_slice(b"{\"data\":null,\"meta\":{\"aggregate\":");
    t.write_obj(inner, &mut out, 3)?;
    out.extend_from_slice(b"}}");
    Ok(out)
}
