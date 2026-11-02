// Every BVType's text and binary writer over a cell's value (pyval::PyVal), each a port of the
// converter in buenavista's BVTYPE_TO_PGTYPE (vendor/buenavista/buenavista/postgres.py) or
// provisa.pgwire.server named in its comment. Where that converter's answer is not PostgreSQL 17's,
// the writer gives PostgreSQL's, and RESULTS.md lists the product defect.

use std::sync::{Arc, RwLock};

use crate::fmt_num::{float4_text, float8_text, numeric_binary, numeric_text};
use crate::pyval::{
    self, date_days, iter_items, packable_float, str_of, to_decimal, to_float, to_int, truthy, type_error, Dec,
    DecSpecial, Inet, PyVal,
};
use crate::temporal::{self, DAY_US};
use crate::{json, uuid_bytes, EncErr, R};

/// A BVType: how a column is described and written.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Bv {
    Null,
    Unknown,
    Array,
    Bool,
    Int2,
    Int4,
    Int8,
    Oid,
    Float4,
    Float8,
    Numeric,
    Text,
    Bytes,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    TimeTz,
    Interval,
    Json,
    Jsonb,
    Uuid,
    Inet,
    Cidr,
    MacAddr,
    MacAddr8,
    Xml,
    Point,
    Money,
    Line,
    Lseg,
    Box,
    Path,
    Polygon,
    Circle,
    Geometry,
    Geography,
    Int4Range,
    Int8Range,
    NumRange,
    TsRange,
    TstzRange,
    DateRange,
    Int4Multirange,
    Int8Multirange,
    NumMultirange,
    TsMultirange,
    TstzMultirange,
    DateMultirange,
    Bit,
    VarBit,
    TsVector,
    TsQuery,
    /// A type held as its text with no binary form here (buenavista's _text_only).
    TextOnly(&'static str),
    Char,
    Name,
    PgNodeTree,
    Refcursor,
    Tid,
    Xid,
    Cid,
    Xid8,
    Snapshot,
    JsonPath,
    PgLsn,
    RegProc,
    RegType,
    RegClass,
    RegNamespace,
    OidVector,
    Int2Vector,
    /// An array of an element type, sent with the array's element OID.
    ArrayOf(Elem, i32),
    /// aclitem[]: text[]'s text, no binary form.
    AclItemArray,
}

/// The element of an array type: its writer and its OID.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Elem {
    Int2,
    Int4,
    Int8,
    Oid,
    Float4,
    Float8,
    Numeric,
    Bool,
    Text,
    Money,
    Interval,
    RegType,
    Xml,
}

impl Elem {
    pub fn bv(self) -> Bv {
        match self {
            Elem::Int2 => Bv::Int2,
            Elem::Int4 => Bv::Int4,
            Elem::Int8 => Bv::Int8,
            Elem::Oid => Bv::Oid,
            Elem::Float4 => Bv::Float4,
            Elem::Float8 => Bv::Float8,
            Elem::Numeric => Bv::Numeric,
            Elem::Bool => Bv::Bool,
            Elem::Text => Bv::Text,
            Elem::Money => Bv::Money,
            Elem::Interval => Bv::Interval,
            Elem::RegType => Bv::RegType,
            Elem::Xml => Bv::Xml,
        }
    }
    pub fn oid(self) -> i32 {
        match self {
            Elem::Int2 => 21,
            Elem::Int4 => 23,
            Elem::Int8 => 20,
            Elem::Oid => 26,
            Elem::Float4 => 700,
            Elem::Float8 => 701,
            Elem::Numeric => 1700,
            Elem::Bool => 16,
            Elem::Text => 25,
            Elem::Money => 790,
            Elem::Interval => 1186,
            Elem::RegType => 2206,
            Elem::Xml => 142,
        }
    }
}

/// The BVType named by its member name (and xml[]'s, which BVType does not list).
pub fn bv_of(name: &str) -> Bv {
    use Bv::*;
    match name {
        "NULL" => Null,
        "ARRAY" => Array,
        "BOOL" => Bool,
        "SMALLINT" => Int2,
        "INTEGER" => Int4,
        "BIGINT" => Int8,
        "OID" => Oid,
        "REAL" => Float4,
        "FLOAT" => Float8,
        "DECIMAL" => Numeric,
        "TEXT" => Text,
        "BYTES" => Bytes,
        "DATE" => Date,
        "TIMESTAMP" => Timestamp,
        "TIMESTAMPTZ" => TimestampTz,
        "TIME" => Time,
        "TIMETZ" => TimeTz,
        "INTERVAL" => Interval,
        "JSON" => Json,
        "JSONB" => Jsonb,
        "UUID" => Uuid,
        "INET" => Inet,
        "CIDR" => Cidr,
        "MACADDR" => MacAddr,
        "MACADDR8" => MacAddr8,
        "XML" => Xml,
        "POINT" => Point,
        "MONEY" => Money,
        "LINE" => Line,
        "LSEG" => Lseg,
        "BOX" => Box,
        "PATH" => Path,
        "POLYGON" => Polygon,
        "CIRCLE" => Circle,
        "GEOMETRY" => Geometry,
        "GEOGRAPHY" => Geography,
        "INT4RANGE" => Int4Range,
        "INT8RANGE" => Int8Range,
        "NUMRANGE" => NumRange,
        "TSRANGE" => TsRange,
        "TSTZRANGE" => TstzRange,
        "DATERANGE" => DateRange,
        "INT4MULTIRANGE" => Int4Multirange,
        "INT8MULTIRANGE" => Int8Multirange,
        "NUMMULTIRANGE" => NumMultirange,
        "TSMULTIRANGE" => TsMultirange,
        "TSTZMULTIRANGE" => TstzMultirange,
        "DATEMULTIRANGE" => DateMultirange,
        "BIT" => Bit,
        "VARBIT" => VarBit,
        "TSVECTOR" => TsVector,
        "TSQUERY" => TsQuery,
        "ACLITEM" => TextOnly("aclitem"),
        "REGPROCEDURE" => TextOnly("regprocedure"),
        "REGOPER" => TextOnly("regoper"),
        "REGOPERATOR" => TextOnly("regoperator"),
        "PG_NDISTINCT" => TextOnly("pg_ndistinct"),
        "PG_DEPENDENCIES" => TextOnly("pg_dependencies"),
        "GTSVECTOR" => TextOnly("gtsvector"),
        "REGCONFIG" => TextOnly("regconfig"),
        "REGDICTIONARY" => TextOnly("regdictionary"),
        "REGROLE" => TextOnly("regrole"),
        "REGCOLLATION" => TextOnly("regcollation"),
        "PG_BRIN_BLOOM_SUMMARY" => TextOnly("pg_brin_bloom_summary"),
        "PG_BRIN_MINMAX_MULTI_SUMMARY" => TextOnly("pg_brin_minmax_multi_summary"),
        "PG_MCV_LIST" => TextOnly("pg_mcv_list"),
        "CHAR" => Char,
        "NAME" => Name,
        "PG_NODE_TREE" => PgNodeTree,
        "REFCURSOR" => Refcursor,
        "TID" => Tid,
        "XID" => Xid,
        "CID" => Cid,
        "XID8" => Xid8,
        "TXID_SNAPSHOT" | "PG_SNAPSHOT" => Snapshot,
        "JSONPATH" => JsonPath,
        "PG_LSN" => PgLsn,
        "REGPROC" => RegProc,
        "REGTYPE" => RegType,
        "REGCLASS" => RegClass,
        "REGNAMESPACE" => RegNamespace,
        "OIDVECTOR" => OidVector,
        "INT2VECTOR" => Int2Vector,
        "SMALLINTARRAY" => ArrayOf(Elem::Int2, 1005),
        "INTEGERARRAY" => ArrayOf(Elem::Int4, 1007),
        "BIGINTARRAY" => ArrayOf(Elem::Int8, 1016),
        "OIDARRAY" => ArrayOf(Elem::Oid, 1028),
        "REALARRAY" => ArrayOf(Elem::Float4, 1021),
        "FLOATARRAY" => ArrayOf(Elem::Float8, 1022),
        "DECIMALARRAY" => ArrayOf(Elem::Numeric, 1231),
        "BOOLARRAY" => ArrayOf(Elem::Bool, 1000),
        "STRINGARRAY" => ArrayOf(Elem::Text, 1009),
        "CHARARRAY" => ArrayOf(Elem::Text, 18),
        "NAMEARRAY" => ArrayOf(Elem::Text, 19),
        "MONEYARRAY" => ArrayOf(Elem::Money, 791),
        "INTERVALARRAY" => ArrayOf(Elem::Interval, 1187),
        "REGTYPEARRAY" => ArrayOf(Elem::RegType, 2211),
        "xml[]" => ArrayOf(Elem::Xml, 143),
        "ACLITEMARRAY" => AclItemArray,
        // BVType.UNKNOWN and any type BVTYPE_TO_PGTYPE does not list: PG_UNKNOWN, str in text.
        _ => Unknown,
    }
}

/// Whether send_data_rows writes the type in binary when asked (its entry has a binary writer).
pub fn has_binary(bv: Bv) -> bool {
    !matches!(bv, Bv::Null | Bv::Unknown)
}

/// The element OID an ArrayOf sends: CHARARRAY and NAMEARRAY send text elements under "char"
/// and name.
fn array_elem_oid(elem: Elem, array_oid: i32) -> i32 {
    match array_oid {
        18 | 19 => array_oid,
        _ => elem.oid(),
    }
}

// --- catalog names (server._register_reg_types), handed over once by register_catalog_names ---

#[derive(Default)]
pub struct CatalogNames {
    pub procs: std::collections::HashMap<i128, String>,
    pub types: std::collections::HashMap<i128, String>,
    pub type_oids: std::collections::HashMap<String, i128>,
}

static NAMES: RwLock<Option<Arc<CatalogNames>>> = RwLock::new(None);

pub fn set_catalog_names(names: CatalogNames) {
    *NAMES.write().expect("catalog names lock") = Some(Arc::new(names));
}

fn catalog_names() -> R<Arc<CatalogNames>> {
    NAMES
        .read()
        .expect("catalog names lock")
        .clone()
        .ok_or_else(|| EncErr::Value("register_catalog_names was not called".into()))
}

// --- writers ---

fn text(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(s.as_bytes());
}

fn str_text(v: &PyVal, out: &mut Vec<u8>) {
    match v {
        PyVal::Str(s) => text(out, s),
        _ => text(out, &str_of(v)),
    }
}

fn struct_error(what: &str) -> EncErr {
    EncErr::Value(format!("struct.error: {what}"))
}

fn int_bytes(n: i128, width: usize, signed: bool, out: &mut Vec<u8>) -> R<()> {
    let bits = (width * 8) as u32;
    let (lo, hi) = if signed {
        (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1)
    } else {
        (0, (1i128 << bits) - 1)
    };
    if n < lo || n > hi {
        return Err(EncErr::Overflow("int too big to convert".into()));
    }
    out.extend_from_slice(&n.to_be_bytes()[16 - width..]);
    Ok(())
}

/// value.to_bytes(width, "big", signed=...): an int (or a bool) only.
fn to_bytes(v: &PyVal, width: usize, signed: bool, out: &mut Vec<u8>) -> R<()> {
    match v {
        PyVal::Int(n) => int_bytes(*n, width, signed, out),
        PyVal::BigInt(_) => Err(EncErr::Overflow("int too big to convert".into())),
        PyVal::Bool(b) => int_bytes(*b as i128, width, signed, out),
        _ => Err(type_error("object has no attribute 'to_bytes'", v)),
    }
}

/// struct.pack("!i"/"!q"/...) of an int.
fn pack_int(n: i128, width: usize, signed: bool, out: &mut Vec<u8>) -> R<()> {
    int_bytes(n, width, signed, out).map_err(|_| struct_error("argument out of range"))
}

fn f32_of(x: f64) -> R<f32> {
    let y = x as f32;
    if y.is_infinite() && !x.is_infinite() {
        return Err(EncErr::Overflow("float too large to pack with f format".into()));
    }
    Ok(y)
}

/// numeric_out: plain notation, its scale's digits after the point, no sign on zero.
pub fn numeric_out(d: &Dec, out: &mut Vec<u8>) {
    match d.special {
        DecSpecial::NaN => text(out, "NaN"),
        DecSpecial::Infinity => text(out, if d.neg { "-Infinity" } else { "Infinity" }),
        DecSpecial::Finite => numeric_text(d.neg, &d.digits, d.exp, out),
    }
}

/// numeric_send, of any decimal: NaN and the infinities as their sign words.
pub fn numeric_send(d: &Dec, out: &mut Vec<u8>) -> R<()> {
    let sign: u16 = match d.special {
        DecSpecial::Finite => return numeric_binary(d.neg, &d.digits, d.exp, out),
        DecSpecial::NaN => 0xC000,
        DecSpecial::Infinity if d.neg => 0xF000,
        DecSpecial::Infinity => 0xD000,
    };
    // numeric_send reads a special value's dscale through the short header's dscale bits: 0 for
    // NaN, 32 for either infinity.
    let dscale: u16 = (sign & 0x1F80) >> 7;
    out.extend_from_slice(&[0, 0, 0, 0]);
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    Ok(())
}

/// _array_text_element of an element's text.
pub fn array_text_element(s: &str, out: &mut Vec<u8>) {
    let needs = s.is_empty()
        || s.eq_ignore_ascii_case("NULL")
        || s.chars()
            .any(|c| matches!(c, '{' | '}' | '"' | ',' | '\\') || crate::is_py_space(c));
    if !needs {
        text(out, s);
        return;
    }
    out.push(b'"');
    for &b in s.as_bytes() {
        if b == b'\\' || b == b'"' {
            out.push(b'\\');
        }
        out.push(b);
    }
    out.push(b'"');
}

/// A list holding only lists that hold nothing at their ends: an empty array, which
/// PostgreSQL writes {} whatever its nesting.
fn empty_nested(v: &PyVal) -> bool {
    match v {
        PyVal::List(items) => items.iter().all(|x| matches!(x, PyVal::List(_)) && empty_nested(x)),
        _ => false,
    }
}

fn array_text(elem: Elem, v: &PyVal, out: &mut Vec<u8>) -> R<()> {
    if empty_nested(v) {
        text(out, "{}");
        return Ok(());
    }
    array_text_items(elem, v, out)
}

fn array_text_items(elem: Elem, v: &PyVal, out: &mut Vec<u8>) -> R<()> {
    out.push(b'{');
    for (k, item) in iter_items(v)?.iter().enumerate() {
        if k > 0 {
            out.push(b',');
        }
        match item {
            PyVal::None => text(out, "NULL"),
            PyVal::List(_) => array_text_items(elem, item, out)?,
            _ => {
                let mut cell = Vec::new();
                if elem == Elem::Bool {
                    // array_out writes a boolean element as t / f
                    cell.push(if truthy(item) { b't' } else { b'f' });
                } else {
                    convert(elem.bv(), false, item, &mut cell)?;
                }
                array_text_element(
                    std::str::from_utf8(&cell).map_err(|e| EncErr::Value(e.to_string()))?,
                    out,
                );
            }
        }
    }
    out.push(b'}');
    Ok(())
}

/// _array_shape: the length of each dimension, read down the first element, and the elements in
/// order.
fn array_shape(v: &PyVal) -> R<(Vec<usize>, Vec<PyVal>)> {
    let mut dims = Vec::new();
    let mut level = Some(v);
    while let Some(PyVal::List(items)) = level {
        dims.push(items.len());
        level = items.first();
    }
    let mut flat = vec![v.clone()];
    if dims.is_empty() {
        flat = iter_items(v)?;
    } else {
        flat = iter_items(&flat[0])?;
        for _ in 1..dims.len() {
            let mut next = Vec::new();
            for row in &flat {
                if *row == PyVal::None {
                    return Err(EncErr::Type("'NoneType' object is not iterable".into()));
                }
                next.extend(iter_items(row)?);
            }
            flat = next;
        }
    }
    let expected: usize = dims.iter().product();
    if flat.len() != expected {
        return Err(EncErr::Value("multidimensional array has unequal sublists".into()));
    }
    Ok((dims, flat))
}

/// array_send. An array with no elements has no dimensions; a vector (oidvector, int2vector) is
/// one dimension from 0.
fn array_binary(elem: Elem, elem_oid: i32, v: &PyVal, vector: bool, out: &mut Vec<u8>) -> R<()> {
    let (mut dims, flat) = array_shape(v)?;
    if !vector && dims.iter().product::<usize>() == 0 && !dims.is_empty() {
        dims.clear();
    }
    let has_null = flat.contains(&PyVal::None);
    out.extend_from_slice(&(dims.len() as i32).to_be_bytes());
    out.extend_from_slice(&(has_null as i32).to_be_bytes());
    out.extend_from_slice(&elem_oid.to_be_bytes());
    let lower: i32 = if vector { 0 } else { 1 };
    for d in &dims {
        out.extend_from_slice(&(*d as i32).to_be_bytes());
        out.extend_from_slice(&lower.to_be_bytes());
    }
    for item in &flat {
        match item {
            PyVal::None => out.extend_from_slice(&(-1i32).to_be_bytes()),
            // PostgreSQL holds no array whose elements are arrays and scalars: a list where an
            // element belongs is malformed (the product writes str(list) as the element).
            PyVal::List(_) => {
                return Err(EncErr::Value(
                    "malformed array literal: a sub-array as an element".into(),
                ))
            }
            _ => {
                let p = out.len();
                out.extend_from_slice(&[0; 4]);
                convert(elem.bv(), true, item, out)?;
                let n = (out.len() - p - 4) as i32;
                out[p..p + 4].copy_from_slice(&n.to_be_bytes());
            }
        }
    }
    Ok(())
}

/// _pg_clock of a datetime or time: its clock, and the offset of an aware value.
fn clock_of(v: &PyVal, out: &mut Vec<u8>) -> R<()> {
    match v {
        PyVal::Str(s) => text(out, s),
        PyVal::Time(us) => temporal::clock_text(*us, out),
        PyVal::DateTime(d) => {
            temporal::clock_text(clock_of_day(d), out);
            if let Some(off) = d.offset {
                temporal::offset_text(off, out);
            }
        }
        _ => return Err(type_error("object has no attribute 'hour'", v)),
    }
    Ok(())
}

fn clock_micros(v: &PyVal) -> R<i64> {
    match v {
        PyVal::Time(us) => Ok(*us),
        PyVal::DateTime(d) => Ok(clock_of_day(d)),
        _ => Err(type_error("object has no attribute 'hour'", v)),
    }
}

/// A datetime's time of day; a TIMETZ's own (TzRepr::Clock), which may be 24:00:00.
fn clock_of_day(d: &crate::pyval::DateTime) -> i64 {
    if d.tz == crate::pyval::TzRepr::Clock {
        d.local as i64
    } else {
        d.local.rem_euclid(DAY_US) as i64
    }
}

/// _interval_parts: (months, days, microseconds). A timedelta's days are its microseconds over a
/// day, truncated (the product divides in floating point; RESULTS.md).
pub fn interval_parts(v: &PyVal) -> R<(i64, i64, i64)> {
    let of_micros = |micros: i128| -> (i64, i64, i64) {
        let days = micros / DAY_US;
        (0, days as i64, (micros - days * DAY_US) as i64)
    };
    match v {
        PyVal::Interval(r) => Ok(r.parts()),
        PyVal::TimeDelta(us) => Ok(of_micros(*us)),
        PyVal::PdTimedelta(Some(ns)) => Ok(of_micros(ns.div_euclid(1000))),
        _ => Err(type_error("object has no attribute 'hours'", v)),
    }
}

/// json.dumps(value) (default separators, ensure_ascii, allow_nan).
pub fn json_dumps(v: &PyVal, out: &mut Vec<u8>) -> R<()> {
    match v {
        PyVal::None => text(out, "null"),
        PyVal::Bool(b) => text(out, if *b { "true" } else { "false" }),
        PyVal::Int(n) => text(out, itoa::Buffer::new().format(*n)),
        PyVal::BigInt(d) => text(out, d),
        PyVal::Float(x) => crate::fmt_num::float_repr(*x, out),
        PyVal::Str(s) => json::string(s, out),
        PyVal::List(items) | PyVal::Tuple(items) => {
            out.push(b'[');
            for (k, item) in items.iter().enumerate() {
                if k > 0 {
                    text(out, ", ");
                }
                json_dumps(item, out)?;
            }
            out.push(b']');
        }
        PyVal::Dict(items) => {
            out.push(b'{');
            for (k, (key, value)) in items.iter().enumerate() {
                if k > 0 {
                    text(out, ", ");
                }
                match key {
                    PyVal::Str(s) => json::string(s, out),
                    PyVal::Float(_) | PyVal::Int(_) | PyVal::BigInt(_) | PyVal::Bool(_) | PyVal::None => {
                        out.push(b'"');
                        json_dumps(key, out)?;
                        out.push(b'"');
                    }
                    _ => return Err(type_error("keys must be str, int, float, bool or None, not", key)),
                }
                text(out, ": ");
                json_dumps(value, out)?;
            }
            out.push(b'}');
        }
        _ => return Err(type_error("Object of this type is not JSON serializable", v)),
    }
    Ok(())
}

fn bytes_of(v: &PyVal) -> R<&[u8]> {
    match v {
        PyVal::Bytes(b) => Ok(b),
        _ => Err(type_error("a bytes-like object is required", v)),
    }
}

/// The value as the converters that read str(value) take it.
fn s(v: &PyVal) -> String {
    str_of(v)
}

/// Writes `v` (not None) as `bv` in text or binary.
pub fn convert(bv: Bv, binary: bool, v: &PyVal, out: &mut Vec<u8>) -> R<()> {
    let binary = binary && has_binary(bv);
    match (bv, binary) {
        (Bv::Null, _) => Err(EncErr::Type("'NoneType' object has no attribute 'encode'".into())),
        (Bv::Unknown, _) => {
            str_text(v, out);
            Ok(())
        }
        (Bv::Array, _) => {
            // "{" + ",".join(v) + "}"
            out.push(b'{');
            for (k, item) in iter_items(v)?.iter().enumerate() {
                if k > 0 {
                    out.push(b',');
                }
                match item {
                    PyVal::Str(x) => text(out, x),
                    _ => return Err(type_error("sequence item: expected str instance", item)),
                }
            }
            out.push(b'}');
            Ok(())
        }
        (Bv::Bool, false) => {
            out.push(if truthy(v) { b't' } else { b'f' });
            Ok(())
        }
        (Bv::Bool, true) => {
            out.push(truthy(v) as u8);
            Ok(())
        }
        (Bv::Int2 | Bv::Int4 | Bv::Int8 | Bv::Oid, false) => {
            str_text(v, out);
            Ok(())
        }
        (Bv::Int2, true) => to_bytes(v, 2, true, out),
        (Bv::Int4, true) => to_bytes(v, 4, true, out),
        (Bv::Int8, true) => to_bytes(v, 8, true, out),
        (Bv::Oid, true) => to_bytes(v, 4, false, out),
        (Bv::Float4, false) => float4_text(to_float(v)?, out),
        (Bv::Float4, true) => {
            out.extend_from_slice(&f32_of(packable_float(v)?)?.to_be_bytes());
            Ok(())
        }
        (Bv::Float8, false) => {
            float8_text(to_float(v)?, out);
            Ok(())
        }
        (Bv::Float8, true) => {
            out.extend_from_slice(&packable_float(v)?.to_be_bytes());
            Ok(())
        }
        (Bv::Numeric, false) => {
            match v {
                PyVal::Dec(d) => numeric_out(d, out),
                _ => str_text(v, out),
            }
            Ok(())
        }
        (Bv::Numeric, true) => numeric_send(&to_decimal(v)?, out),
        (Bv::Text, _) | (Bv::Xml, _) | (Bv::Name, _) | (Bv::PgNodeTree, _) | (Bv::Refcursor, _) => {
            str_text(v, out);
            Ok(())
        }
        (Bv::Bytes, false) => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            if let PyVal::Float(x) = v {
                // "\\x" + v.hex(): a float has a hex() of its own
                text(out, "\\x");
                text(out, &float_hex(*x));
                return Ok(());
            }
            let b = bytes_of(v)?;
            text(out, "\\x");
            for &c in b {
                out.push(HEX[(c >> 4) as usize]);
                out.push(HEX[(c & 15) as usize]);
            }
            Ok(())
        }
        (Bv::Bytes, true) => {
            out.extend_from_slice(bytes_of(v)?);
            Ok(())
        }
        (Bv::Date, false) => date_text(v, out),
        (Bv::Date, true) => {
            let (days, inf) = date_days(v).ok_or_else(|| type_error("object has no attribute 'toordinal'", v))?;
            if inf != 0 {
                out.extend_from_slice(&(if inf > 0 { i32::MAX } else { i32::MIN }).to_be_bytes());
                return Ok(());
            }
            int_bytes(days as i128 - 10957, 4, true, out)
        }
        (Bv::Timestamp | Bv::TimestampTz, false) => match v {
            PyVal::Date { inf, .. } if *inf != 0 => {
                text(out, if *inf > 0 { "infinity" } else { "-infinity" });
                Ok(())
            }
            PyVal::DateTime(d) if d.inf != 0 => {
                text(out, if d.inf > 0 { "infinity" } else { "-infinity" });
                Ok(())
            }
            PyVal::DateTime(d) => {
                let mut s = String::new();
                pyval_date(d.local.div_euclid(DAY_US) as i64, &mut s);
                text(out, &s);
                out.push(b' ');
                clock_of(v, out)
            }
            _ => Err(type_error("object has no attribute 'hour'", v)),
        },
        (Bv::Timestamp | Bv::TimestampTz, true) => match v {
            PyVal::Date { inf, .. } if *inf != 0 => {
                out.extend_from_slice(&(if *inf > 0 { i64::MAX } else { i64::MIN }).to_be_bytes());
                Ok(())
            }
            PyVal::DateTime(d) if d.inf != 0 => {
                out.extend_from_slice(&(if d.inf > 0 { i64::MAX } else { i64::MIN }).to_be_bytes());
                Ok(())
            }
            PyVal::DateTime(d) => {
                if bv == Bv::TimestampTz && d.offset.is_none() {
                    return Err(EncErr::Value("timestamptz value without a time zone".into()));
                }
                let micros = if d.offset.is_some() { d.utc() } else { d.local };
                int_bytes(micros - 946_684_800_000_000, 8, true, out)
            }
            _ => Err(type_error("unsupported operand type(s) for -", v)),
        },
        (Bv::Time | Bv::TimeTz, false) => clock_of(v, out),
        (Bv::Time, true) => int_bytes(clock_micros(v)? as i128, 8, false, out),
        (Bv::TimeTz, true) => {
            let (us, offset) = match v {
                PyVal::Str(s) => timetz_of_str(s)?,
                PyVal::DateTime(d) => (clock_micros(v)?, d.offset),
                PyVal::Time(us) => (*us, None),
                _ => return Err(type_error("object has no attribute 'utcoffset'", v)),
            };
            let offset = offset.ok_or_else(|| EncErr::Value("timetz value without a time zone".into()))?;
            out.extend_from_slice(&us.to_be_bytes());
            out.extend_from_slice(&(-offset).to_be_bytes());
            Ok(())
        }
        (Bv::Interval, false) => {
            let (m, d, us) = interval_parts(v)?;
            temporal::interval_text(m, d, us, out);
            Ok(())
        }
        (Bv::Interval, true) => {
            let (m, d, us) = interval_parts(v)?;
            out.extend_from_slice(&us.to_be_bytes());
            pack_int(d as i128, 4, true, out)?;
            pack_int(m as i128, 4, true, out)
        }
        (Bv::Json | Bv::Jsonb, _) => {
            if binary && bv == Bv::Jsonb {
                out.push(1);
            }
            match v {
                PyVal::Str(s) => text(out, s),
                _ => json_dumps(v, out)?,
            }
            Ok(())
        }
        (Bv::Uuid, false) => {
            str_text(v, out);
            Ok(())
        }
        (Bv::Uuid, true) => {
            match v {
                PyVal::Uuid(b) => out.extend_from_slice(b),
                _ => out.extend_from_slice(&uuid_bytes(&s(v))?),
            }
            Ok(())
        }
        (Bv::Inet, false) => {
            match v {
                PyVal::Inet(a) => inet_out(a, false, out),
                _ => str_text(v, out),
            }
            Ok(())
        }
        (Bv::Cidr, false) => {
            match v {
                PyVal::Inet(a) => inet_out(a, true, out),
                _ => str_text(v, out),
            }
            Ok(())
        }
        (Bv::Inet | Bv::Cidr, true) => {
            let a = match v {
                PyVal::Inet(a) if bv == Bv::Inet => *a,
                _ => parse_address(&s(v), bv == Bv::Cidr)?,
            };
            let len = if a.v6 { 16 } else { 4 };
            out.extend_from_slice(&[if a.v6 { 3 } else { 2 }, a.bits, (bv == Bv::Cidr) as u8, len]);
            out.extend_from_slice(&a.addr.to_be_bytes()[16 - len as usize..]);
            Ok(())
        }
        (Bv::MacAddr | Bv::MacAddr8, false) => {
            str_text(v, out);
            Ok(())
        }
        (Bv::MacAddr, true) => {
            let raw = mac_bytes(&s(v))?;
            if raw.len() != 6 {
                return Err(EncErr::Value("not a macaddr".into()));
            }
            out.extend_from_slice(&raw);
            Ok(())
        }
        (Bv::MacAddr8, true) => {
            let mut raw = mac_bytes(&s(v))?;
            if raw.len() == 6 {
                raw.splice(3..3, [0xff, 0xfe]);
            }
            if raw.len() != 8 {
                return Err(EncErr::Value("not a macaddr8".into()));
            }
            out.extend_from_slice(&raw);
            Ok(())
        }
        (Bv::Point, false) => match v {
            PyVal::List(items) | PyVal::Tuple(items) => {
                let (x, y) = point_parts(items)?;
                out.push(b'(');
                float8_text(x, out);
                out.push(b',');
                float8_text(y, out);
                out.push(b')');
                Ok(())
            }
            _ => {
                str_text(v, out);
                Ok(())
            }
        },
        (Bv::Point, true) => {
            let (x, y) = match v {
                PyVal::List(items) | PyVal::Tuple(items) => point_parts(items)?,
                _ => {
                    let t = s(v);
                    let t = t
                        .trim_matches(crate::is_py_space)
                        .trim_matches(|c| c == '(' || c == ')');
                    let parts: Vec<&str> = t.split(',').collect();
                    if parts.len() != 2 {
                        return Err(EncErr::Value("not enough values to unpack".into()));
                    }
                    (pyval::float_of_str(parts[0])?, pyval::float_of_str(parts[1])?)
                }
            };
            out.extend_from_slice(&x.to_be_bytes());
            out.extend_from_slice(&y.to_be_bytes());
            Ok(())
        }
        (Bv::Money, false) => match v {
            PyVal::Str(s) => {
                text(out, s);
                Ok(())
            }
            _ => money_text(money_cents(v)?, out),
        },
        (Bv::Money, true) => {
            out.extend_from_slice(&money_cents(v)?.to_be_bytes());
            Ok(())
        }
        (
            Bv::Line
            | Bv::Lseg
            | Bv::Box
            | Bv::Path
            | Bv::Polygon
            | Bv::Circle
            | Bv::Geometry
            | Bv::Geography
            | Bv::Int4Range
            | Bv::Int8Range
            | Bv::NumRange
            | Bv::TsRange
            | Bv::TstzRange
            | Bv::DateRange
            | Bv::Int4Multirange
            | Bv::Int8Multirange
            | Bv::NumMultirange
            | Bv::TsMultirange
            | Bv::TstzMultirange
            | Bv::DateMultirange
            | Bv::Bit
            | Bv::VarBit
            | Bv::TsVector
            | Bv::TsQuery
            | Bv::TextOnly(_)
            | Bv::Char
            | Bv::Tid
            | Bv::Xid
            | Bv::Cid
            | Bv::Xid8
            | Bv::Snapshot
            | Bv::JsonPath,
            false,
        ) => {
            str_text(v, out);
            Ok(())
        }
        (Bv::Line | Bv::Circle, true) => shape_binary(&s(v), 3, out),
        (Bv::Lseg | Bv::Box, true) => shape_binary(&s(v), 4, out),
        (Bv::Path, true) => {
            let t = s(v);
            let values = floats(&t)?;
            let closed = t.trim_matches(crate::is_py_space).starts_with('(');
            out.push(closed as u8);
            pack_int((values.len() / 2) as i128, 4, true, out)?;
            for x in values {
                out.extend_from_slice(&x.to_be_bytes());
            }
            Ok(())
        }
        (Bv::Polygon, true) => {
            let values = floats(&s(v))?;
            pack_int((values.len() / 2) as i128, 4, true, out)?;
            for x in values {
                out.extend_from_slice(&x.to_be_bytes());
            }
            Ok(())
        }
        (Bv::Geometry | Bv::Geography, true) => Err(EncErr::Value(
            "geometry and geography are sent in text format only".into(),
        )),
        (Bv::Int4Range | Bv::Int8Range | Bv::NumRange | Bv::TsRange | Bv::TstzRange | Bv::DateRange, true) => {
            range_binary(bv, &s(v), out)
        }
        (Bv::Int4Multirange, true) => multirange_binary(Bv::Int4Range, &s(v), out),
        (Bv::Int8Multirange, true) => multirange_binary(Bv::Int8Range, &s(v), out),
        (Bv::NumMultirange, true) => multirange_binary(Bv::NumRange, &s(v), out),
        (Bv::TsMultirange, true) => multirange_binary(Bv::TsRange, &s(v), out),
        (Bv::TstzMultirange, true) => multirange_binary(Bv::TstzRange, &s(v), out),
        (Bv::DateMultirange, true) => multirange_binary(Bv::DateRange, &s(v), out),
        (Bv::Bit | Bv::VarBit, true) => bit_binary(&s(v), out),
        (Bv::TsVector, true) => crate::tsearch::tsvector_send(&s(v), out),
        (Bv::TsQuery, true) => crate::tsearch::tsquery_send(&s(v), out),
        (Bv::TextOnly(name), true) => Err(EncErr::Value(format!("{name} is sent in text format only"))),
        (Bv::Char, true) => {
            for c in s(v).chars() {
                let u = c as u32;
                if u > 0xff {
                    return Err(EncErr::Value("'latin-1' codec can't encode character".into()));
                }
                out.push(u as u8);
            }
            Ok(())
        }
        (Bv::Tid, true) => {
            let t = s(v);
            let parts: Vec<&str> = t.trim_matches(|c| c == '(' || c == ')').split(',').collect();
            if parts.len() != 2 {
                return Err(EncErr::Value("not enough values to unpack".into()));
            }
            pack_int(pyval::int_of_str(parts[0])?, 4, false, out)?;
            pack_int(pyval::int_of_str(parts[1])?, 2, false, out)
        }
        (Bv::Xid | Bv::Cid, true) => int_bytes(to_int(v)?, 4, false, out),
        (Bv::Xid8, true) => int_bytes(to_int(v)?, 8, false, out),
        (Bv::Snapshot, true) => {
            let t = s(v);
            let parts: Vec<&str> = t.split(':').collect();
            if parts.len() != 3 {
                return Err(EncErr::Value("not enough values to unpack".into()));
            }
            let running: Vec<i128> = parts[2]
                .split(',')
                .filter(|x| !x.is_empty())
                .map(pyval::int_of_str)
                .collect::<R<_>>()?;
            pack_int(running.len() as i128, 4, true, out)?;
            pack_int(pyval::int_of_str(parts[0])?, 8, true, out)?;
            pack_int(pyval::int_of_str(parts[1])?, 8, true, out)?;
            for x in running {
                pack_int(x, 8, true, out)?;
            }
            Ok(())
        }
        (Bv::JsonPath, true) => {
            out.push(1);
            str_text(v, out);
            Ok(())
        }
        (Bv::PgLsn, false) => {
            let p = lsn_position(v)?;
            text(out, &format!("{}/{}", hex_upper(p >> 32), hex_upper(p & 0xFFFF_FFFF)));
            Ok(())
        }
        (Bv::PgLsn, true) => int_bytes(lsn_position(v)?, 8, false, out),
        (Bv::RegProc | Bv::RegType, false) => {
            match v {
                PyVal::Str(s) => text(out, s),
                PyVal::List(_) | PyVal::Dict(_) => {
                    return Err(type_error("unhashable type", v));
                }
                _ => {
                    let names = catalog_names()?;
                    let table = if bv == Bv::RegProc { &names.procs } else { &names.types };
                    let key = match v {
                        PyVal::Int(n) => Some(*n),
                        PyVal::Bool(b) => Some(*b as i128),
                        PyVal::Float(x) if x.fract() == 0.0 && x.is_finite() => Some(*x as i128),
                        // a Decimal equal to an int hashes as it does
                        PyVal::Dec(d) if d.special == DecSpecial::Finite && to_decimal_is_integral(d) => to_int(v).ok(),
                        _ => None,
                    };
                    match key {
                        Some(0) => text(out, "-"),
                        Some(k) if table.contains_key(&k) => text(out, &table[&k]),
                        _ => str_text(v, out),
                    }
                }
            }
            Ok(())
        }
        (Bv::RegProc, true) => match v {
            PyVal::Str(_) => Err(EncErr::Type(
                "a reg* value read as its text form has no binary form here".into(),
            )),
            _ => to_bytes(v, 4, false, out),
        },
        (Bv::RegType, true) => match v {
            PyVal::Str(name) => match catalog_names()?.type_oids.get(name) {
                Some(oid) => int_bytes(*oid, 4, false, out),
                None => Err(EncErr::Type(
                    "a reg* value read as its text form has no binary form here".into(),
                )),
            },
            _ => to_bytes(v, 4, false, out),
        },
        (Bv::RegClass | Bv::RegNamespace, false) => match v {
            PyVal::Str(s) => {
                text(out, s);
                Ok(())
            }
            _ => match dict_get(v, "name")? {
                PyVal::Str(s) => {
                    text(out, s);
                    Ok(())
                }
                other => Err(type_error("object has no attribute 'encode'", other)),
            },
        },
        (Bv::RegClass | Bv::RegNamespace, true) => match v {
            PyVal::Str(_) => Err(EncErr::Type(
                "a reg* value read as its text form has no binary form here".into(),
            )),
            _ => to_bytes(dict_get(v, "oid")?, 4, false, out),
        },
        (Bv::OidVector | Bv::Int2Vector, false) => {
            match v {
                PyVal::Str(s) => text(out, s),
                _ => {
                    let items: Vec<String> = iter_items(v)?.iter().map(str_of).collect();
                    text(out, &items.join(" "));
                }
            }
            Ok(())
        }
        (Bv::OidVector | Bv::Int2Vector, true) => {
            let elem = if bv == Bv::OidVector { Elem::Oid } else { Elem::Int2 };
            let values = match v {
                PyVal::Str(s) => PyVal::List(
                    s.split(crate::is_py_space)
                        .filter(|x| !x.is_empty())
                        .map(|x| pyval::int_of_str(x).map(PyVal::Int))
                        .collect::<R<_>>()?,
                ),
                _ => v.clone(),
            };
            array_binary(elem, elem.oid(), &values, true, out)
        }
        (Bv::ArrayOf(elem, _), false) => array_text(elem, v, out),
        (Bv::ArrayOf(elem, oid), true) => array_binary(elem, array_elem_oid(elem, oid), v, false, out),
        (Bv::AclItemArray, false) => array_text(Elem::Text, v, out),
        (Bv::AclItemArray, true) => Err(EncErr::Type(
            "aclitem has no binary form; request the column as text".into(),
        )),
    }
}

fn pyval_date(days: i64, s: &mut String) {
    let (y, m, d) = temporal::civil(days);
    s.push_str(&format!("{y:04}-{m:02}-{d:02}"));
}

/// _date_text: infinity, else value.isoformat().
fn date_text(v: &PyVal, out: &mut Vec<u8>) -> R<()> {
    let mut s = String::new();
    match v {
        PyVal::Date { inf, .. } | PyVal::DateTime(pyval::DateTime { inf, .. }) if *inf != 0 => {
            s.push_str(if *inf > 0 { "infinity" } else { "-infinity" });
        }
        PyVal::Date { days, .. } => pyval_date(*days, &mut s),
        PyVal::DateTime(d) => pyval::datetime_iso(d, 'T', &mut s),
        PyVal::Time(_) => pyval::py_str(v, &mut s),
        PyVal::PdTimedelta(Some(ns)) => pd_timedelta_iso(*ns, &mut s),
        PyVal::PdTimedelta(None) | PyVal::NaT => s.push_str("NaT"),
        _ => return Err(type_error("object has no attribute 'isoformat'", v)),
    }
    text(out, &s);
    Ok(())
}

fn to_decimal_is_integral(d: &Dec) -> bool {
    d.exp >= 0
        || d.digits[(d.digits.len() as i32 + d.exp).max(0) as usize..]
            .iter()
            .all(|&c| c == b'0')
}

/// pandas.Timedelta.isoformat()
fn pd_timedelta_iso(ns: i128, s: &mut String) {
    let day = DAY_US * 1000;
    let (days, rest) = (ns.div_euclid(day), ns.rem_euclid(day));
    let secs = rest / 1_000_000_000;
    let frac = rest % 1_000_000_000;
    s.push_str(&format!("P{days}DT{}H{}M{}", secs / 3600, secs / 60 % 60, secs % 60));
    if frac != 0 {
        s.push_str(format!(".{frac:09}").trim_end_matches('0'));
    }
    s.push('S');
}

/// float.hex()
fn float_hex(x: f64) -> String {
    if x.is_nan() {
        return "nan".into();
    }
    if x.is_infinite() {
        return (if x > 0.0 { "inf" } else { "-inf" }).into();
    }
    let sign = if x.is_sign_negative() { "-" } else { "" };
    if x == 0.0 {
        return format!("{sign}0x0.0p+0");
    }
    let bits = x.to_bits();
    let exp = ((bits >> 52) & 0x7ff) as i64;
    let mant = bits & ((1u64 << 52) - 1);
    let (lead, e) = if exp == 0 { (0, -1022) } else { (1, exp - 1023) };
    format!(
        "{sign}0x{lead}.{mant:013x}p{}{}",
        if e < 0 { "-" } else { "+" },
        e.abs()
    )
}

fn dict_get<'a>(v: &'a PyVal, key: &str) -> R<&'a PyVal> {
    match v {
        PyVal::Dict(items) => items
            .iter()
            .find(|(k, _)| matches!(k, PyVal::Str(s) if s == key))
            .map(|(_, v)| v)
            .ok_or_else(|| EncErr::Value(format!("KeyError: {key:?}"))),
        _ => Err(type_error("object is not subscriptable", v)),
    }
}

fn hex_upper(n: i128) -> String {
    if n < 0 {
        format!("-{:X}", -n)
    } else {
        format!("{n:X}")
    }
}

/// int(s, 16): whitespace around it, a sign, an optional 0x, underscores between digits.
fn int_hex(s: &str) -> R<i128> {
    let bad = || EncErr::Value(format!("invalid literal for int() with base 16: {s:?}"));
    let t = s.trim_matches(crate::is_py_space);
    let (neg, body) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, t),
    };
    let mut body = body;
    if body.len() > 1 && (body.starts_with("0x") || body.starts_with("0X")) {
        body = &body[2..];
        if let Some(rest) = body.strip_prefix('_') {
            body = rest;
        }
    }
    if body.is_empty() || body.starts_with('_') || body.ends_with('_') || body.contains("__") {
        return Err(bad());
    }
    let digits = body.replace('_', "");
    let v = i128::from_str_radix(&digits, 16).map_err(|_| bad())?;
    if digits.starts_with(['+', '-']) {
        return Err(bad());
    }
    Ok(if neg { -v } else { v })
}

fn lsn_position(v: &PyVal) -> R<i128> {
    match v {
        PyVal::Int(n) => Ok(*n),
        PyVal::Bool(b) => Ok(*b as i128),
        _ => {
            let t = s(v);
            let parts: Vec<&str> = t.split('/').collect();
            if parts.len() != 2 {
                return Err(EncErr::Value("not enough values to unpack".into()));
            }
            Ok((int_hex(parts[0])? << 32) | int_hex(parts[1])?)
        }
    }
}

fn point_parts(items: &[PyVal]) -> R<(f64, f64)> {
    if items.len() < 2 {
        return Err(EncErr::Value("IndexError: list index out of range".into()));
    }
    Ok((to_float(&items[0])?, to_float(&items[1])?))
}

/// bytes.fromhex of the text with its : - . separators dropped.
fn mac_bytes(t: &str) -> R<Vec<u8>> {
    let bad = || EncErr::Value("non-hexadecimal number found in fromhex() arg".into());
    let cleaned: Vec<char> = t.chars().filter(|c| !matches!(c, ':' | '-' | '.')).collect();
    let mut out = Vec::new();
    let mut k = 0;
    while k < cleaned.len() {
        if cleaned[k].is_ascii_whitespace() {
            k += 1;
            continue;
        }
        let hi = cleaned[k]
            .to_digit(16)
            .filter(|_| cleaned[k].is_ascii())
            .ok_or_else(bad)?;
        let lo = cleaned
            .get(k + 1)
            .and_then(|c| c.to_digit(16).filter(|_| c.is_ascii()))
            .ok_or_else(bad)?;
        out.push((hi * 16 + lo) as u8);
        k += 2;
    }
    Ok(out)
}

/// The float8s of a shape's text, as buenavista's _FLOAT8 finds them.
fn floats(t: &str) -> R<Vec<f64>> {
    let chars: Vec<char> = t.chars().collect();
    let is_digit = |c: char| c.is_ascii_digit() || (!c.is_ascii() && crate::int_ascii(c).is_ascii_digit());
    let digits_from = |mut k: usize| {
        while k < chars.len() && is_digit(chars[k]) {
            k += 1;
        }
        k
    };
    let starts_with = |k: usize, w: &str| {
        let wc: Vec<char> = w.chars().collect();
        chars.len() >= k + wc.len() && chars[k..k + wc.len()] == wc[..]
    };
    // the token at k (after an optional sign), its end
    let token_at = |k: usize| -> Option<usize> {
        for w in ["Infinity", "NaN", "inf"] {
            if starts_with(k, w) {
                return Some(k + w.chars().count());
            }
        }
        let mut e;
        if k < chars.len() && is_digit(chars[k]) {
            e = digits_from(k);
            if e < chars.len() && chars[e] == '.' {
                e = digits_from(e + 1);
            }
        } else if k + 1 < chars.len() && chars[k] == '.' && is_digit(chars[k + 1]) {
            e = digits_from(k + 1);
        } else {
            return None;
        }
        if e < chars.len() && (chars[e] == 'e' || chars[e] == 'E') {
            let mut x = e + 1;
            if x < chars.len() && (chars[x] == '-' || chars[x] == '+') {
                x += 1;
            }
            if x < chars.len() && is_digit(chars[x]) {
                e = digits_from(x);
            }
        }
        Some(e)
    };
    let mut values = Vec::new();
    let mut k = 0;
    while k < chars.len() {
        let signed = chars[k] == '-' || chars[k] == '+';
        let end = if signed { token_at(k + 1) } else { None }.or_else(|| token_at(k));
        match end {
            Some(e) => {
                let tok: String = chars[k..e].iter().collect();
                values.push(pyval::float_of_str(&tok)?);
                k = e;
            }
            None => k += 1,
        }
    }
    Ok(values)
}

fn shape_binary(t: &str, count: usize, out: &mut Vec<u8>) -> R<()> {
    let values = floats(t)?;
    if values.len() != count {
        return Err(EncErr::Value(format!("expected {count} numbers in {t:?}")));
    }
    for x in values {
        out.extend_from_slice(&x.to_be_bytes());
    }
    Ok(())
}

fn bit_binary(t: &str, out: &mut Vec<u8>) -> R<()> {
    if t.chars().any(|c| c != '0' && c != '1') && !t.trim_matches(['0', '1']).is_empty() {
        return Err(EncErr::Value(format!("not a bit string: {t:?}")));
    }
    let bits = t.as_bytes();
    pack_int(bits.len() as i128, 4, true, out)?;
    for chunk in bits.chunks(8) {
        let mut byte = 0u8;
        for (k, &b) in chunk.iter().enumerate() {
            if b == b'1' {
                byte |= 0x80 >> k;
            }
        }
        out.push(byte);
    }
    Ok(())
}

/// The money value's int8 cents: a number, or money's own text, rounded half away from zero.
/// A value past money's range is refused, in text as PostgreSQL refuses it.
fn money_cents(v: &PyVal) -> R<i64> {
    let d = match v {
        PyVal::Str(t) => {
            let cleaned = t.trim_matches(crate::is_py_space).replace([',', '$'], "");
            pyval::dec_of_str(&cleaned)?
        }
        _ => to_decimal(v)?,
    };
    if d.special != DecSpecial::Finite {
        return Err(EncErr::Value("[<class 'decimal.InvalidOperation'>]".into()));
    }
    // round to two places, half up
    let n = d.digits.len() as i32;
    let shift = d.exp + 2; // digits * 10^shift = cents
    let mut cents: i128 = 0;
    let out_of_range = || EncErr::Value("money out of range".into());
    if shift >= 0 {
        for &c in &d.digits {
            cents = cents
                .checked_mul(10)
                .and_then(|x| x.checked_add((c - b'0') as i128))
                .ok_or_else(out_of_range)?;
        }
        for _ in 0..shift {
            cents = cents.checked_mul(10).ok_or_else(out_of_range)?;
        }
    } else {
        let keep = (n + shift).max(0) as usize;
        for &c in &d.digits[..keep] {
            cents = cents
                .checked_mul(10)
                .and_then(|x| x.checked_add((c - b'0') as i128))
                .ok_or_else(out_of_range)?;
        }
        let first = if keep < d.digits.len() { d.digits[keep] } else { b'0' };
        let first = if (n + shift) < 0 { b'0' } else { first };
        if first >= b'5' {
            cents += 1;
        }
    }
    if d.neg {
        cents = -cents;
    }
    i64::try_from(cents).map_err(|_| out_of_range())
}

/// cash_out under lc_monetary en_US.UTF-8: -$1,234.57
fn money_text(cents: i64, out: &mut Vec<u8>) -> R<()> {
    let a = cents.unsigned_abs();
    let (dollars, rem) = (a / 100, a % 100);
    let digits = dollars.to_string();
    let mut grouped = String::new();
    for (k, c) in digits.chars().enumerate() {
        if k > 0 && (digits.len() - k) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(c);
    }
    text(out, &format!("{}${grouped}.{rem:02}", if cents < 0 { "-" } else { "" }));
    Ok(())
}

// --- addresses ---

/// inet_out (cidr_out with `cidr`): the address, then /bits unless an inet's prefix is the whole
/// address.
fn inet_out(a: &Inet, cidr: bool, out: &mut Vec<u8>) {
    let mut s = String::new();
    pyval::inet_addr_text(a, true, &mut s);
    let max = if a.v6 { 128 } else { 32 };
    if cidr || a.bits != max {
        s.push('/');
        s.push_str(&a.bits.to_string());
    }
    text(out, &s);
}

fn parse_v4(t: &str) -> Option<u32> {
    let parts: Vec<&str> = t.split('.').collect();
    if parts.len() != 4 {
        return None;
    }
    let mut v: u32 = 0;
    for p in parts {
        if p.is_empty() || p.len() > 3 || !p.bytes().all(|c| c.is_ascii_digit()) || (p.len() > 1 && p.starts_with('0'))
        {
            return None;
        }
        let o: u32 = p.parse().ok()?;
        if o > 255 {
            return None;
        }
        v = v << 8 | o;
    }
    Some(v)
}

fn parse_v6(t: &str) -> Option<u128> {
    // a scope id (%name) is not part of the packed address
    let t = t.split('%').next()?;
    let mut parts: Vec<&str> = t.split(':').collect();
    if parts.len() < 3 {
        return None;
    }
    let mut tail: Vec<u16> = Vec::new();
    if let Some(last) = parts.last() {
        if last.contains('.') {
            let v4 = parse_v4(last)?;
            parts.pop();
            tail = vec![(v4 >> 16) as u16, v4 as u16];
            parts.push("x"); // placeholder, removed below
        }
    }
    let had_v4 = !tail.is_empty();
    if had_v4 {
        parts.pop();
    }
    let max_parts = if had_v4 { 7 } else { 8 };
    let n = parts.len();
    if n > max_parts + 1 {
        return None;
    }
    let skip = (1..n - 1).filter(|&k| parts[k].is_empty()).collect::<Vec<_>>();
    let mut words: Vec<u16> = Vec::new();
    let hextet = |p: &str| -> Option<u16> {
        if p.is_empty() || p.len() > 4 || !p.bytes().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        u16::from_str_radix(p, 16).ok()
    };
    let want = 8 - tail.len();
    if skip.len() > 1 {
        return None;
    }
    if let Some(&k) = skip.first() {
        let (mut lo, mut hi) = (0, n);
        if parts[0].is_empty() {
            if k != 1 {
                return None;
            }
            lo = 1;
        }
        if parts[n - 1].is_empty() {
            if k != n - 2 {
                return None;
            }
            hi = n - 1;
        }
        let head: Vec<u16> = parts[lo..k].iter().map(|p| hextet(p)).collect::<Option<_>>()?;
        let rest: Vec<u16> = parts[k + 1..hi].iter().map(|p| hextet(p)).collect::<Option<_>>()?;
        if head.len() + rest.len() >= want {
            return None;
        }
        words.extend(head);
        words.extend(std::iter::repeat_n(0, want - words.len() - rest.len()));
        words.extend(rest);
    } else {
        if n != want || parts.iter().any(|p| p.is_empty()) {
            return None;
        }
        words = parts.iter().map(|p| hextet(p)).collect::<Option<_>>()?;
    }
    words.extend(tail);
    Some(words.iter().fold(0u128, |acc, &w| acc << 16 | w as u128))
}

/// ipaddress.ip_interface (or, with `network`, ip_network, strict) of a text: IPv4 first.
fn parse_address(t: &str, network: bool) -> R<Inet> {
    let bad = || EncErr::Value(format!("{t:?} does not appear to be an IPv4 or IPv6 address"));
    let pieces: Vec<&str> = t.split('/').collect();
    if pieces.len() > 2 {
        return Err(bad());
    }
    let (addr, prefix) = (pieces[0], pieces.get(1).copied());
    let (v6, value, max) = if let Some(v) = parse_v4(addr) {
        (false, v as u128, 32u8)
    } else if let Some(v) = parse_v6(addr) {
        (true, v, 128u8)
    } else {
        return Err(bad());
    };
    let bits = match prefix {
        None => max,
        Some(p) if !p.is_empty() && p.bytes().all(|c| c.is_ascii_digit()) => {
            let b: u32 = p.parse().map_err(|_| bad())?;
            if b > max as u32 {
                return Err(bad());
            }
            b as u8
        }
        Some(p) if !v6 => {
            // a netmask, or a hostmask
            let m = parse_v4(p).ok_or_else(bad)?;
            let ones = m.leading_ones();
            if m == u32::MAX.checked_shl(32 - ones).unwrap_or(0) {
                ones as u8
            } else {
                let inv = !m;
                let ones = inv.leading_ones();
                if inv == u32::MAX.checked_shl(32 - ones).unwrap_or(0) {
                    ones as u8
                } else {
                    return Err(bad());
                }
            }
        }
        _ => return Err(bad()),
    };
    if network {
        let host = if bits == max { 0 } else { (1u128 << (max - bits)) - 1 };
        if value & host != 0 {
            return Err(EncErr::Value(format!("{t:?} has host bits set")));
        }
    }
    Ok(Inet { v6, addr: value, bits })
}

// --- ranges ---

/// _range_bounds: (lower, upper, flags), each bound None when unbounded.
fn range_bounds(t: &str) -> R<(Option<String>, Option<String>, u8)> {
    let t = t.trim_matches(crate::is_py_space);
    if t.to_lowercase() == "empty" {
        return Ok((None, None, 0x01));
    }
    let chars: Vec<char> = t.chars().collect();
    if chars.is_empty() {
        return Err(EncErr::Value("IndexError: string index out of range".into()));
    }
    let mut flags = if chars[0] == '[' { 0x02 } else { 0 } | if chars[chars.len() - 1] == ']' { 0x04 } else { 0 };
    let inner: &[char] = if chars.len() >= 2 {
        &chars[1..chars.len() - 1]
    } else {
        &[]
    };
    let (mut bounds, mut current, mut quoted, mut i) = (Vec::new(), String::new(), false, 0);
    while i < inner.len() {
        let ch = inner[i];
        if ch == '\\' && i + 1 < inner.len() {
            current.push(inner[i + 1]);
            i += 2;
            continue;
        }
        if ch == '"' {
            quoted = !quoted;
        } else if ch == ',' && !quoted {
            bounds.push(std::mem::take(&mut current));
        } else {
            current.push(ch);
        }
        i += 1;
    }
    bounds.push(current);
    if bounds.len() != 2 {
        return Err(EncErr::Value(format!("not a range: {t:?}")));
    }
    let take = |b: &str| {
        let s = b.trim_matches(crate::is_py_space);
        (!s.is_empty()).then(|| s.to_string())
    };
    let (lower, upper) = (take(&bounds[0]), take(&bounds[1]));
    if lower.is_none() {
        flags = (flags | 0x08) & !0x02;
    }
    if upper.is_none() {
        flags = (flags | 0x10) & !0x04;
    }
    Ok((lower, upper, flags))
}

/// A date, timestamp or timestamptz bound as PostgreSQL writes it (ISO, a BC suffix, infinity):
/// (days or microseconds since 2000-01-01, infinity).
struct Stamp {
    days: i64,
    micros: i64,
    offset: Option<i32>,
    has_time: bool,
    inf: i32,
}

fn parse_stamp(t: &str) -> R<Stamp> {
    let bad = || EncErr::Value(format!("Invalid isoformat string: {t:?}"));
    match t {
        "infinity" => {
            return Ok(Stamp {
                days: 0,
                micros: 0,
                offset: None,
                has_time: false,
                inf: 1,
            })
        }
        "-infinity" => {
            return Ok(Stamp {
                days: 0,
                micros: 0,
                offset: None,
                has_time: false,
                inf: -1,
            })
        }
        _ => {}
    }
    let (body, bc) = match t.strip_suffix(" BC") {
        Some(b) => (b, true),
        None => (t, false),
    };
    let b = body.as_bytes();
    let num = |from: usize, len: usize| -> R<i64> {
        let part = body.get(from..from + len).ok_or_else(bad)?;
        if !part.bytes().all(|c| c.is_ascii_digit()) {
            return Err(bad());
        }
        part.parse().map_err(|_| bad())
    };
    let ylen = b.iter().position(|&c| c == b'-').ok_or_else(bad)?;
    if ylen < 4 {
        return Err(bad());
    }
    let mut year = num(0, ylen)?;
    if b.get(ylen) != Some(&b'-') || b.get(ylen + 3) != Some(&b'-') {
        return Err(bad());
    }
    let month = num(ylen + 1, 2)?;
    let day = num(ylen + 4, 2)?;
    if bc {
        year = 1 - year;
    }
    if !(1..=12).contains(&month) || day < 1 || day > temporal::month_days(year, month as u32) {
        return Err(bad());
    }
    let days = temporal::days_from_civil(year, month as u32, day as u32) - 10957;
    let mut p = ylen + 6;
    if p == b.len() {
        let micros = days * 86_400_000_000;
        return Ok(Stamp {
            days,
            micros,
            offset: None,
            has_time: false,
            inf: 0,
        });
    }
    if b[p] != b' ' && b[p] != b'T' {
        return Err(bad());
    }
    p += 1;
    let hour = num(p, 2)?;
    p += 2;
    let (mut minute, mut second, mut frac) = (0, 0, 0i64);
    if b.get(p) == Some(&b':') {
        minute = num(p + 1, 2)?;
        p += 3;
        if b.get(p) == Some(&b':') {
            second = num(p + 1, 2)?;
            p += 3;
            if b.get(p) == Some(&b'.') {
                let start = p + 1;
                let mut e = start;
                while e < b.len() && b[e].is_ascii_digit() {
                    e += 1;
                }
                if e == start {
                    return Err(bad());
                }
                let digits = &body[start..e.min(start + 6)];
                frac = digits.parse::<i64>().map_err(|_| bad())? * 10i64.pow(6 - digits.len() as u32);
                p = e;
            }
        }
    }
    if hour > 24 || minute > 59 || second > 60 {
        return Err(bad());
    }
    let mut offset = None;
    if p < b.len() {
        match b[p] {
            b'Z' if p + 1 == b.len() => offset = Some(0),
            b'+' | b'-' => {
                let sign = if b[p] == b'-' { -1 } else { 1 };
                let rest = &body[p + 1..];
                let fields: Vec<&str> = rest.split(':').collect();
                if fields.is_empty() || fields.len() > 3 || fields.iter().any(|f| f.len() != 2) {
                    return Err(bad());
                }
                let mut secs = 0;
                for (k, f) in fields.iter().enumerate() {
                    let n: i32 = f.parse().map_err(|_| bad())?;
                    secs += n * [3600, 60, 1][k];
                }
                offset = Some(sign * secs);
            }
            _ => return Err(bad()),
        }
    }
    let micros = days * 86_400_000_000 + ((hour * 60 + minute) * 60 + second) * 1_000_000 + frac
        - offset.unwrap_or(0) as i64 * 1_000_000;
    Ok(Stamp {
        days,
        micros,
        offset,
        has_time: true,
        inf: 0,
    })
}

/// A range bound in its element's binary form.
fn bound_binary(bv: Bv, b: &str, out: &mut Vec<u8>) -> R<()> {
    match bv {
        Bv::Int4Range => pack_int(pyval::int_of_str(b)?, 4, true, out),
        Bv::Int8Range => pack_int(pyval::int_of_str(b)?, 8, true, out),
        Bv::NumRange => numeric_send(&pyval::dec_of_str(b)?, out),
        Bv::DateRange => {
            let st = parse_stamp(b)?;
            if st.has_time {
                return Err(EncErr::Value(format!("Invalid isoformat string: {b:?}")));
            }
            let v = match st.inf {
                0 => i32::try_from(st.days).map_err(|_| EncErr::Value("date out of range".into()))?,
                1 => i32::MAX,
                _ => i32::MIN,
            };
            out.extend_from_slice(&v.to_be_bytes());
            Ok(())
        }
        Bv::TsRange | Bv::TstzRange => {
            let st = parse_stamp(b)?;
            let v = match st.inf {
                0 => {
                    if bv == Bv::TstzRange && st.offset.is_none() {
                        return Err(EncErr::Value("timestamptz value without a time zone".into()));
                    }
                    st.micros
                }
                1 => i64::MAX,
                _ => i64::MIN,
            };
            out.extend_from_slice(&v.to_be_bytes());
            Ok(())
        }
        _ => unreachable!("a range type"),
    }
}

/// range_send: flags, then each bounded bound length-prefixed.
fn range_binary(bv: Bv, t: &str, out: &mut Vec<u8>) -> R<()> {
    let (lower, upper, flags) = range_bounds(t)?;
    out.push(flags);
    for b in [lower, upper].into_iter().flatten() {
        let p = out.len();
        out.extend_from_slice(&[0; 4]);
        bound_binary(bv, &b, out)?;
        let n = (out.len() - p - 4) as i32;
        out[p..p + 4].copy_from_slice(&n.to_be_bytes());
    }
    Ok(())
}

/// _multirange_ranges: each range's text of a multirange's text.
fn multirange_ranges(t: &str) -> Vec<String> {
    let trimmed: Vec<char> = t.trim_matches(crate::is_py_space).chars().collect();
    let inner: &[char] = if trimmed.len() >= 2 {
        &trimmed[1..trimmed.len() - 1]
    } else {
        &[]
    };
    let (mut ranges, mut start, mut quoted) = (Vec::new(), None::<usize>, false);
    for (at, &ch) in inner.iter().enumerate() {
        if ch == '"' {
            quoted = !quoted;
        } else if quoted {
            continue;
        } else if (ch == '[' || ch == '(') && start.is_none() {
            start = Some(at);
        } else if ch == ']' || ch == ')' {
            ranges.push(inner[start.unwrap_or(0)..at + 1].iter().collect());
            start = None;
        }
    }
    ranges
}

fn multirange_binary(range: Bv, t: &str, out: &mut Vec<u8>) -> R<()> {
    let ranges = multirange_ranges(t);
    pack_int(ranges.len() as i128, 4, true, out)?;
    for r in ranges {
        let p = out.len();
        out.extend_from_slice(&[0; 4]);
        range_binary(range, &r, out)?;
        let n = (out.len() - p - 4) as i32;
        out[p..p + 4].copy_from_slice(&n.to_be_bytes());
    }
    Ok(())
}

/// time.fromisoformat of a timetz's text (its offset given minutes when it has none): the clock
/// and the offset.
fn timetz_of_str(t: &str) -> R<(i64, Option<i32>)> {
    let st = parse_stamp(&format!("2000-01-01 {t}"))?;
    let has_full = {
        let b = t.as_bytes();
        b.len() >= 6
            && (b[b.len() - 6] == b'+' || b[b.len() - 6] == b'-')
            && b[b.len() - 5].is_ascii_digit()
            && b[b.len() - 4].is_ascii_digit()
            && b[b.len() - 3] == b':'
            && b[b.len() - 2].is_ascii_digit()
            && b[b.len() - 1].is_ascii_digit()
    };
    let _ = has_full;
    let off = st.offset;
    let local = st.micros + off.unwrap_or(0) as i64 * 1_000_000;
    Ok((local.rem_euclid(86_400_000_000), off))
}
