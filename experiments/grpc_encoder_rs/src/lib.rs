// Experimental Rust port of the gRPC result encoder: an engine's Arrow result to the serialized
// {Type} / {Type}Batch response messages the product writes (provisa/grpc/server.py: Arrow ->
// column_values rows -> _proto_value per cell -> msg_cls(**kwargs) -> SerializeToString). Nothing in
// the product calls this; experiments/grpc_encoder_rs/test_compare.py holds it to the Python path.

mod descriptor;
mod pyfmt;

use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::cast;
use arrow::datatypes::*;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyNotImplementedError, PyOverflowError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

use descriptor::*;
use pyfmt::{DAY_US, EPOCH_ORDINAL, MAX_DAYS, MAX_US, MIN_DAYS, MIN_US};

#[derive(Debug, Clone)]
pub enum EncErr {
    Overflow(String),
    Value(String),
    Type(String),
    Unsupported(String),
}

impl From<EncErr> for PyErr {
    fn from(e: EncErr) -> PyErr {
        match e {
            EncErr::Overflow(m) => PyOverflowError::new_err(m),
            EncErr::Value(m) => PyValueError::new_err(m),
            EncErr::Type(m) => PyTypeError::new_err(m),
            EncErr::Unsupported(m) => PyNotImplementedError::new_err(m),
        }
    }
}

pub type R<T> = Result<T, EncErr>;

fn unsupported<T>(what: impl Into<String>) -> R<T> {
    Err(EncErr::Unsupported(what.into()))
}

// ---- reading Arrow values as to_pylist (core.ir_arrow.column_values) hands them over ----------

/// One cell as the Python value column_values makes of it.
enum V<'a> {
    Int(i128),
    Float(f64),
    Bool(bool),
    /// A decimal128: unscaled value and scale (Decimal exponent -scale).
    Dec(i128, i32),
    Str(&'a str),
    Bin(&'a [u8]),
    /// A date: days from 1970-01-01 (an infinity already its date.max / date.min).
    Date(i64),
    /// A datetime: its wall clock in microseconds from 1970-01-01, and its UTC offset when aware.
    Ts(i64, Option<i32>),
    /// A time of day in microseconds.
    Time(i64),
    /// An interval (timedelta / relativedelta) as ir_types.interval_parts holds it.
    Interval(i64, i64, i64),
}

/// A column's (or a struct child's) values.
enum Reader {
    Null,
    Bool(BooleanArray),
    I64(Arc<dyn Fn(usize) -> i128 + Send + Sync>),
    F64(Arc<dyn Fn(usize) -> f64 + Send + Sync>),
    Dec(Decimal128Array, i32),
    Utf8(StringArray),
    LargeUtf8(LargeStringArray),
    Utf8View(StringViewArray),
    Bin(BinaryArray),
    LargeBin(LargeBinaryArray),
    FixedBin(FixedSizeBinaryArray),
    /// Date values, and whether they carry the IR infinities (a column's own values only).
    Date32(ScalarBuffer<i32>, bool),
    /// Timestamp values, microseconds per unit, the offset when aware, and whether the column
    /// carries the IR infinities (µs only: ir_arrow._with_infinities).
    Ts(ScalarBuffer<i64>, i64, Option<i32>, bool),
    Time(Arc<dyn Fn(usize) -> i64 + Send + Sync>),
    Duration(ScalarBuffer<i64>, i64),
    Mdn(IntervalMonthDayNanoArray),
    Struct(StructArray),
    List(OffsetBuffer<i32>, Box<Reader>, Option<NullBuffer>),
    LargeList(OffsetBuffer<i64>, Box<Reader>, Option<NullBuffer>),
    /// A type the port does not read: refused at the first non-null value.
    Refused(String),
}

fn int_reader<T: ArrowPrimitiveType>(a: &ArrayRef) -> Reader
where
    T::Native: Into<i128>,
{
    let v = a.as_primitive::<T>().values().clone();
    Reader::I64(Arc::new(move |i| v[i].into()))
}

fn fixed_offset(tz: &str) -> Option<i32> {
    if tz == "UTC" || tz == "+00:00" {
        return Some(0);
    }
    let b = tz.as_bytes();
    if b.len() != 6 || (b[0] != b'+' && b[0] != b'-') || b[3] != b':' {
        return None;
    }
    let num = |s: &str| s.parse::<i32>().ok();
    let (h, m) = (num(&tz[1..3])?, num(&tz[4..6])?);
    let s = (h * 60 + m) * 60;
    Some(if b[0] == b'-' { -s } else { s })
}

fn unit_us(unit: &TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => Some(1_000_000),
        TimeUnit::Millisecond => Some(1_000),
        TimeUnit::Microsecond => Some(1),
        TimeUnit::Nanosecond => None,
    }
}

impl Reader {
    /// Inside a struct or a list: only a column's own values carry the IR infinities
    /// (column_values); there, to_pylist reads the sentinel as the out-of-range value it is.
    fn nested(a: &ArrayRef) -> Reader {
        Self::read(a, false)
    }

    fn read(a: &ArrayRef, top: bool) -> Reader {
        use DataType as D;
        match a.data_type() {
            D::Null => Reader::Null,
            D::Boolean => Reader::Bool(a.as_boolean().clone()),
            D::Int8 => int_reader::<Int8Type>(a),
            D::Int16 => int_reader::<Int16Type>(a),
            D::Int32 => int_reader::<Int32Type>(a),
            D::Int64 => int_reader::<Int64Type>(a),
            D::UInt8 => int_reader::<UInt8Type>(a),
            D::UInt16 => int_reader::<UInt16Type>(a),
            D::UInt32 => int_reader::<UInt32Type>(a),
            D::UInt64 => int_reader::<UInt64Type>(a),
            D::Float32 => {
                let v = a.as_primitive::<Float32Type>().values().clone();
                Reader::F64(Arc::new(move |i| v[i] as f64))
            }
            D::Float64 => {
                let v = a.as_primitive::<Float64Type>().values().clone();
                Reader::F64(Arc::new(move |i| v[i]))
            }
            D::Decimal128(_, s) => Reader::Dec(a.as_primitive::<Decimal128Type>().clone(), *s as i32),
            D::Utf8 => Reader::Utf8(a.as_string::<i32>().clone()),
            D::LargeUtf8 => Reader::LargeUtf8(a.as_string::<i64>().clone()),
            D::Utf8View => Reader::Utf8View(a.as_string_view().clone()),
            D::Binary => Reader::Bin(a.as_binary::<i32>().clone()),
            D::LargeBinary => Reader::LargeBin(a.as_binary::<i64>().clone()),
            D::FixedSizeBinary(_) => Reader::FixedBin(a.as_fixed_size_binary().clone()),
            D::Date32 => Reader::Date32(a.as_primitive::<Date32Type>().values().clone(), top),
            D::Timestamp(unit, tz) => {
                let Some(mult) = unit_us(unit) else {
                    return Reader::Refused("nanosecond timestamps".into());
                };
                let offset = match tz {
                    None => None,
                    Some(tz) => match fixed_offset(tz) {
                        Some(o) => Some(o),
                        None => return Reader::Refused(format!("timestamps in time zone {tz:?}")),
                    },
                };
                let values = match unit {
                    TimeUnit::Second => a.as_primitive::<TimestampSecondType>().values().clone(),
                    TimeUnit::Millisecond => a.as_primitive::<TimestampMillisecondType>().values().clone(),
                    _ => a.as_primitive::<TimestampMicrosecondType>().values().clone(),
                };
                Reader::Ts(values, mult, offset, top && *unit == TimeUnit::Microsecond)
            }
            D::Time32(TimeUnit::Second) => {
                let v = a.as_primitive::<Time32SecondType>().values().clone();
                Reader::Time(Arc::new(move |i| v[i] as i64 * 1_000_000))
            }
            D::Time32(TimeUnit::Millisecond) => {
                let v = a.as_primitive::<Time32MillisecondType>().values().clone();
                Reader::Time(Arc::new(move |i| v[i] as i64 * 1_000))
            }
            D::Time64(TimeUnit::Microsecond) => {
                let v = a.as_primitive::<Time64MicrosecondType>().values().clone();
                Reader::Time(Arc::new(move |i| v[i]))
            }
            D::Duration(unit) => {
                let Some(mult) = unit_us(unit) else {
                    return Reader::Refused("nanosecond durations".into());
                };
                let values = match unit {
                    TimeUnit::Second => a.as_primitive::<DurationSecondType>().values().clone(),
                    TimeUnit::Millisecond => a.as_primitive::<DurationMillisecondType>().values().clone(),
                    _ => a.as_primitive::<DurationMicrosecondType>().values().clone(),
                };
                Reader::Duration(values, mult)
            }
            D::Interval(IntervalUnit::MonthDayNano) => Reader::Mdn(a.as_primitive::<IntervalMonthDayNanoType>().clone()),
            D::Struct(_) => Reader::Struct(a.as_struct().clone()),
            D::List(_) => {
                let l = a.as_list::<i32>();
                Reader::List(l.offsets().clone(), Box::new(Reader::nested(l.values())), l.values().logical_nulls())
            }
            D::LargeList(_) => {
                let l = a.as_list::<i64>();
                Reader::LargeList(l.offsets().clone(), Box::new(Reader::nested(l.values())), l.values().logical_nulls())
            }
            D::Dictionary(_, value_type) => match cast(a, value_type) {
                Ok(plain) => Reader::read(&plain, top),
                Err(e) => Reader::Refused(format!("dictionary column: {e}")),
            },
            other => Reader::Refused(format!("Arrow type {other}")),
        }
    }

    /// The value at a non-null row, raising where to_pylist / column_values raises.
    fn get(&self, i: usize) -> R<V<'_>> {
        Ok(match self {
            Reader::Null => unreachable!("a null column has no non-null row"),
            Reader::Bool(a) => V::Bool(a.value(i)),
            Reader::I64(f) => V::Int(f(i)),
            Reader::F64(f) => V::Float(f(i)),
            Reader::Dec(a, s) => V::Dec(a.value(i), *s),
            Reader::Utf8(a) => V::Str(a.value(i)),
            Reader::LargeUtf8(a) => V::Str(a.value(i)),
            Reader::Utf8View(a) => V::Str(a.value(i)),
            Reader::Bin(a) => V::Bin(a.value(i)),
            Reader::LargeBin(a) => V::Bin(a.value(i)),
            Reader::FixedBin(a) => V::Bin(a.value(i)),
            Reader::Date32(v, infinities) => {
                let d = v[i] as i64;
                if *infinities && d == i32::MAX as i64 {
                    V::Date(MAX_DAYS) // ir_types.infinite_date: date.max
                } else if *infinities && d == -(i32::MAX as i64) {
                    V::Date(MIN_DAYS)
                } else if (MIN_DAYS..=MAX_DAYS).contains(&d) {
                    V::Date(d)
                } else {
                    return Err(EncErr::Value(format!("date32 {d} is out of range")));
                }
            }
            Reader::Ts(v, mult, offset, infinities) => {
                let raw = v[i];
                if *infinities && raw.unsigned_abs() == i64::MAX as u64 {
                    // ir_types.infinite_timestamp: datetime.max / min, in UTC when aware.
                    let edge = if raw > 0 { MAX_US } else { MIN_US };
                    return Ok(V::Ts(edge, offset.map(|_| 0)));
                }
                let utc = (raw as i128) * (*mult as i128);
                let local = utc + offset.unwrap_or(0) as i128 * 1_000_000;
                let range = MIN_US as i128..=MAX_US as i128;
                if !range.contains(&utc) || !range.contains(&local) {
                    return Err(EncErr::Value(format!("timestamp {raw} is out of range")));
                }
                V::Ts(local as i64, *offset)
            }
            Reader::Time(f) => {
                let us = f(i);
                if !(0..DAY_US).contains(&us) {
                    return Err(EncErr::Value(format!("time {us}us is out of range")));
                }
                V::Time(us)
            }
            Reader::Duration(v, mult) => {
                // a timedelta; ir_types.interval_parts of it
                let t = v[i] as i128 * *mult as i128;
                let (lo, hi) = (-999_999_999i128 * DAY_US as i128, 1_000_000_000i128 * DAY_US as i128 - 1);
                if t < lo || t > hi {
                    return Err(EncErr::Overflow(format!("duration {t}us is out of timedelta's range")));
                }
                let a = t.abs();
                let (days, us) = ((a / DAY_US as i128) as i64, (a % DAY_US as i128) as i64);
                if t < 0 {
                    V::Interval(0, -days, -us)
                } else {
                    V::Interval(0, days, us)
                }
            }
            Reader::Mdn(a) => {
                let v = a.value(i);
                let micros = (v.nanoseconds as i64).div_euclid(1000);
                // ir_types.interval_value: a month-less one is first a timedelta, which raises
                // when its normalized days leave timedelta's range.
                if v.months == 0 && (v.days as i64).abs() <= 999_999_999 {
                    let days = v.days as i64 + micros.div_euclid(DAY_US);
                    if days.abs() > 999_999_999 {
                        return Err(EncErr::Overflow("interval is out of timedelta's range".into()));
                    }
                }
                V::Interval(v.months as i64, v.days as i64, micros)
            }
            Reader::Struct(_) | Reader::List(..) | Reader::LargeList(..) => {
                return unsupported("a nested value in a scalar field (the product sends its Python repr)")
            }
            Reader::Refused(what) => return unsupported(what.clone()),
        })
    }

    /// Reads every non-null value under row `i` (struct children, list items), as to_pylist
    /// converts a column whether or not the encoder then uses it.
    fn check(&self, i: usize) -> R<()> {
        match self {
            Reader::Null => Ok(()),
            Reader::Struct(s) => {
                for c in s.columns() {
                    if c.is_valid(i) {
                        Reader::nested(c).check(i)?;
                    }
                }
                Ok(())
            }
            Reader::List(o, items, nulls) => check_items(o[i] as usize, o[i + 1] as usize, items, nulls),
            Reader::LargeList(o, items, nulls) => check_items(o[i] as usize, o[i + 1] as usize, items, nulls),
            _ => self.get(i).map(|_| ()),
        }
    }
}

fn check_items(start: usize, end: usize, items: &Reader, nulls: &Option<NullBuffer>) -> R<()> {
    for j in start..end {
        if nulls.as_ref().map_or(true, |n| n.is_valid(j)) {
            items.check(j)?;
        }
    }
    Ok(())
}

// ---- _proto_value: a Python value to the proto field's type ----------------------------------

fn py_type(v: &V) -> &'static str {
    match v {
        V::Int(_) => "int",
        V::Float(_) => "float",
        V::Bool(_) => "bool",
        V::Dec(..) => "decimal.Decimal",
        V::Str(_) => "str",
        V::Bin(_) => "bytes",
        V::Date(_) => "datetime.date",
        V::Ts(..) => "datetime.datetime",
        V::Time(_) => "datetime.time",
        V::Interval(..) => "interval",
    }
}

fn type_err<T>(what: &str, v: &V) -> R<T> {
    Err(EncErr::Type(format!("{what}: {}", py_type(v))))
}

/// float(value)
fn to_float(v: &V) -> R<f64> {
    Ok(match v {
        V::Int(x) => *x as f64,
        V::Float(x) => *x,
        V::Bool(b) => *b as u8 as f64,
        V::Dec(u, s) => format!("{u}e{}", -s).parse::<f64>().unwrap(),
        V::Str(_) | V::Bin(_) => return unsupported("float() of text"),
        _ => return type_err("float() argument", v),
    })
}

/// int(value.toordinal()) for a date or datetime, else int(value)
fn to_int(v: &V) -> R<i128> {
    Ok(match v {
        V::Int(x) => *x,
        V::Bool(b) => *b as i128,
        V::Float(x) => {
            if x.is_nan() {
                return Err(EncErr::Value("cannot convert float NaN to integer".into()));
            }
            if x.is_infinite() {
                return Err(EncErr::Overflow("cannot convert float infinity to integer".into()));
            }
            if x.abs() >= 1.7e38 {
                return Err(EncErr::Value("Value out of range".into()));
            }
            x.trunc() as i128
        }
        V::Dec(u, s) => {
            if *s >= 0 {
                u / 10i128.pow(*s as u32)
            } else {
                10i128
                    .checked_pow((-s) as u32)
                    .and_then(|p| u.checked_mul(p))
                    .ok_or_else(|| EncErr::Value("Value out of range".into()))?
            }
        }
        V::Date(d) => (d + EPOCH_ORDINAL) as i128,
        V::Ts(local, _) => (local.div_euclid(DAY_US) + EPOCH_ORDINAL) as i128,
        V::Str(_) | V::Bin(_) => return unsupported("int() of text"),
        V::Time(_) | V::Interval(..) => return type_err("int() argument", v),
    })
}

/// bool(value)
fn to_bool(v: &V) -> R<bool> {
    Ok(match v {
        V::Int(x) => *x != 0,
        V::Float(x) => *x != 0.0,
        V::Bool(b) => *b,
        V::Dec(u, _) => *u != 0,
        V::Str(s) => !s.is_empty(),
        V::Bin(b) => !b.is_empty(),
        V::Date(_) | V::Ts(..) | V::Time(_) => true,
        V::Interval(..) => return unsupported("bool() of an interval"),
    })
}

/// str(value), an interval its ISO 8601 duration (string fields only).
fn write_str(v: &V, out: &mut Vec<u8>) -> R<()> {
    match v {
        V::Str(s) => out.extend_from_slice(s.as_bytes()),
        V::Int(x) => pyfmt::int_str(*x, out),
        V::Float(x) => pyfmt::float_str(*x, out),
        V::Bool(b) => out.extend_from_slice(if *b { b"True" } else { b"False" }),
        V::Dec(u, s) => pyfmt::decimal128_str(*u, *s, out),
        V::Date(d) => pyfmt::date_str(*d, out),
        V::Ts(local, off) => pyfmt::datetime_str(*local, *off, out),
        V::Time(us) => pyfmt::time_str(*us, out),
        V::Interval(m, d, us) => pyfmt::iso8601_duration(*m, *d, *us, out),
        V::Bin(_) => return unsupported("str() of bytes"),
    }
    Ok(())
}

// ---- protobuf wire ------------------------------------------------------------------------------

#[inline]
fn varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline]
fn tag(number: u32, wire_type: u8, out: &mut Vec<u8>) {
    varint(((number as u64) << 3) | wire_type as u64, out);
}

/// Writes what `body` appends as a length-delimited field: the body is written in place, then
/// moved up by the length prefix's size.
#[inline]
fn delimited(number: u32, out: &mut Vec<u8>, body: impl FnOnce(&mut Vec<u8>) -> R<()>) -> R<()> {
    tag(number, 2, out);
    let start = out.len();
    body(out)?;
    let len = out.len() - start;
    let mut prefix = [0u8; 10];
    let mut n = 0;
    let mut l = len as u64;
    loop {
        prefix[n] = (l as u8 & 0x7f) | if l >= 0x80 { 0x80 } else { 0 };
        n += 1;
        l >>= 7;
        if l == 0 {
            break;
        }
    }
    out.resize(out.len() + n, 0);
    out.copy_within(start..start + len, start + n);
    out[start..start + n].copy_from_slice(&prefix[..n]);
    Ok(())
}

fn out_of_range<T>(x: i128) -> R<T> {
    Err(EncErr::Value(format!("Value out of range: {x}")))
}

/// Timestamp.FromDatetime: a naive datetime is UTC, an aware one is moved to UTC.
fn write_timestamp(number: u32, v: &V, out: &mut Vec<u8>) -> R<()> {
    let (local, off) = match v {
        V::Ts(local, off) => (*local, *off),
        V::Str(_) => return unsupported("datetime.fromisoformat of text"),
        _ => return type_err("fromisoformat: argument must be str", v),
    };
    let utc = local - off.unwrap_or(0) as i64 * 1_000_000;
    if !(MIN_US..=MAX_US).contains(&utc) {
        return Err(EncErr::Overflow("date value out of range".into()));
    }
    let (seconds, nanos) = (utc.div_euclid(1_000_000), utc.rem_euclid(1_000_000) * 1000);
    delimited(number, out, |out| {
        if seconds != 0 {
            out.push(0x08);
            varint(seconds as u64, out);
        }
        if nanos != 0 {
            out.push(0x10);
            varint(nanos as u64, out);
        }
        Ok(())
    })
}

fn write_scalar(f: &FieldDesc, v: &V, out: &mut Vec<u8>) -> R<()> {
    let n = f.number;
    match f.ftype {
        T_DOUBLE => {
            let x = to_float(v)?;
            if f.presence || x.to_bits() != 0 {
                tag(n, 1, out);
                out.extend_from_slice(&x.to_le_bytes());
            }
        }
        T_FLOAT => {
            let x = to_float(v)? as f32;
            if f.presence || x.to_bits() != 0 {
                tag(n, 5, out);
                out.extend_from_slice(&x.to_le_bytes());
            }
        }
        T_INT32 | T_INT64 | T_UINT32 | T_UINT64 => {
            let x = to_int(v)?;
            let ok = match f.ftype {
                T_INT32 => i32::try_from(x).is_ok(),
                T_INT64 => i64::try_from(x).is_ok(),
                T_UINT32 => u32::try_from(x).is_ok(),
                _ => u64::try_from(x).is_ok(),
            };
            if !ok {
                return out_of_range(x);
            }
            if f.presence || x != 0 {
                tag(n, 0, out);
                // a negative int32 / int64 is its 64-bit two's complement
                varint(if x < 0 { x as i64 as u64 } else { x as u64 }, out);
            }
        }
        T_BOOL => {
            let b = to_bool(v)?;
            if f.presence || b {
                tag(n, 0, out);
                out.push(b as u8);
            }
        }
        T_STRING | T_BYTES => {
            if let (T_BYTES, V::Interval(..)) = (f.ftype, v) {
                return unsupported("str() of a timedelta / relativedelta");
            }
            let mark = out.len();
            delimited(n, out, |out| match v {
                V::Bin(b) if f.ftype == T_BYTES => {
                    out.extend_from_slice(b);
                    Ok(())
                }
                _ => write_str(v, out),
            })?;
            if !f.presence && out.len() == mark + varint_len(n as u64 * 8) + 1 {
                out.truncate(mark); // an implicit-presence empty string is not written
            }
        }
        other => return unsupported(format!("proto field type {other}")),
    }
    Ok(())
}

fn varint_len(v: u64) -> usize {
    ((64 - (v | 1).leading_zeros() as usize) + 6) / 7
}

// ---- plans: which column feeds which field, and how --------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// Query{Type}: server._kwargs_for -> msg_cls(**kwargs). A column with no field raises.
    Row,
    /// A relationship-shaped row: server._dict_row_to_message. Unknown keys are skipped, nested
    /// dicts (struct) and lists of them (list<struct>) become nested and repeated messages.
    Node,
}

enum Kind {
    Scalar(FieldDesc),
    Timestamp(u32),
    /// A message field from a struct column (one message) or a list<struct> column (one per
    /// non-null item).
    Message(u32, Box<Plan>),
    /// Not written; its values are still read (to_pylist converts every column).
    Unused,
    /// Raises at its first non-null value.
    Fails(EncErr),
}

struct Col {
    nulls: Option<NullBuffer>,
    reader: Reader,
    kind: Kind,
}

struct Plan {
    cols: Vec<Col>,
}

const TIMESTAMP: &str = "google.protobuf.Timestamp";

impl Plan {
    fn build(pool: &Pool, msg: &MsgDesc, columns: Vec<(String, ArrayRef)>, mode: Mode, top: bool) -> R<Plan> {
        let mut seen = std::collections::HashSet::new();
        let mut cols = Vec::with_capacity(columns.len());
        for (name, array) in columns {
            if !seen.insert(name.clone()) {
                return unsupported(format!("two columns named {name:?}"));
            }
            let reader = Reader::read(&array, top);
            let nulls = array.logical_nulls();
            let kind = match (msg.field(&name), mode) {
                (None, Mode::Row) => Kind::Fails(EncErr::Value(format!(
                    "Protocol message {} has no \"{name}\" field.",
                    msg.full_name.rsplit('.').next().unwrap()
                ))),
                (None, Mode::Node) => Kind::Unused,
                (Some(f), _) if f.ftype == T_MESSAGE && f.type_name == TIMESTAMP => {
                    if f.repeated {
                        return unsupported("a repeated Timestamp field");
                    }
                    Kind::Timestamp(f.number)
                }
                (Some(f), Mode::Row) if f.ftype == T_MESSAGE => Kind::Fails(EncErr::Unsupported(
                    "a message field in a Query row (msg_cls(**kwargs) builds it from the raw value)".into(),
                )),
                (Some(f), Mode::Node) if f.ftype == T_MESSAGE => Self::nested(pool, f, &array)?,
                (Some(f), _) if f.repeated => Kind::Fails(EncErr::Unsupported("a repeated scalar field".into())),
                (Some(f), _) => Kind::Scalar(f.clone()),
            };
            cols.push(Col { nulls, reader, kind });
        }
        // the fields in number order, as upb writes them
        let number = |c: &Col| match &c.kind {
            Kind::Scalar(f) => f.number,
            Kind::Timestamp(n) | Kind::Message(n, _) => *n,
            _ => u32::MAX,
        };
        cols.sort_by_key(number);
        Ok(Plan { cols })
    }

    /// _dict_row_to_message's nested branch: a dict becomes the field's message (a repeated
    /// field's only one), a list its messages from the items that are dicts; anything else is
    /// not a dict and is dropped.
    fn nested(pool: &Pool, f: &FieldDesc, array: &ArrayRef) -> R<Kind> {
        let sub = pool.message(&f.type_name)?;
        let struct_plan = |s: &StructArray| -> R<Box<Plan>> {
            let children = s
                .fields()
                .iter()
                .zip(s.columns())
                .map(|(fl, c)| (fl.name().clone(), c.clone()))
                .collect();
            Ok(Box::new(Plan::build(pool, sub, children, Mode::Node, false)?))
        };
        Ok(match array.data_type() {
            DataType::Struct(_) => Kind::Message(f.number, struct_plan(array.as_struct())?),
            DataType::List(_) | DataType::LargeList(_) if f.repeated => {
                let values = match array.data_type() {
                    DataType::List(_) => array.as_list::<i32>().values().clone(),
                    _ => array.as_list::<i64>().values().clone(),
                };
                match values.data_type() {
                    DataType::Struct(_) => Kind::Message(f.number, struct_plan(values.as_struct())?),
                    _ => Kind::Unused, // no item is a dict
                }
            }
            DataType::Map(..) => Kind::Fails(EncErr::Unsupported("a map column".into())),
            _ => Kind::Unused,
        })
    }

    fn encode(&self, i: usize, out: &mut Vec<u8>) -> R<()> {
        for c in &self.cols {
            if c.nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                continue;
            }
            match &c.kind {
                Kind::Scalar(f) => write_scalar(f, &c.reader.get(i)?, out)?,
                Kind::Timestamp(n) => write_timestamp(*n, &c.reader.get(i)?, out)?,
                Kind::Message(n, sub) => match &c.reader {
                    Reader::Struct(_) => delimited(*n, out, |out| sub.encode(i, out))?,
                    Reader::List(o, _, nulls) => {
                        encode_items(*n, o[i] as usize, o[i + 1] as usize, nulls, sub, out)?
                    }
                    Reader::LargeList(o, _, nulls) => {
                        encode_items(*n, o[i] as usize, o[i + 1] as usize, nulls, sub, out)?
                    }
                    _ => unreachable!("a message plan reads a struct or a list"),
                },
                Kind::Unused => c.reader.check(i)?,
                Kind::Fails(e) => {
                    c.reader.check(i)?;
                    return Err(e.clone());
                }
            }
        }
        Ok(())
    }
}

fn encode_items(
    number: u32,
    start: usize,
    end: usize,
    nulls: &Option<NullBuffer>,
    sub: &Plan,
    out: &mut Vec<u8>,
) -> R<()> {
    for j in start..end {
        if nulls.as_ref().map_or(true, |n| n.is_valid(j)) {
            delimited(number, out, |out| sub.encode(j, out))?;
        }
    }
    Ok(())
}

// ---- the Python entry point ---------------------------------------------------------------------

/// Encodes an Arrow result as the response messages of one RPC.
///
/// ``Encoder(descriptor_set, message, columns, mode="row", batch_message=None, batch_rows=0)``:
/// ``descriptor_set`` is the serialized FileDescriptorSet the service was compiled from;
/// ``message`` the row message's full name (``provisa.v1.Orders``); ``columns`` the field name
/// each result column is written to (the server's ``out_cols``). ``mode`` is ``"row"``
/// (Query{Type}) or ``"node"`` (relationship-shaped rows, ``_dict_row_to_message``). With
/// ``batch_message`` (``provisa.v1.OrdersBatch``) rows are written ``batch_rows`` to a message, as
/// Query{Type}Batch does, across Arrow batches.
#[pyclass]
struct Encoder {
    pool: Arc<Pool>,
    message: String,
    columns: Vec<String>,
    mode: Mode,
    /// (the batch message's rows field number, rows per batch message)
    batch: Option<(u32, usize)>,
    pending: Vec<u8>,
    pending_rows: usize,
}

/// Messages laid end to end, each ended at an offset.
struct Messages {
    data: Vec<u8>,
    ends: Vec<usize>,
    /// Whether each message is preceded by its gRPC length-prefixed frame header.
    framed: bool,
}

impl Messages {
    fn new(framed: bool) -> Self {
        Messages { data: Vec::new(), ends: Vec::new(), framed }
    }

    fn begin(&mut self) -> usize {
        if self.framed {
            self.data.extend_from_slice(&[0; 5]);
        }
        self.data.len()
    }

    fn end(&mut self, start: usize) {
        if self.framed {
            let len = (self.data.len() - start) as u32;
            self.data[start - 4..start].copy_from_slice(&len.to_be_bytes());
        }
        self.ends.push(self.data.len());
    }

    fn push(&mut self, message: &[u8]) {
        let start = self.begin();
        self.data.extend_from_slice(message);
        self.end(start);
    }
}

impl Encoder {
    fn encode_batch(&mut self, batch: &RecordBatch, out: &mut Messages) -> R<()> {
        if batch.num_columns() != self.columns.len() {
            return Err(EncErr::Value(format!(
                "{} columns named for a batch of {}",
                self.columns.len(),
                batch.num_columns()
            )));
        }
        let columns = self.columns.iter().cloned().zip(batch.columns().iter().cloned()).collect();
        let msg = self.pool.message(&self.message)?;
        let plan = Plan::build(&self.pool, msg, columns, self.mode, true)?;
        for i in 0..batch.num_rows() {
            match self.batch {
                None => {
                    let start = out.begin();
                    plan.encode(i, &mut out.data)?;
                    out.end(start);
                }
                Some((rows_field, per)) => {
                    delimited(rows_field, &mut self.pending, |buf| plan.encode(i, buf))?;
                    self.pending_rows += 1;
                    if self.pending_rows == per {
                        out.push(&self.pending);
                        self.pending.clear();
                        self.pending_rows = 0;
                    }
                }
            }
        }
        Ok(())
    }

    fn flush(&mut self, out: &mut Messages) {
        if self.pending_rows > 0 {
            out.push(&self.pending);
            self.pending.clear();
            self.pending_rows = 0;
        }
    }

    fn run(&mut self, py: Python<'_>, batch: Option<&Bound<'_, PyAny>>, framed: bool) -> PyResult<Messages> {
        let batch = batch.map(RecordBatch::from_pyarrow_bound).transpose()?;
        py.allow_threads(|| {
            let mut out = Messages::new(framed);
            match &batch {
                Some(b) => self.encode_batch(b, &mut out)?,
                None => self.flush(&mut out),
            }
            Ok(out)
        })
    }
}

fn as_list<'py>(py: Python<'py>, m: Messages) -> PyResult<Bound<'py, PyList>> {
    let mut start = 0;
    let items: Vec<Bound<'py, PyBytes>> = m
        .ends
        .iter()
        .map(|&end| {
            let b = PyBytes::new(py, &m.data[start..end]);
            start = end;
            b
        })
        .collect();
    PyList::new(py, items)
}

#[pymethods]
impl Encoder {
    #[new]
    #[pyo3(signature = (descriptor_set, message, columns, mode="row", batch_message=None, batch_rows=0))]
    fn new(
        descriptor_set: &[u8],
        message: String,
        columns: Vec<String>,
        mode: &str,
        batch_message: Option<String>,
        batch_rows: usize,
    ) -> PyResult<Self> {
        let pool = Pool::parse(descriptor_set)?;
        let row = pool.message(&message)?.full_name.clone();
        let mode = match mode {
            "row" => Mode::Row,
            "node" => Mode::Node,
            other => return Err(PyValueError::new_err(format!("mode {other:?}"))),
        };
        let batch = match batch_message {
            None => None,
            Some(name) => {
                let b = pool.message(&name)?;
                let rows = b
                    .fields
                    .iter()
                    .find(|f| f.repeated && f.ftype == T_MESSAGE && f.type_name == row)
                    .ok_or_else(|| PyValueError::new_err(format!("{name} has no repeated {row} field")))?;
                if batch_rows == 0 {
                    return Err(PyValueError::new_err("batch_rows must be positive"));
                }
                Some((rows.number, batch_rows))
            }
        };
        Ok(Encoder {
            pool: Arc::new(pool),
            message: row,
            columns,
            mode,
            batch,
            pending: Vec::new(),
            pending_rows: 0,
        })
    }

    /// The serialized messages this Arrow batch completes.
    fn encode<'py>(&mut self, py: Python<'py>, batch: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyList>> {
        let m = self.run(py, Some(batch), false)?;
        as_list(py, m)
    }

    /// The messages this Arrow batch completes, each in its gRPC length-prefixed frame
    /// (flag byte 0, big-endian u32 length), in one buffer.
    fn encode_framed<'py>(&mut self, py: Python<'py>, batch: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        let m = self.run(py, Some(batch), true)?;
        Ok(PyBytes::new(py, &m.data))
    }

    /// The last, partly filled batch message, if any (Query{Type}Batch's final yield).
    fn finish<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let m = self.run(py, None, false)?;
        as_list(py, m)
    }

    fn finish_framed<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let m = self.run(py, None, true)?;
        Ok(PyBytes::new(py, &m.data))
    }
}

#[pymodule]
fn grpc_encoder_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Encoder>()?;
    Ok(())
}
