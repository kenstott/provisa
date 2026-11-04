// REQ-2194, REQ-1965: the gRPC result encoder (provisa._encoders.grpc). An engine's Arrow batches
// and the organisation's compiled service in, the serialized {Type} / {Type}Batch response
// messages out, with no Python value in between: grpc/server.py hands each result's
// arrow_batches() to an Encoder and yields the bytes it returns. Each value is written as
// REQ-1965 has gRPC write it: an array, struct or map in a string field as JSON, bytes in a
// string field as PostgreSQL's hex, PostgreSQL's infinities as "infinity" / "-infinity" in a
// string field and as Timestamp's maximum / minimum in a Timestamp field, and a value beyond a
// float or double field's range refused by name. tests/unit/grpc_encoder holds it to the Python
// path it replaced (python_ref.py).

mod descriptor;
mod json;
mod parse;
mod pyfmt;
mod pyrows;
// A named zone's offset at an instant, as zoneinfo finds it: the pgwire crate's zone code, read
// in by path so both encoders answer from one tz database (J498).
#[path = "../../../pgwire/src/tz.rs"]
mod tz;
#[path = "../../../pgwire/src/tz_rules.rs"]
#[allow(dead_code)] // TZDB_VERSION: the pgwire module reports it
mod tz_rules;
/// What tz.rs reads of the pgwire crate's temporal module.
mod temporal {
    pub use crate::pyfmt::civil;
}

use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::cast;
use arrow::datatypes::*;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyNotImplementedError, PyOverflowError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

use descriptor::*;
use json::{Mask, J};
use pyrows::{Cells, Pv};
use pyfmt::{Clock, DAY_US, EPOCH_ORDINAL, MAX_DAYS, MAX_US, MIN_DAYS, MIN_US};

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

// ---- reading Arrow values as core.ir_arrow.column_values hands them over ----------------------

/// The zone of a timestamp column: none (naive), a fixed offset, or a named zone.
#[derive(Clone, Copy)]
enum Zone {
    Naive,
    Fixed(i32),
    Named(tz::Zone),
}

impl Zone {
    /// ir_arrow._zone: "UTC" and ±HH:MM are fixed offsets, any other name an IANA zone.
    fn parse(name: &str) -> R<Zone> {
        if name == "UTC" || name == "Z" {
            return Ok(Zone::Fixed(0));
        }
        let b = name.as_bytes();
        if b.len() == 6 && (b[0] == b'+' || b[0] == b'-') && b[3] == b':' {
            let num = |s: &str| s.parse::<i32>().ok();
            if let (Some(h), Some(m)) = (num(&name[1..3]), num(&name[4..6])) {
                let s = (h * 60 + m) * 60;
                return Ok(Zone::Fixed(if b[0] == b'-' { -s } else { s }));
            }
        }
        tz::Zone::named(name)
            .map(Zone::Named)
            .ok_or_else(|| EncErr::Value(format!("No time zone found with key {name}")))
    }

    /// The UTC offset in seconds at an instant within years 1..=9999, None for a naive column.
    fn offset(&self, utc_us: i64) -> Option<i32> {
        match self {
            Zone::Naive => None,
            Zone::Fixed(o) => Some(*o),
            Zone::Named(z) => Some(z.offset_at(utc_us.div_euclid(1_000_000))),
        }
    }
}

/// A datetime: its wall clock, its UTC offset when aware, and PostgreSQL's infinity (1, -1) when
/// it is ir_types.infinite_timestamp's sentinel (0 otherwise).
#[derive(Clone, Copy)]
struct Ts {
    local: Clock,
    offset: Option<i32>,
    inf: i8,
}

impl Ts {
    /// The instant in microseconds from the epoch and the nanoseconds past it.
    fn utc(&self) -> (i64, u32) {
        (self.local.us - self.offset.unwrap_or(0) as i64 * 1_000_000, self.local.sub_ns)
    }
}

#[derive(Clone, Copy)]
enum Dec {
    D128(i128, i32),
    D256(i256, i32),
}

impl Dec {
    fn is_zero(&self) -> bool {
        match self {
            Dec::D128(u, _) => *u == 0,
            Dec::D256(u, _) => *u == i256::ZERO,
        }
    }

    /// str(Decimal): the coefficient and exponent pyarrow hands back (trailing zeros kept).
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Dec::D128(u, s) => pyfmt::decimal128_str(*u, *s, out),
            Dec::D256(u, s) => {
                let text = u.to_string();
                let (neg, digits) = match text.strip_prefix('-') {
                    Some(d) => (true, d),
                    None => (false, text.as_str()),
                };
                pyfmt::decimal_str(neg, digits.as_bytes(), -s, out);
            }
        }
    }

    /// float(Decimal), correctly rounded.
    fn to_f64(&self) -> f64 {
        match self {
            Dec::D128(u, s) => format!("{u}e{}", -s).parse::<f64>().unwrap(),
            Dec::D256(u, s) => format!("{u}e{}", -s).parse::<f64>().unwrap(),
        }
    }

    /// int(Decimal): truncated toward zero; None past i128 (out of every proto integer's range).
    fn to_i128(&self) -> Option<i128> {
        match *self {
            Dec::D128(u, s) if s >= 0 => Some(if s > 38 { 0 } else { u / 10i128.pow(s as u32) }),
            Dec::D128(u, s) => 10i128.checked_pow((-s) as u32).and_then(|p| u.checked_mul(p)),
            Dec::D256(u, s) if s >= 0 => {
                if s > 76 {
                    return Some(0);
                }
                u.checked_div(i256::from_i128(10).checked_pow(s as u32)?)?.to_i128()
            }
            Dec::D256(u, s) => u.checked_mul(i256::from_i128(10).checked_pow((-s) as u32)?)?.to_i128(),
        }
    }
}

/// One cell as the Python value the product's path made of it.
#[derive(Clone, Copy)]
enum V<'a> {
    Int(i128),
    /// An int past i128, as its digits (from JSON only).
    BigInt(&'a str),
    Float(f64),
    Bool(bool),
    Dec(Dec),
    Str(&'a str),
    Bin(&'a [u8]),
    /// A date: days from 1970-01-01 (an infinity at date.max / date.min) and its infinity.
    Date(i64, i8),
    Ts(Ts),
    Time(Clock),
    /// An interval (timedelta / relativedelta) as ir_types.interval_parts holds it.
    Interval(i64, i64, i64),
    /// A struct (dict), a list or a map (a list of (key, value) tuples) at a row of its reader.
    Nested(&'a Reader, usize),
    /// An array or object json.loads gave.
    Json(&'a J),
    /// A list or dict of a row result's cell.
    Py(&'a Pv),
}

/// A column's (or a nested child's) values.
enum Reader {
    Null,
    Bool(BooleanArray),
    Int(Arc<dyn Fn(usize) -> i128 + Send + Sync>),
    Float(Arc<dyn Fn(usize) -> f64 + Send + Sync>),
    Dec128(Decimal128Array, i32),
    Dec256(Decimal256Array, i32),
    Utf8(StringArray),
    LargeUtf8(LargeStringArray),
    Utf8View(StringViewArray),
    Bin(BinaryArray),
    LargeBin(LargeBinaryArray),
    BinView(BinaryViewArray),
    FixedBin(FixedSizeBinaryArray),
    /// Date values, and whether they carry the IR infinities (a column's own values only).
    Date32(ScalarBuffer<i32>, bool),
    Date64(ScalarBuffer<i64>),
    /// Timestamp values, nanoseconds per unit, the zone, and whether the column carries the IR
    /// infinities (µs only: ir_arrow._with_infinities).
    Ts(ScalarBuffer<i64>, i64, Zone, bool),
    /// A time of day in microseconds (a nanosecond time truncated, as to_pylist does).
    Time(Arc<dyn Fn(usize) -> i64 + Send + Sync>),
    /// Duration values and nanoseconds per unit.
    Duration(ScalarBuffer<i64>, i64),
    Mdn(IntervalMonthDayNanoArray),
    /// Each child's name, values and nulls; whether two children share a name.
    Struct(Vec<(String, Reader, Option<NullBuffer>)>, bool),
    /// Offsets (start of row i at i), the items and their nulls.
    List(Vec<usize>, Box<Reader>, Option<NullBuffer>),
    /// Offsets, keys, values and the values' nulls.
    Map(Vec<usize>, Box<Reader>, Box<Reader>, Option<NullBuffer>),
    /// A type the encoder does not read: refused at the first non-null value.
    Refused(String),
    /// A row result's column: its cells as they are (REQ-2195).
    Py(Vec<Pv>),
}

fn int_reader<T: ArrowPrimitiveType>(a: &ArrayRef) -> Reader
where
    T::Native: Into<i128>,
{
    let v = a.as_primitive::<T>().values().clone();
    Reader::Int(Arc::new(move |i| v[i].into()))
}

fn unit_ns(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    }
}

fn offsets<O: OffsetSizeTrait>(o: &OffsetBuffer<O>) -> Vec<usize> {
    o.iter().map(|v| v.as_usize()).collect()
}

impl Reader {
    /// Inside a struct, a list or a map: only a column's own values carry the IR infinities
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
            D::Float16 => {
                let v = a.as_primitive::<Float16Type>().values().clone();
                Reader::Float(Arc::new(move |i| v[i].to_f64()))
            }
            D::Float32 => {
                let v = a.as_primitive::<Float32Type>().values().clone();
                Reader::Float(Arc::new(move |i| v[i] as f64))
            }
            D::Float64 => {
                let v = a.as_primitive::<Float64Type>().values().clone();
                Reader::Float(Arc::new(move |i| v[i]))
            }
            D::Decimal128(_, s) => Reader::Dec128(a.as_primitive::<Decimal128Type>().clone(), *s as i32),
            D::Decimal256(_, s) => Reader::Dec256(a.as_primitive::<Decimal256Type>().clone(), *s as i32),
            D::Utf8 => Reader::Utf8(a.as_string::<i32>().clone()),
            D::LargeUtf8 => Reader::LargeUtf8(a.as_string::<i64>().clone()),
            D::Utf8View => Reader::Utf8View(a.as_string_view().clone()),
            D::Binary => Reader::Bin(a.as_binary::<i32>().clone()),
            D::LargeBinary => Reader::LargeBin(a.as_binary::<i64>().clone()),
            D::BinaryView => Reader::BinView(a.as_binary_view().clone()),
            D::FixedSizeBinary(_) => Reader::FixedBin(a.as_fixed_size_binary().clone()),
            D::Date32 => Reader::Date32(a.as_primitive::<Date32Type>().values().clone(), top),
            D::Date64 => Reader::Date64(a.as_primitive::<Date64Type>().values().clone()),
            D::Timestamp(unit, tz) => {
                let zone = match tz {
                    None => Zone::Naive,
                    Some(name) => match Zone::parse(name) {
                        Ok(z) => z,
                        Err(EncErr::Value(m)) => return Reader::Refused(m),
                        Err(_) => unreachable!(),
                    },
                };
                let values = match unit {
                    TimeUnit::Second => a.as_primitive::<TimestampSecondType>().values().clone(),
                    TimeUnit::Millisecond => a.as_primitive::<TimestampMillisecondType>().values().clone(),
                    TimeUnit::Microsecond => a.as_primitive::<TimestampMicrosecondType>().values().clone(),
                    TimeUnit::Nanosecond => a.as_primitive::<TimestampNanosecondType>().values().clone(),
                };
                Reader::Ts(values, unit_ns(unit), zone, top && *unit == TimeUnit::Microsecond)
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
            D::Time64(TimeUnit::Nanosecond) => {
                let v = a.as_primitive::<Time64NanosecondType>().values().clone();
                Reader::Time(Arc::new(move |i| v[i].div_euclid(1000)))
            }
            D::Duration(unit) => {
                let values = match unit {
                    TimeUnit::Second => a.as_primitive::<DurationSecondType>().values().clone(),
                    TimeUnit::Millisecond => a.as_primitive::<DurationMillisecondType>().values().clone(),
                    TimeUnit::Microsecond => a.as_primitive::<DurationMicrosecondType>().values().clone(),
                    TimeUnit::Nanosecond => a.as_primitive::<DurationNanosecondType>().values().clone(),
                };
                Reader::Duration(values, unit_ns(unit))
            }
            D::Interval(IntervalUnit::MonthDayNano) => Reader::Mdn(a.as_primitive::<IntervalMonthDayNanoType>().clone()),
            D::Struct(_) => {
                let s = a.as_struct();
                let children: Vec<(String, Reader, Option<NullBuffer>)> = s
                    .fields()
                    .iter()
                    .zip(s.columns())
                    .map(|(f, c)| (f.name().clone(), Reader::nested(c), c.logical_nulls()))
                    .collect();
                let mut names: Vec<&str> = children.iter().map(|c| c.0.as_str()).collect();
                names.sort_unstable();
                let shared = names.windows(2).any(|w| w[0] == w[1]);
                Reader::Struct(children, shared)
            }
            D::List(_) => {
                let l = a.as_list::<i32>();
                Reader::List(offsets(l.offsets()), Box::new(Reader::nested(l.values())), l.values().logical_nulls())
            }
            D::LargeList(_) => {
                let l = a.as_list::<i64>();
                Reader::List(offsets(l.offsets()), Box::new(Reader::nested(l.values())), l.values().logical_nulls())
            }
            D::FixedSizeList(_, size) => {
                let l = a.as_fixed_size_list();
                let size = *size as usize;
                let start = l.offset();
                let offs = (0..=l.len()).map(|i| (start + i) * size).collect();
                Reader::List(offs, Box::new(Reader::nested(l.values())), l.values().logical_nulls())
            }
            D::Map(..) => {
                let m = a.as_map();
                Reader::Map(
                    offsets(m.offsets()),
                    Box::new(Reader::nested(m.keys())),
                    Box::new(Reader::nested(m.values())),
                    m.values().logical_nulls(),
                )
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
            Reader::Int(f) => V::Int(f(i)),
            Reader::Float(f) => V::Float(f(i)),
            Reader::Dec128(a, s) => V::Dec(Dec::D128(a.value(i), *s)),
            Reader::Dec256(a, s) => V::Dec(Dec::D256(a.value(i), *s)),
            Reader::Utf8(a) => V::Str(a.value(i)),
            Reader::LargeUtf8(a) => V::Str(a.value(i)),
            Reader::Utf8View(a) => V::Str(a.value(i)),
            Reader::Bin(a) => V::Bin(a.value(i)),
            Reader::LargeBin(a) => V::Bin(a.value(i)),
            Reader::BinView(a) => V::Bin(a.value(i)),
            Reader::FixedBin(a) => V::Bin(a.value(i)),
            Reader::Date32(v, infinities) => {
                let d = v[i] as i64;
                if *infinities && d == i32::MAX as i64 {
                    V::Date(MAX_DAYS, 1) // ir_types.infinite_date: date.max
                } else if *infinities && d == -(i32::MAX as i64) {
                    V::Date(MIN_DAYS, -1)
                } else if (MIN_DAYS..=MAX_DAYS).contains(&d) {
                    V::Date(d, 0)
                } else {
                    return Err(EncErr::Value(format!("date32 {d} is out of range")));
                }
            }
            Reader::Date64(v) => {
                let d = v[i].div_euclid(86_400_000);
                if !(MIN_DAYS..=MAX_DAYS).contains(&d) {
                    return Err(EncErr::Value(format!("date64 {} is out of range", v[i])));
                }
                V::Date(d, 0)
            }
            Reader::Ts(v, mult, zone, infinities) => {
                let raw = v[i];
                if *infinities && raw.unsigned_abs() == i64::MAX as u64 {
                    // ir_types.infinite_timestamp: datetime.max / min, in UTC when aware.
                    let edge = if raw > 0 { MAX_US } else { MIN_US };
                    let offset = match zone {
                        Zone::Naive => None,
                        _ => Some(0),
                    };
                    return Ok(V::Ts(Ts {
                        local: Clock { us: edge, sub_ns: 0 },
                        offset,
                        inf: if raw > 0 { 1 } else { -1 },
                    }));
                }
                let ns = raw as i128 * *mult as i128;
                // A zoned value is the instant floored to the microsecond (ir_arrow._zoned_values);
                // a naive nanosecond value is pandas' Timestamp, its nanoseconds kept.
                let sub_ns = match zone {
                    Zone::Naive => ns.rem_euclid(1000) as u32,
                    _ => 0,
                };
                let utc = ns.div_euclid(1000);
                let range = MIN_US as i128..=MAX_US as i128;
                if !range.contains(&utc) {
                    return Err(EncErr::Value(format!("timestamp {raw} is out of range")));
                }
                let utc = utc as i64;
                let offset = zone.offset(utc);
                let local = utc as i128 + offset.unwrap_or(0) as i128 * 1_000_000;
                if !range.contains(&local) {
                    return Err(EncErr::Value(format!("timestamp {raw} is out of range in its time zone")));
                }
                V::Ts(Ts { local: Clock { us: local as i64, sub_ns }, offset, inf: 0 })
            }
            Reader::Time(f) => {
                let us = f(i);
                if !(0..DAY_US).contains(&us) {
                    return Err(EncErr::Value(format!("time {us}us is out of range")));
                }
                V::Time(Clock { us, sub_ns: 0 })
            }
            Reader::Duration(v, mult) => {
                // a timedelta (pandas' Timedelta for nanoseconds, its nanoseconds dropped toward
                // zero by ir_types.interval_parts); interval_parts of it
                let ns = v[i] as i128 * *mult as i128;
                let t = ns.signum() * (ns.abs() / 1000);
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
                let micros = v.nanoseconds.div_euclid(1000);
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
            Reader::Struct(..) | Reader::List(..) | Reader::Map(..) => V::Nested(self, i),
            Reader::Refused(what) => return Err(EncErr::Type(what.clone())),
            Reader::Py(cells) => pv_value(&cells[i]),
        })
    }

    /// Reads every non-null value under row `i` (struct children, list items), as to_pylist
    /// converts a column whether or not the encoder then uses it.
    fn check(&self, i: usize) -> R<()> {
        match self {
            Reader::Null => Ok(()),
            Reader::Struct(children, _) => {
                for (_, r, nulls) in children {
                    if valid(nulls, i) {
                        r.check(i)?;
                    }
                }
                Ok(())
            }
            Reader::List(o, items, nulls) => {
                for j in o[i]..o[i + 1] {
                    if valid(nulls, j) {
                        items.check(j)?;
                    }
                }
                Ok(())
            }
            Reader::Map(o, keys, values, nulls) => {
                for j in o[i]..o[i + 1] {
                    keys.check(j)?;
                    if valid(nulls, j) {
                        values.check(j)?;
                    }
                }
                Ok(())
            }
            _ => self.get(i).map(|_| ()),
        }
    }
}

#[inline]
fn valid(nulls: &Option<NullBuffer>, i: usize) -> bool {
    nulls.as_ref().is_none_or(|n| n.is_valid(i))
}

// ---- _proto_value: a value to the proto field's type --------------------------------------------

fn py_type(v: &V) -> &'static str {
    match v {
        V::Int(_) | V::BigInt(_) => "int",
        V::Float(_) => "float",
        V::Bool(_) => "bool",
        V::Dec(..) => "decimal.Decimal",
        V::Str(_) => "str",
        V::Bin(_) => "bytes",
        V::Date(..) => "datetime.date",
        V::Ts(..) => "datetime.datetime",
        V::Time(_) => "datetime.time",
        V::Interval(..) => "interval",
        V::Nested(Reader::Struct(..), _) => "dict",
        V::Nested(..) => "list",
        V::Json(J::Obj(_)) => "dict",
        V::Json(_) => "list",
        V::Py(Pv::Dict(_)) => "dict",
        V::Py(_) => "list",
    }
}

fn type_err<T>(what: &str, v: &V) -> R<T> {
    Err(EncErr::Type(format!("{what}: {}", py_type(v))))
}

fn text_of<'a>(v: &V<'a>) -> R<&'a str> {
    match v {
        V::Str(s) => Ok(s),
        V::Bin(b) => std::str::from_utf8(b).map_err(|_| EncErr::Value("bytes that are not text".into())),
        _ => unreachable!(),
    }
}

/// float(value). A finite value whose double is infinite is refused by name, as PostgreSQL
/// refuses it (J502b): a numeric past a double's range is never sent as an infinity.
fn to_float(f: &FieldDesc, v: &V) -> R<f64> {
    let (x, finite_source) = match v {
        V::Int(x) => (*x as f64, true),
        V::BigInt(d) => (d.parse::<f64>().unwrap(), true),
        V::Float(x) => (*x, false),
        V::Bool(b) => (*b as u8 as f64, false),
        V::Dec(d) => (d.to_f64(), true),
        V::Str(_) | V::Bin(_) => {
            let t = text_of(v)?;
            let x = parse::float(t)?;
            let named_infinity = t.trim().trim_start_matches(['+', '-']).to_ascii_lowercase().starts_with("inf");
            (x, !named_infinity)
        }
        _ => return type_err("float() argument must be a string or a real number, not", v),
    };
    if finite_source && x.is_infinite() {
        let mut shown = Vec::new();
        match v {
            V::Dec(d) => d.write(&mut shown),
            V::Str(_) | V::Bin(_) => shown_number(text_of(v)?.trim(), &mut shown),
            V::Int(i) => pyfmt::int_str(*i, &mut shown),
            V::BigInt(d) => shown.extend_from_slice(d.as_bytes()),
            _ => unreachable!(),
        }
        return Err(EncErr::Value(format!(
            "column \"{}\": value {} is out of range for type double precision",
            f.name,
            String::from_utf8_lossy(&shown)
        )));
    }
    Ok(x)
}

/// A number's text as a refusal names it: numeric_out's long run of integer digits (a numeric past
/// a double's range, as ir_arrow.row_batch holds it) in Decimal's exponent form, 1E+400.
fn shown_number(text: &str, out: &mut Vec<u8>) {
    let (sign, digits) = match text.strip_prefix('-') {
        Some(d) => ("-", d),
        None => ("", text),
    };
    if digits.len() <= 20 || !digits.bytes().all(|c| c.is_ascii_digit()) || digits.starts_with('0') {
        out.extend_from_slice(text.as_bytes());
        return;
    }
    let significant = digits.trim_end_matches('0');
    out.extend_from_slice(sign.as_bytes());
    out.push(significant.as_bytes()[0]);
    if significant.len() > 1 {
        out.push(b'.');
        out.extend_from_slice(&significant.as_bytes()[1..]);
    }
    out.extend_from_slice(format!("E+{}", digits.len() - 1).as_bytes());
}

/// int(value.toordinal()) for a date or datetime, else int(value).
fn to_int(v: &V) -> R<i128> {
    Ok(match v {
        V::Int(x) => *x,
        V::BigInt(d) => return Err(EncErr::Value(format!("Value out of range: {d}"))),
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
        V::Dec(d) => d.to_i128().ok_or_else(|| EncErr::Value("Value out of range".into()))?,
        V::Date(d, _) => (d + EPOCH_ORDINAL) as i128,
        V::Ts(t) => (t.local.us.div_euclid(DAY_US) + EPOCH_ORDINAL) as i128,
        V::Str(_) | V::Bin(_) => parse::int(text_of(v)?)?,
        _ => return type_err("int() argument must be a string, a bytes-like object or a real number, not", v),
    })
}

/// bool(value).
fn to_bool(v: &V) -> bool {
    match v {
        V::Int(x) => *x != 0,
        V::BigInt(_) => true,
        V::Float(x) => *x != 0.0,
        V::Bool(b) => *b,
        V::Dec(d) => !d.is_zero(),
        V::Str(s) => !s.is_empty(),
        V::Bin(b) => !b.is_empty(),
        V::Date(..) | V::Ts(..) | V::Time(_) => true,
        V::Interval(m, d, us) => (*m, *d, *us) != (0, 0, 0),
        V::Nested(Reader::Struct(children, _), _) => !children.is_empty(),
        V::Nested(Reader::List(o, ..), i) | V::Nested(Reader::Map(o, ..), i) => o[*i + 1] > o[*i],
        V::Nested(..) => unreachable!(),
        V::Json(J::Arr(items)) => !items.is_empty(),
        V::Json(J::Obj(entries)) => !entries.is_empty(),
        V::Json(_) => unreachable!(),
        V::Py(Pv::List(items)) => !items.is_empty(),
        V::Py(Pv::Dict(entries)) => !entries.is_empty(),
        V::Py(_) => unreachable!(),
    }
}

fn infinity_text(sign: i8, out: &mut Vec<u8>) {
    out.extend_from_slice(if sign > 0 { b"infinity" } else { b"-infinity" });
}

/// grpc/server._text: the text a string field carries.
fn write_text(f: &FieldDesc, v: &V, mask: Option<&Mask>, out: &mut Vec<u8>) -> R<()> {
    match v {
        V::Str(s) => out.extend_from_slice(s.as_bytes()),
        V::Date(_, inf) | V::Ts(Ts { inf, .. }) if *inf != 0 => infinity_text(*inf, out),
        V::Interval(m, d, us) => pyfmt::iso8601_duration(*m, *d, *us, out),
        V::Bin(b) => bytea_hex(b, out),
        V::Nested(..) | V::Json(_) | V::Py(_) => write_json(f, v, mask, out)?,
        _ => write_str(v, out)?,
    }
    Ok(())
}

/// ir_types.bytea_hex: PostgreSQL's hex output.
fn bytea_hex(b: &[u8], out: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.extend_from_slice(b"\\x");
    for &c in b {
        out.push(HEX[(c >> 4) as usize]);
        out.push(HEX[(c & 15) as usize]);
    }
}

/// str(value) of a scalar.
fn write_str(v: &V, out: &mut Vec<u8>) -> R<()> {
    match v {
        V::Str(s) => out.extend_from_slice(s.as_bytes()),
        V::Int(x) => pyfmt::int_str(*x, out),
        V::BigInt(d) => out.extend_from_slice(d.as_bytes()),
        V::Float(x) => pyfmt::float_str(*x, out),
        V::Bool(b) => out.extend_from_slice(if *b { b"True" } else { b"False" }),
        V::Dec(d) => d.write(out),
        V::Date(d, _) => pyfmt::date_str(*d, out),
        V::Ts(t) => pyfmt::datetime_str(t.local, t.offset, b' ', false, out),
        V::Time(c) => pyfmt::time_str(*c, false, out),
        _ => return type_err("str() of", v),
    }
    Ok(())
}

/// grpc/server._json_value, written as json.dumps(..., ensure_ascii=False, separators=(",", ":"))
/// writes it: an array, struct or map with what it holds, a decimal as its exact text, a date or
/// time as PostgreSQL's json writes it, an interval and bytes as their text. `mask` is a read
/// mask's selection (query_ir._restrict_decoded), applied before.
fn write_json(f: &FieldDesc, v: &V, mask: Option<&Mask>, out: &mut Vec<u8>) -> R<()> {
    let string = |out: &mut Vec<u8>, write: &dyn Fn(&mut Vec<u8>) -> R<()>| -> R<()> {
        let mut text = Vec::new();
        write(&mut text)?;
        json::write_str(std::str::from_utf8(&text).unwrap(), false, out);
        Ok(())
    };
    match v {
        V::Str(s) => json::write_str(s, false, out),
        V::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        V::Int(x) => pyfmt::int_str(*x, out),
        V::BigInt(d) => out.extend_from_slice(d.as_bytes()),
        V::Float(x) => {
            if !x.is_finite() {
                let mut shown = Vec::new();
                pyfmt::float_str(*x, &mut shown);
                return Err(EncErr::Value(format!(
                    "column \"{}\": {} is not a number JSON can hold",
                    f.name,
                    String::from_utf8_lossy(&shown)
                )));
            }
            pyfmt::float_repr(*x, out)
        }
        V::Dec(d) => string(out, &|o| {
            d.write(o);
            Ok(())
        })?,
        // serialize.temporal_text: isoformat, the fraction's trailing zeros dropped
        V::Date(_, inf) | V::Ts(Ts { inf, .. }) if *inf != 0 => string(out, &|o| {
            infinity_text(*inf, o);
            Ok(())
        })?,
        V::Date(d, _) => string(out, &|o| {
            pyfmt::date_str(*d, o);
            Ok(())
        })?,
        V::Ts(t) => string(out, &|o| {
            pyfmt::datetime_str(t.local, t.offset, b'T', true, o);
            Ok(())
        })?,
        V::Time(c) => string(out, &|o| {
            pyfmt::time_str(*c, true, o);
            Ok(())
        })?,
        V::Interval(m, d, us) => string(out, &|o| {
            pyfmt::iso8601_duration(*m, *d, *us, o);
            Ok(())
        })?,
        V::Bin(b) => string(out, &|o| {
            bytea_hex(b, o);
            Ok(())
        })?,
        V::Nested(Reader::Struct(children, shared), i) => {
            let selected = |name: &str| match mask {
                None => Some(None),
                Some(m) => m.get(name).map(|below| below.as_ref()),
            };
            if *shared {
                // a dict: a name given twice keeps its first place and its last value
                let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
                for (name, r, nulls) in children {
                    let mut item = Vec::new();
                    write_item(f, r, nulls, *i, None, &mut item)?;
                    json::put(&mut entries, name.clone(), item);
                }
                out.push(b'{');
                let mut first = true;
                for (name, item) in entries.iter().filter(|(n, _)| selected(n).is_some()) {
                    if !first {
                        out.push(b',');
                    }
                    first = false;
                    json::write_str(name, false, out);
                    out.push(b':');
                    match selected(name).unwrap() {
                        None => out.extend_from_slice(item),
                        Some(below) => {
                            let child = children.iter().rev().find(|c| &c.0 == name).unwrap();
                            write_item(f, &child.1, &child.2, *i, Some(below), out)?;
                        }
                    }
                }
                out.push(b'}');
            } else {
                out.push(b'{');
                let mut first = true;
                for (name, r, nulls) in children {
                    let Some(below) = selected(name) else {
                        r.check_if(nulls, *i)?;
                        continue;
                    };
                    if !first {
                        out.push(b',');
                    }
                    first = false;
                    json::write_str(name, false, out);
                    out.push(b':');
                    write_item(f, r, nulls, *i, below, out)?;
                }
                out.push(b'}');
            }
        }
        V::Nested(Reader::List(o, items, nulls), i) => {
            out.push(b'[');
            for j in o[*i]..o[*i + 1] {
                if j > o[*i] {
                    out.push(b',');
                }
                write_item(f, items, nulls, j, mask, out)?;
            }
            out.push(b']');
        }
        V::Nested(Reader::Map(o, keys, values, nulls), i) => {
            // a list of (key, value) tuples: a dict keyed by str(key), or [] when empty; a read
            // mask selects nothing inside one (_restrict_decoded keeps a tuple as it is)
            if o[*i] == o[*i + 1] {
                out.extend_from_slice(b"[]");
                return Ok(());
            }
            let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
            for j in o[*i]..o[*i + 1] {
                let key = keys.get(j)?;
                let mut k = Vec::new();
                match key {
                    V::Str(_) | V::Int(_) | V::Float(_) | V::Bool(_) | V::Dec(_) | V::Date(..) | V::Ts(_) | V::Time(_) => {
                        write_str(&key, &mut k)?
                    }
                    _ => return type_err("a map key of type", &key),
                }
                let mut item = Vec::new();
                write_item(f, values, nulls, j, None, &mut item)?;
                json::put(&mut entries, String::from_utf8(k).unwrap(), item);
            }
            out.push(b'{');
            for (n, (k, item)) in entries.iter().enumerate() {
                if n > 0 {
                    out.push(b',');
                }
                json::write_str(k, false, out);
                out.push(b':');
                out.extend_from_slice(item);
            }
            out.push(b'}');
        }
        V::Nested(..) => unreachable!(),
        V::Json(j) => write_json_tree(f, j, out)?,
        V::Py(Pv::List(items)) => {
            out.push(b'[');
            for (n, item) in items.iter().enumerate() {
                if n > 0 {
                    out.push(b',');
                }
                write_cell(f, item, mask, out)?;
            }
            out.push(b']');
        }
        V::Py(Pv::Dict(entries)) => {
            out.push(b'{');
            let mut first = true;
            for (name, item) in entries {
                let below = match mask {
                    None => None,
                    Some(m) => match m.get(name) {
                        Some(below) => below.as_ref(),
                        None => continue,
                    },
                };
                if !first {
                    out.push(b',');
                }
                first = false;
                json::write_str(name, false, out);
                out.push(b':');
                write_cell(f, item, below, out)?;
            }
            out.push(b'}');
        }
        V::Py(_) => unreachable!(),
    }
    Ok(())
}

/// A row result's cell as the encoder reads an Arrow cell: a list or dict stays a reference to it.
fn pv_value(p: &Pv) -> V<'_> {
    match p {
        Pv::Null => unreachable!("a null cell has no value"),
        Pv::Bool(b) => V::Bool(*b),
        Pv::Int(x) => V::Int(*x),
        Pv::BigInt(d) => V::BigInt(d),
        Pv::Float(x) => V::Float(*x),
        Pv::Dec(d) => V::Dec(*d),
        Pv::Str(s) => V::Str(s),
        Pv::Bin(b) => V::Bin(b),
        Pv::Date(d, inf) => V::Date(*d, *inf),
        Pv::Ts(t) => V::Ts(*t),
        Pv::Time(c) => V::Time(*c),
        Pv::Interval(m, d, us) => V::Interval(*m, *d, *us),
        Pv::List(_) | Pv::Dict(_) => V::Py(p),
    }
}

/// A list item or dict value of a row result's cell, as JSON.
fn write_cell(f: &FieldDesc, p: &Pv, mask: Option<&Mask>, out: &mut Vec<u8>) -> R<()> {
    match p {
        Pv::Null => {
            out.extend_from_slice(b"null");
            Ok(())
        }
        _ => write_json(f, &pv_value(p), mask, out),
    }
}

impl Reader {
    fn check_if(&self, nulls: &Option<NullBuffer>, i: usize) -> R<()> {
        if valid(nulls, i) {
            self.check(i)?;
        }
        Ok(())
    }
}

fn write_item(f: &FieldDesc, r: &Reader, nulls: &Option<NullBuffer>, i: usize, mask: Option<&Mask>, out: &mut Vec<u8>) -> R<()> {
    if !valid(nulls, i) || matches!(r, Reader::Null) {
        out.extend_from_slice(b"null");
        return Ok(());
    }
    write_json(f, &r.get(i)?, mask, out)
}

/// _json_value of what json.loads gave (a group's nodes): compact, NaN and the infinities refused.
fn write_json_tree(f: &FieldDesc, j: &J, out: &mut Vec<u8>) -> R<()> {
    match j {
        J::Null => out.extend_from_slice(b"null"),
        J::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        J::Int(d) => out.extend_from_slice(d.as_bytes()),
        J::Float(x) => write_json(f, &V::Float(*x), None, out)?,
        J::Str(s) => json::write_str(s, false, out),
        J::Arr(items) => {
            out.push(b'[');
            for (n, item) in items.iter().enumerate() {
                if n > 0 {
                    out.push(b',');
                }
                write_json_tree(f, item, out)?;
            }
            out.push(b']');
        }
        J::Obj(entries) => {
            out.push(b'{');
            for (n, (k, item)) in entries.iter().enumerate() {
                if n > 0 {
                    out.push(b',');
                }
                json::write_str(k, false, out);
                out.push(b':');
                write_json_tree(f, item, out)?;
            }
            out.push(b'}');
        }
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

fn varint_len(v: u64) -> usize {
    ((64 - (v | 1).leading_zeros() as usize) + 6) / 7
}

/// Writes what `body` appends as a length-delimited field: the body is written in place, then
/// moved up by the length prefix's size.
#[inline]
fn delimited(number: u32, out: &mut Vec<u8>, body: impl FnOnce(&mut Vec<u8>) -> R<()>) -> R<()> {
    tag(number, 2, out);
    let start = out.len();
    body(out)?;
    let len = out.len() - start;
    let n = varint_len(len as u64);
    let mut prefix = Vec::with_capacity(n);
    varint(len as u64, &mut prefix);
    out.resize(out.len() + n, 0);
    out.copy_within(start..start + len, start + n);
    out[start..start + n].copy_from_slice(&prefix);
    Ok(())
}

/// Timestamp.FromDatetime: a naive datetime is UTC, an aware one is moved to UTC; PostgreSQL's
/// infinities are Timestamp's maximum and minimum (REQ-1965), since Timestamp has none. A
/// nanosecond value keeps its nanoseconds.
fn write_timestamp(number: u32, v: &V, out: &mut Vec<u8>) -> R<()> {
    let (utc, sub_ns) = match v {
        V::Ts(t) if t.inf != 0 => (if t.inf > 0 { MAX_US } else { MIN_US }, 0),
        V::Ts(t) => t.utc(),
        V::Str(s) => {
            let (local, off) = parse::isoformat(s)?;
            (local - off.unwrap_or(0) as i64 * 1_000_000, 0)
        }
        _ => return type_err("fromisoformat: argument must be str, not", v),
    };
    if !(MIN_US..=MAX_US).contains(&utc) {
        return Err(EncErr::Overflow("date value out of range".into()));
    }
    let (seconds, nanos) = (utc.div_euclid(1_000_000), utc.rem_euclid(1_000_000) * 1000 + sub_ns as i64);
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

fn write_scalar(f: &FieldDesc, v: &V, mask: Option<&Mask>, out: &mut Vec<u8>) -> R<()> {
    let n = f.number;
    match f.ftype {
        T_DOUBLE => {
            let x = to_float(f, v)?;
            if f.presence || x.to_bits() != 0 {
                tag(n, 1, out);
                out.extend_from_slice(&x.to_le_bytes());
            }
        }
        T_FLOAT => {
            let x = to_float(f, v)?;
            let y = x as f32;
            if x.is_finite() && y.is_infinite() {
                // as PostgreSQL refuses a real out of range (protobuf would send an infinity)
                let mut shown = Vec::new();
                pyfmt::float_repr(x, &mut shown);
                return Err(EncErr::Value(format!(
                    "column \"{}\": value {} is out of range for type real",
                    f.name,
                    String::from_utf8_lossy(&shown)
                )));
            }
            if f.presence || y.to_bits() != 0 {
                tag(n, 5, out);
                out.extend_from_slice(&y.to_le_bytes());
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
                return Err(EncErr::Value(format!("Value out of range: {x}")));
            }
            if f.presence || x != 0 {
                tag(n, 0, out);
                // a negative int32 / int64 is its 64-bit two's complement
                varint(if x < 0 { x as i64 as u64 } else { x as u64 }, out);
            }
        }
        T_BOOL => {
            let b = to_bool(v);
            if f.presence || b {
                tag(n, 0, out);
                out.push(b as u8);
            }
        }
        T_STRING | T_BYTES => {
            let mark = out.len();
            delimited(n, out, |out| match v {
                V::Bin(b) if f.ftype == T_BYTES => {
                    out.extend_from_slice(b);
                    Ok(())
                }
                _ => write_text(f, v, mask, out),
            })?;
            if !f.presence && out.len() == mark + varint_len(n as u64 * 8) + 1 {
                out.truncate(mark); // an implicit-presence empty string is not written
            }
        }
        other => return Err(EncErr::Type(format!("proto field type {other}"))),
    }
    Ok(())
}

// ---- Query{Type} rows: which column feeds which field ------------------------------------------

enum Kind {
    Scalar(FieldDesc),
    Timestamp(u32),
    /// Raises at its first non-null value.
    Fails(EncErr),
}

struct Col {
    nulls: Option<NullBuffer>,
    reader: Reader,
    kind: Kind,
    mask: Option<Arc<Mask>>,
    /// The field it writes (columns sharing one write the last non-null among them).
    number: u32,
}

struct Plan {
    cols: Vec<Col>,
}

const TIMESTAMP: &str = "google.protobuf.Timestamp";

impl Plan {
    /// server._kwargs_for -> msg_cls(**kwargs): a column with no field raises at its first
    /// non-null value, as msg_cls does on the unknown name.
    fn build(msg: &MsgDesc, columns: Vec<(String, Reader, Option<NullBuffer>, Option<Arc<Mask>>)>) -> Plan {
        let mut cols = Vec::with_capacity(columns.len());
        for (name, reader, nulls, mask) in columns {
            let (kind, number) = match msg.field(&name) {
                None => (
                    Kind::Fails(EncErr::Value(format!(
                        "Protocol message {} has no \"{name}\" field.",
                        msg.full_name.rsplit('.').next().unwrap()
                    ))),
                    u32::MAX,
                ),
                Some(f) if f.ftype == T_MESSAGE && f.type_name == TIMESTAMP && !f.repeated => {
                    (Kind::Timestamp(f.number), f.number)
                }
                Some(f) if f.ftype == T_MESSAGE || f.repeated => (
                    Kind::Fails(EncErr::Type(format!(
                        "column \"{name}\": a result column is never written to a message or repeated field"
                    ))),
                    f.number,
                ),
                Some(f) => (Kind::Scalar(f.clone()), f.number),
            };
            cols.push(Col { nulls, reader, kind, mask, number });
        }
        // the fields in number order, as upb writes them (a stable sort keeps columns' order)
        cols.sort_by_key(|c| c.number);
        Plan { cols }
    }

    fn encode(&self, i: usize, out: &mut Vec<u8>) -> R<()> {
        let mut k = 0;
        while k < self.cols.len() {
            // the run of columns that write one field: every value is converted (kwargs gets
            // each in turn), the last non-null one is written
            let mut end = k + 1;
            while end < self.cols.len() && self.cols[end].number == self.cols[k].number && self.cols[k].number != u32::MAX {
                end += 1;
            }
            let mut written = false;
            for c in self.cols[k..end].iter().rev() {
                if c.nulls.as_ref().is_some_and(|n| n.is_null(i)) || matches!(c.reader, Reader::Null) {
                    continue;
                }
                if written {
                    let mut scratch = Vec::new();
                    self.write(c, i, &mut scratch)?;
                } else {
                    self.write(c, i, out)?;
                    written = true;
                }
            }
            k = end;
        }
        Ok(())
    }

    fn write(&self, c: &Col, i: usize, out: &mut Vec<u8>) -> R<()> {
        let mask = c.mask.as_deref();
        match &c.kind {
            Kind::Scalar(f) => {
                let v = c.reader.get(i)?;
                match (mask, v) {
                    // query_ir.restrict_json: JSON text is read, restricted and written back
                    // as json.dumps writes it
                    (Some(m), V::Str(s)) => {
                        let mut text = Vec::new();
                        json::dumps(&json::restrict(json::loads(s)?, m), &mut text);
                        let text = String::from_utf8(text).unwrap();
                        write_scalar(f, &V::Str(&text), None, out)
                    }
                    (_, v) => write_scalar(f, &v, mask, out),
                }
            }
            Kind::Timestamp(n) => write_timestamp(*n, &c.reader.get(i)?, out),
            Kind::Fails(e) => {
                c.reader.check(i)?;
                Err(e.clone())
            }
        }
    }
}

// ---- a group's nodes (server._dict_row_to_message) ---------------------------------------------

/// A {Type} message from a node's object: its keys that are fields, NULLs left out; a relation
/// field from an object (a repeated one's only item) or a list's objects, anything else dropped.
fn node_message(pool: &Pool, msg: &MsgDesc, entries: &[(String, J)], out: &mut Vec<u8>) -> R<()> {
    let mut fields: Vec<(&FieldDesc, &J)> = entries
        .iter()
        .filter(|(_, v)| !matches!(v, J::Null))
        .filter_map(|(k, v)| msg.field(k).map(|f| (f, v)))
        .collect();
    fields.sort_by_key(|(f, _)| f.number);
    for (f, v) in fields {
        if f.ftype == T_MESSAGE && f.type_name != TIMESTAMP {
            let Ok(sub) = pool.message(&f.type_name) else { continue };
            let items: Vec<&J> = match (v, f.repeated) {
                (J::Arr(items), true) => items.iter().filter(|j| matches!(j, J::Obj(_))).collect(),
                (J::Obj(_), _) => vec![v],
                _ => vec![],
            };
            for item in items {
                let J::Obj(sub_entries) = item else { unreachable!() };
                delimited(f.number, out, |out| node_message(pool, sub, sub_entries, out))?;
            }
            continue;
        }
        let value = match v {
            J::Bool(b) => V::Bool(*b),
            J::Int(d) => d.parse::<i128>().map(V::Int).unwrap_or(V::BigInt(d)),
            J::Float(x) => V::Float(*x),
            J::Str(s) => V::Str(s),
            J::Arr(_) | J::Obj(_) => V::Json(v),
            J::Null => unreachable!(),
        };
        if f.repeated {
            return type_err(&format!("field \"{}\" is repeated, a node gives", f.name), &value);
        }
        if f.ftype == T_MESSAGE {
            write_timestamp(f.number, &value, out)?;
        } else {
            write_scalar(f, &value, None, out)?;
        }
    }
    Ok(())
}

// ---- the Python entry points --------------------------------------------------------------------

/// An organisation's compiled service, read from the serialized FileDescriptorSet it was compiled
/// into: ``Service(descriptor_set)``. Built once per compiled service and shared by its RPCs.
#[pyclass(frozen, module = "provisa._encoders.grpc")]
struct Service {
    pool: Arc<Pool>,
}

#[pymethods]
impl Service {
    #[new]
    fn new(descriptor_set: &[u8]) -> PyResult<Self> {
        Ok(Service { pool: Arc::new(Pool::parse(descriptor_set)?) })
    }
}

/// Encodes one RPC's Arrow result as its response messages.
///
/// ``Encoder(service, message, columns, masks=None, batch_message=None, batch_rows=0)``:
/// ``message`` is the row message's full name (``provisa.v1.Orders``); ``columns`` the field
/// name each result column is written to (the server's ``out_cols``); ``masks`` a read mask's
/// selection inside each column's JSON (``{key: None | {...}}``, None for the whole value). With
/// ``batch_message`` (``provisa.v1.OrdersBatch``) rows are written ``batch_rows`` to a message, as
/// Query{Type}Batch does, across Arrow batches.
#[pyclass(module = "provisa._encoders.grpc")]
struct Encoder {
    pool: Arc<Pool>,
    message: String,
    columns: Vec<String>,
    masks: Vec<Option<Arc<Mask>>>,
    /// (the batch message's rows field number, rows per batch message)
    batch: Option<(u32, usize)>,
    pending: Vec<u8>,
    pending_rows: usize,
}

fn mask_of(d: &Bound<'_, PyDict>) -> PyResult<Mask> {
    let mut entries = Vec::with_capacity(d.len());
    for (k, v) in d.iter() {
        let below = if v.is_none() { None } else { Some(mask_of(v.downcast::<PyDict>()?)?) };
        entries.push((k.extract::<String>()?, below));
    }
    Ok(Mask(entries))
}

/// Messages laid end to end, each ended at an offset.
struct Messages {
    data: Vec<u8>,
    ends: Vec<usize>,
}

impl Messages {
    fn push(&mut self, message: &[u8]) {
        self.data.extend_from_slice(message);
        self.ends.push(self.data.len());
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
        let columns = self
            .columns
            .iter()
            .cloned()
            .zip(batch.columns().iter())
            .zip(self.masks.iter().cloned())
            .map(|((n, a), m)| (n, Reader::read(a, true), a.logical_nulls(), m))
            .collect();
        self.encode_plan(columns, batch.num_rows(), out)
    }

    /// A row result's batch (REQ-2195): each column's cells read as they are, then written as
    /// an Arrow batch's are.
    fn encode_cells(&mut self, columns: Vec<Vec<Pv>>, rows: usize, out: &mut Messages) -> R<()> {
        let columns = self
            .columns
            .iter()
            .cloned()
            .zip(columns)
            .zip(self.masks.iter().cloned())
            .map(|((n, cells), m)| {
                let nulls = cells
                    .iter()
                    .any(|c| matches!(c, Pv::Null))
                    .then(|| NullBuffer::from_iter(cells.iter().map(|c| !matches!(c, Pv::Null))));
                (n, Reader::Py(cells), nulls, m)
            })
            .collect();
        self.encode_plan(columns, rows, out)
    }

    fn encode_plan(
        &mut self,
        columns: Vec<(String, Reader, Option<NullBuffer>, Option<Arc<Mask>>)>,
        rows: usize,
        out: &mut Messages,
    ) -> R<()> {
        let plan = Plan::build(self.pool.message(&self.message)?, columns);
        for i in 0..rows {
            match self.batch {
                None => {
                    plan.encode(i, &mut out.data)?;
                    out.ends.push(out.data.len());
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

    fn run(&mut self, py: Python<'_>, batch: Option<&Bound<'_, PyAny>>) -> PyResult<Messages> {
        let batch = batch.map(RecordBatch::from_pyarrow_bound).transpose()?;
        py.allow_threads(|| {
            let mut out = Messages { data: Vec::new(), ends: Vec::new() };
            match &batch {
                Some(b) => self.encode_batch(b, &mut out)?,
                None if self.pending_rows > 0 => {
                    out.push(&self.pending);
                    self.pending.clear();
                    self.pending_rows = 0;
                }
                None => {}
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
    #[pyo3(signature = (service, message, columns, masks=None, batch_message=None, batch_rows=0))]
    fn new(
        service: &Service,
        message: String,
        columns: Vec<String>,
        masks: Option<Vec<Option<Bound<'_, PyDict>>>>,
        batch_message: Option<String>,
        batch_rows: usize,
    ) -> PyResult<Self> {
        let pool = service.pool.clone();
        let row = pool.message(&message)?.full_name.clone();
        let masks = match masks {
            None => vec![None; columns.len()],
            Some(m) if m.len() == columns.len() => m
                .iter()
                .map(|d| d.as_ref().map(|d| mask_of(d).map(Arc::new)).transpose())
                .collect::<PyResult<_>>()?,
            Some(m) => {
                return Err(PyValueError::new_err(format!("{} masks for {} columns", m.len(), columns.len())))
            }
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
        Ok(Encoder { pool, message: row, columns, masks, batch, pending: Vec::new(), pending_rows: 0 })
    }

    /// The serialized messages this Arrow record batch completes.
    fn encode<'py>(&mut self, py: Python<'py>, batch: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyList>> {
        let m = self.run(py, Some(batch))?;
        as_list(py, m)
    }

    /// The serialized messages these rows complete: a row result's batch, each row a sequence of
    /// its cells in the columns' order, ``types`` the engine's type name of each column (or None).
    #[pyo3(signature = (rows, types=None))]
    fn encode_rows<'py>(
        &mut self,
        py: Python<'py>,
        rows: &Bound<'py, PyAny>,
        types: Option<Vec<Option<String>>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let width = self.columns.len();
        let types = types.unwrap_or_else(|| vec![None; width]);
        if types.len() != width {
            return Err(PyValueError::new_err(format!("{} types for {width} columns", types.len())));
        }
        let cells = Cells::new(py)?;
        let mut columns: Vec<Vec<Pv>> = (0..width).map(|_| Vec::new()).collect();
        let mut n = 0;
        for row in rows.try_iter()? {
            let row = row?;
            if row.len()? != width {
                return Err(EncErr::Value(format!("{width} columns named for a row of {}", row.len()?)).into());
            }
            for (c, column) in columns.iter_mut().enumerate() {
                column.push(cells.read(&row.get_item(c)?, types[c].as_deref())?);
            }
            n += 1;
        }
        let m = py.allow_threads(|| {
            let mut out = Messages { data: Vec::new(), ends: Vec::new() };
            self.encode_cells(columns, n, &mut out).map(|_| out)
        })?;
        as_list(py, m)
    }

    /// The last, partly filled batch message, if any (Query{Type}Batch's final message).
    fn finish<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let m = self.run(py, None)?;
        as_list(py, m)
    }
}

/// ``encode_nodes(service, message, field_number, nodes)``: a group's nodes (``nodes``, the JSON
/// text of a list of objects) as entries of the group message's repeated ``message`` field
/// ``field_number``, to follow the rest of that message's serialized fields.
#[pyfunction]
fn encode_nodes<'py>(
    py: Python<'py>,
    service: &Service,
    message: &str,
    field_number: u32,
    nodes: &str,
) -> PyResult<Bound<'py, PyBytes>> {
    let msg = service.pool.message(message)?;
    let J::Arr(items) = json::loads(nodes)? else {
        return Err(PyValueError::new_err("a group's nodes are a JSON list"));
    };
    let mut out = Vec::new();
    for item in &items {
        if let J::Obj(entries) = item {
            delimited(field_number, &mut out, |out| node_message(&service.pool, msg, entries, out))?;
        }
    }
    Ok(PyBytes::new(py, &out))
}

#[pymodule]
pub fn grpc_encoder(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Service>()?;
    m.add_class::<Encoder>()?;
    m.add_function(wrap_pyfunction!(encode_nodes, m)?)?;
    m.add("TZDB_VERSION", chrono_tz::IANA_TZDB_VERSION)?;
    Ok(())
}
