// REQ-2194: pgwire's DataRow encoder. provisa._encoders.pgwire (rust/encoders) serves it, and
// provisa.pgwire.server's send_data_rows writes every result through it, straight from the
// engine's Arrow batches. tests/unit/pgwire_encoder/test_differential.py holds it to PostgreSQL
// 17's answers, byte for byte (floats within one part in 10^14).

mod coerce;
mod fmt_num;
mod json;
mod pgtypes;
mod pyval;
mod temporal;
mod tsearch;
// Shared with the JSON encoders (rust/encoders/json): one reading of a named zone.
pub mod tz;
mod tz_rules;
mod unicode_tables;

use std::sync::Arc;

use arrow::array::*;
use arrow::compute::cast;
use arrow::datatypes::*;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyOverflowError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use coerce::Coercer;
use fmt_num::{float4_text, float8_text, numeric_binary, numeric_text};
use pgtypes::Bv;
use temporal::{
    date_text, interval_parts_of_duration, interval_parts_of_mdn, interval_text, timestamp_text, TzSpec, DAY_US,
};

#[derive(Debug)]
pub enum EncErr {
    Overflow(String),
    Value(String),
    Type(String),
    /// No direct Arrow writer takes the column: it is written from its values (never raised).
    NoDirectWriter(String),
}

impl From<EncErr> for PyErr {
    fn from(e: EncErr) -> PyErr {
        match e {
            EncErr::Overflow(m) => PyOverflowError::new_err(m),
            EncErr::Value(m) => PyValueError::new_err(m),
            EncErr::Type(m) => PyTypeError::new_err(m),
            EncErr::NoDirectWriter(m) => PyTypeError::new_err(m),
        }
    }
}

pub type R<T> = Result<T, EncErr>;

/// Writes the value at a row index (known non-null) without its length prefix.
type Enc = Box<dyn Fn(usize, &mut Vec<u8>) -> R<()> + Send + Sync>;

/// An interval column's (months, days, microseconds) at a row index.
type IntervalParts = Box<dyn Fn(usize) -> R<(i64, i64, i64)> + Send + Sync>;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Elem {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Bool,
    Text,
}

impl Elem {
    fn oid(self) -> i32 {
        match self {
            Elem::Int2 => 21,
            Elem::Int4 => 23,
            Elem::Int8 => 20,
            Elem::Float4 => 700,
            Elem::Float8 => 701,
            Elem::Numeric => 1700,
            Elem::Bool => 16,
            Elem::Text => 25,
        }
    }
    fn kind(self) -> Kind {
        match self {
            Elem::Int2 => Kind::Int2,
            Elem::Int4 => Kind::Int4,
            Elem::Int8 => Kind::Int8,
            Elem::Float4 => Kind::Float4,
            Elem::Float8 => Kind::Float8,
            Elem::Numeric => Kind::Numeric,
            Elem::Bool => Kind::Bool,
            Elem::Text => Kind::Text,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
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
    Interval,
    Json,
    Jsonb,
    Uuid,
    Array(Elem),
}

/// The direct Arrow writer of a BVType, where there is one.
fn kind_of(name: &str) -> Option<Kind> {
    Some(match name {
        "BOOL" => Kind::Bool,
        "SMALLINT" => Kind::Int2,
        "INTEGER" => Kind::Int4,
        "BIGINT" => Kind::Int8,
        "OID" => Kind::Oid,
        "REAL" => Kind::Float4,
        "FLOAT" => Kind::Float8,
        "DECIMAL" => Kind::Numeric,
        "TEXT" => Kind::Text,
        "BYTES" => Kind::Bytes,
        "DATE" => Kind::Date,
        "TIMESTAMP" => Kind::Timestamp,
        "TIMESTAMPTZ" => Kind::TimestampTz,
        "TIME" => Kind::Time,
        "INTERVAL" => Kind::Interval,
        "JSON" => Kind::Json,
        "JSONB" => Kind::Jsonb,
        "UUID" => Kind::Uuid,
        "SMALLINTARRAY" => Kind::Array(Elem::Int2),
        "INTEGERARRAY" => Kind::Array(Elem::Int4),
        "BIGINTARRAY" => Kind::Array(Elem::Int8),
        "REALARRAY" => Kind::Array(Elem::Float4),
        "FLOATARRAY" => Kind::Array(Elem::Float8),
        "DECIMALARRAY" => Kind::Array(Elem::Numeric),
        "BOOLARRAY" => Kind::Array(Elem::Bool),
        "STRINGARRAY" => Kind::Array(Elem::Text),
        _ => return None,
    })
}

fn unsupported(kind: Kind, dt: &DataType) -> EncErr {
    EncErr::NoDirectWriter(format!("no direct writer of {kind:?} from Arrow {dt}"))
}

/// Downcasts `arr` to `T`, owning a clone (Arrow arrays are cheap Arc'd buffers).
pub(crate) fn typed<T: Array + Clone + 'static>(arr: &ArrayRef) -> T {
    arr.as_any()
        .downcast_ref::<T>()
        .expect("downcast matches DataType")
        .clone()
}

/// An integer column's value as i128 (every Arrow integer width fits).
fn int_getter(arr: &ArrayRef) -> Option<Box<dyn Fn(usize) -> i128 + Send + Sync>> {
    macro_rules! g {
        ($t:ty) => {{
            let a = typed::<PrimitiveArray<$t>>(arr);
            Some(Box::new(move |i| a.value(i) as i128))
        }};
    }
    match arr.data_type() {
        DataType::Int8 => g!(Int8Type),
        DataType::Int16 => g!(Int16Type),
        DataType::Int32 => g!(Int32Type),
        DataType::Int64 => g!(Int64Type),
        DataType::UInt8 => g!(UInt8Type),
        DataType::UInt16 => g!(UInt16Type),
        DataType::UInt32 => g!(UInt32Type),
        DataType::UInt64 => g!(UInt64Type),
        _ => None,
    }
}

/// A float column's value as the Python float to_pylist gives (float32 widened exactly).
fn float_getter(arr: &ArrayRef) -> Option<Box<dyn Fn(usize) -> f64 + Send + Sync>> {
    match arr.data_type() {
        DataType::Float32 => {
            let a = typed::<Float32Array>(arr);
            Some(Box::new(move |i| a.value(i) as f64))
        }
        DataType::Float64 => {
            let a = typed::<Float64Array>(arr);
            Some(Box::new(move |i| a.value(i)))
        }
        _ => int_getter(arr).map(|g| -> Box<dyn Fn(usize) -> f64 + Send + Sync> {
            // float(int): correctly rounded, as `as f64` is.
            Box::new(move |i| g(i) as f64)
        }),
    }
}

/// A string column's value at a row index.
type StrGetter = Box<dyn Fn(usize) -> &'static str + Send + Sync>;

fn str_getter(arr: &ArrayRef) -> Option<StrGetter> {
    // The returned &str borrows the array's buffer, which the closure keeps alive; the
    // 'static is sound only for the duration of one encode call, which owns every closure.
    macro_rules! g {
        ($t:ty) => {{
            let a = typed::<$t>(arr);
            Some(Box::new(move |i| {
                let s: &str = a.value(i);
                unsafe { std::mem::transmute::<&str, &'static str>(s) }
            }))
        }};
    }
    match arr.data_type() {
        DataType::Utf8 => g!(StringArray),
        DataType::LargeUtf8 => g!(LargeStringArray),
        DataType::Utf8View => g!(StringViewArray),
        _ => None,
    }
}

fn bytes_getter(arr: &ArrayRef) -> Option<Box<dyn Fn(usize) -> &'static [u8] + Send + Sync>> {
    macro_rules! g {
        ($t:ty) => {{
            let a = typed::<$t>(arr);
            Some(Box::new(move |i| {
                let s: &[u8] = a.value(i);
                unsafe { std::mem::transmute::<&[u8], &'static [u8]>(s) }
            }))
        }};
    }
    match arr.data_type() {
        DataType::Binary => g!(BinaryArray),
        DataType::LargeBinary => g!(LargeBinaryArray),
        DataType::BinaryView => g!(BinaryViewArray),
        DataType::FixedSizeBinary(_) => g!(FixedSizeBinaryArray),
        _ => None,
    }
}

/// (negative, absolute coefficient digits, exponent) of a decimal or integer column's value, the
/// Decimal pyarrow's to_pylist hands on.
type DecGetter = Box<dyn Fn(usize, &mut Vec<u8>) -> (bool, i32) + Send + Sync>;

fn dec_getter(arr: &ArrayRef) -> Option<DecGetter> {
    match arr.data_type() {
        DataType::Decimal128(_, s) => {
            let a = typed::<Decimal128Array>(arr);
            let exp = -(*s as i32);
            Some(Box::new(move |i, digits: &mut Vec<u8>| {
                let v = a.value(i);
                digits.clear();
                let mut b = itoa::Buffer::new();
                digits.extend_from_slice(b.format(v.unsigned_abs()).as_bytes());
                (v < 0, exp)
            }))
        }
        DataType::Decimal256(_, s) => {
            let a = typed::<Decimal256Array>(arr);
            let exp = -(*s as i32);
            Some(Box::new(move |i, digits: &mut Vec<u8>| {
                let v = a.value(i);
                let text = v.to_string();
                digits.clear();
                let neg = text.starts_with('-');
                digits.extend_from_slice(text.trim_start_matches('-').as_bytes());
                (neg, exp)
            }))
        }
        _ => int_getter(arr).map(|g| -> DecGetter {
            Box::new(move |i, digits: &mut Vec<u8>| {
                let v = g(i);
                digits.clear();
                let mut b = itoa::Buffer::new();
                digits.extend_from_slice(b.format(v.unsigned_abs()).as_bytes());
                (v < 0, 0)
            })
        }),
    }
}

fn int_binary(v: i128, width: usize, signed: bool, out: &mut Vec<u8>) -> R<()> {
    // int.to_bytes(width, "big", signed=...): OverflowError when it does not fit.
    let bits = (width * 8) as u32;
    let (lo, hi) = if signed {
        (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1)
    } else {
        (0, (1i128 << bits) - 1)
    };
    if v < lo || v > hi {
        return Err(EncErr::Overflow("int too big to convert".into()));
    }
    out.extend_from_slice(&v.to_be_bytes()[16 - width..]);
    Ok(())
}

fn f32_of(x: f64) -> R<f32> {
    // struct.pack("!f", x): a finite double past float's range is an OverflowError.
    let y = x as f32;
    if y.is_infinite() && !x.is_infinite() {
        return Err(EncErr::Overflow("float too large to pack with f format".into()));
    }
    Ok(y)
}

/// The writer of one scalar column of `kind` in text or binary form.
fn scalar_enc(kind: Kind, binary: bool, arr: &ArrayRef) -> R<Enc> {
    let dt = arr.data_type().clone();
    if let DataType::Dictionary(_, value_type) = &dt {
        // to_pylist of a dictionary column is its decoded values.
        let decoded = cast(arr, value_type).map_err(|e| EncErr::Value(e.to_string()))?;
        return scalar_enc(kind, binary, &decoded);
    }
    match kind {
        Kind::Bool => {
            let DataType::Boolean = dt else {
                return Err(unsupported(kind, &dt));
            };
            let a = typed::<BooleanArray>(arr);
            Ok(if binary {
                Box::new(move |i, out| {
                    out.push(a.value(i) as u8);
                    Ok(())
                })
            } else {
                Box::new(move |i, out| {
                    out.push(if a.value(i) { b't' } else { b'f' });
                    Ok(())
                })
            })
        }
        Kind::Int2 | Kind::Int4 | Kind::Int8 | Kind::Oid => {
            let g = int_getter(arr).ok_or_else(|| unsupported(kind, &dt))?;
            if binary {
                let (w, signed) = match kind {
                    Kind::Int2 => (2, true),
                    Kind::Int4 => (4, true),
                    Kind::Int8 => (8, true),
                    _ => (4, false),
                };
                Ok(Box::new(move |i, out| int_binary(g(i), w, signed, out)))
            } else {
                Ok(Box::new(move |i, out| {
                    let mut b = itoa::Buffer::new();
                    out.extend_from_slice(b.format(g(i)).as_bytes());
                    Ok(())
                }))
            }
        }
        Kind::Float8 | Kind::Float4 => {
            let g = float_getter(arr).ok_or_else(|| unsupported(kind, &dt))?;
            match (kind, binary) {
                (Kind::Float8, false) => Ok(Box::new(move |i, out| {
                    float8_text(g(i), out);
                    Ok(())
                })),
                (Kind::Float8, true) => Ok(Box::new(move |i, out| {
                    out.extend_from_slice(&g(i).to_be_bytes());
                    Ok(())
                })),
                (_, false) => Ok(Box::new(move |i, out| float4_text(g(i), out))),
                (_, true) => Ok(Box::new(move |i, out| {
                    out.extend_from_slice(&f32_of(g(i))?.to_be_bytes());
                    Ok(())
                })),
            }
        }
        Kind::Numeric => {
            let g = dec_getter(arr).ok_or_else(|| unsupported(kind, &dt))?;
            Ok(Box::new(move |i, out| {
                let mut digits = Vec::with_capacity(40);
                let (neg, exp) = g(i, &mut digits);
                if binary {
                    numeric_binary(neg, &digits, exp, out)
                } else {
                    numeric_text(neg, &digits, exp, out);
                    Ok(())
                }
            }))
        }
        Kind::Text => {
            let g = str_getter(arr).ok_or_else(|| unsupported(kind, &dt))?;
            Ok(Box::new(move |i, out| {
                out.extend_from_slice(g(i).as_bytes());
                Ok(())
            }))
        }
        Kind::Bytes => {
            let g = bytes_getter(arr).ok_or_else(|| unsupported(kind, &dt))?;
            Ok(if binary {
                Box::new(move |i, out| {
                    out.extend_from_slice(g(i));
                    Ok(())
                })
            } else {
                Box::new(move |i, out| {
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    let v = g(i);
                    out.reserve(2 + v.len() * 2);
                    out.extend_from_slice(b"\\x");
                    for &b in v {
                        out.push(HEX[(b >> 4) as usize]);
                        out.push(HEX[(b & 15) as usize]);
                    }
                    Ok(())
                })
            })
        }
        Kind::Date => {
            // (days since 1970-01-01, the ±infinity a date32 marks). to_pylist of a date64 is the
            // date of its milliseconds, floored; only date32 carries the infinity markers.
            let days: Box<dyn Fn(usize) -> (i64, i32) + Send + Sync> = match &dt {
                DataType::Date32 => {
                    let a = typed::<Date32Array>(arr);
                    Box::new(move |i| (a.value(i) as i64, temporal::date_infinity(a.value(i))))
                }
                DataType::Date64 => {
                    let a = typed::<Date64Array>(arr);
                    Box::new(move |i| (a.value(i).div_euclid(86_400_000), 0))
                }
                _ => return Err(unsupported(kind, &dt)),
            };
            Ok(Box::new(move |i, out| {
                let (days, inf) = days(i);
                if binary {
                    if inf != 0 {
                        out.extend_from_slice(&(if inf > 0 { i32::MAX } else { i32::MIN }).to_be_bytes());
                    } else {
                        temporal::check_date(days)?;
                        out.extend_from_slice(&((days - 10957) as i32).to_be_bytes());
                    }
                    Ok(())
                } else {
                    date_text(days, inf, out)
                }
            }))
        }
        Kind::Timestamp | Kind::TimestampTz => {
            let DataType::Timestamp(unit, tz) = &dt else {
                return Err(unsupported(kind, &dt));
            };
            let unit = *unit;
            // Only a microsecond column carries the ±infinity markers (column_values).
            let infinity_marked = unit == TimeUnit::Microsecond;
            let tz = match tz {
                None => TzSpec::Naive,
                Some(name) => TzSpec::parse(name)?,
            };
            let a = typed::<Int64Array>(&cast(arr, &DataType::Int64).map_err(|e| EncErr::Value(e.to_string()))?);
            let tz_required = kind == Kind::TimestampTz;
            Ok(Box::new(move |i, out| {
                let raw = a.value(i);
                let inf = if infinity_marked {
                    temporal::timestamp_infinity(raw)
                } else {
                    0
                };
                // A zoned nanosecond value is floored to the microsecond, at any local time
                // (ir_arrow._zoned_values); pandas' range does not bound it.
                let micros = if unit == TimeUnit::Nanosecond && tz != TzSpec::Naive {
                    raw.div_euclid(1000) as i128
                } else {
                    temporal::micros_of(raw, unit)?
                };
                if binary {
                    if inf != 0 {
                        out.extend_from_slice(&(if inf > 0 { i64::MAX } else { i64::MIN }).to_be_bytes());
                        return Ok(());
                    }
                    temporal::check_timestamp(micros, &tz)?;
                    if tz_required && tz == TzSpec::Naive {
                        return Err(EncErr::Value("timestamptz value without a time zone".into()));
                    }
                    let since_2000 = micros - 946_684_800_000_000;
                    out.extend_from_slice(&(since_2000 as i64).to_be_bytes());
                    Ok(())
                } else {
                    if inf != 0 {
                        out.extend_from_slice(if inf > 0 { b"infinity" } else { b"-infinity" });
                        return Ok(());
                    }
                    timestamp_text(micros, &tz, out)
                }
            }))
        }
        Kind::Time => {
            let micros: Box<dyn Fn(usize) -> R<i64> + Send + Sync> = match &dt {
                DataType::Time64(TimeUnit::Microsecond) => {
                    let a = typed::<Time64MicrosecondArray>(arr);
                    Box::new(move |i| Ok(a.value(i)))
                }
                DataType::Time32(TimeUnit::Millisecond) => {
                    let a = typed::<Time32MillisecondArray>(arr);
                    Box::new(move |i| Ok(a.value(i) as i64 * 1000))
                }
                DataType::Time32(TimeUnit::Second) => {
                    let a = typed::<Time32SecondArray>(arr);
                    Box::new(move |i| Ok(a.value(i) as i64 * 1_000_000))
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    // as_py is pandas.Timestamp(v, unit="ns").time(): the time of day of that
                    // instant, its nanoseconds dropped.
                    let a = typed::<Time64NanosecondArray>(arr);
                    Box::new(move |i| {
                        let us = temporal::micros_of(a.value(i), TimeUnit::Nanosecond)?;
                        Ok(us.rem_euclid(DAY_US) as i64)
                    })
                }
                _ => return Err(unsupported(kind, &dt)),
            };
            Ok(Box::new(move |i, out| {
                let us = micros(i)?;
                // 24:00:00 is a time PostgreSQL holds (time_in, a time rounded up to it)
                if !(0..=86_400_000_000).contains(&us) {
                    return Err(EncErr::Value("hour must be in 0..23".into()));
                }
                if binary {
                    out.extend_from_slice(&(us as u64).to_be_bytes());
                } else {
                    temporal::clock_text(us, out);
                }
                Ok(())
            }))
        }
        Kind::Interval => {
            let parts: IntervalParts = match &dt {
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    let a = typed::<IntervalMonthDayNanoArray>(arr);
                    Box::new(move |i| {
                        let v = a.value(i);
                        interval_parts_of_mdn(v.months, v.days, v.nanoseconds)
                    })
                }
                DataType::Duration(unit) => {
                    let unit = *unit;
                    let a = typed::<PrimitiveArray<Int64Type>>(
                        &cast(arr, &DataType::Int64).map_err(|e| EncErr::Value(e.to_string()))?,
                    );
                    Box::new(move |i| interval_parts_of_duration(temporal::micros_of(a.value(i), unit)?))
                }
                _ => return Err(unsupported(kind, &dt)),
            };
            Ok(Box::new(move |i, out| {
                let (months, days, micros) = parts(i)?;
                if binary {
                    let m = i32::try_from(months).map_err(|_| EncErr::Value("argument out of range".into()))?;
                    let d = i32::try_from(days).map_err(|_| EncErr::Value("argument out of range".into()))?;
                    out.extend_from_slice(&micros.to_be_bytes());
                    out.extend_from_slice(&d.to_be_bytes());
                    out.extend_from_slice(&m.to_be_bytes());
                } else {
                    interval_text(months, days, micros, out);
                }
                Ok(())
            }))
        }
        Kind::Json | Kind::Jsonb => {
            let version = if binary && kind == Kind::Jsonb { Some(1u8) } else { None };
            if let Some(g) = str_getter(arr) {
                return Ok(Box::new(move |i, out| {
                    if let Some(v) = version {
                        out.push(v);
                    }
                    out.extend_from_slice(g(i).as_bytes());
                    Ok(())
                }));
            }
            json::check_dumpable(&dt).map_err(|_| unsupported(kind, &dt))?;
            let a = arr.clone();
            Ok(Box::new(move |i, out| {
                if let Some(v) = version {
                    out.push(v);
                }
                json::dumps(a.as_ref(), i, out)
            }))
        }
        Kind::Uuid => {
            if let DataType::FixedSizeBinary(16) = dt {
                // The 16 bytes of a UUID (arrow.uuid's storage): to_pylist of an arrow.uuid column
                // is uuid.UUID values; str() is the canonical form, .bytes the bytes.
                let a = typed::<FixedSizeBinaryArray>(arr);
                return Ok(if binary {
                    Box::new(move |i, out| {
                        out.extend_from_slice(a.value(i));
                        Ok(())
                    })
                } else {
                    Box::new(move |i, out| {
                        uuid_text(a.value(i), out);
                        Ok(())
                    })
                });
            }
            if let Some(g) = bytes_getter(arr) {
                // A UUID's 16 bytes in a plain binary column (pgwire's _uuid: a fixed_size_binary(16)
                // without the arrow.uuid extension reads back as bytes), written as that UUID.
                let sixteen = move |i: usize| -> R<&'static [u8]> {
                    let b = g(i);
                    if b.len() == 16 {
                        Ok(b)
                    } else {
                        Err(EncErr::Value("bytes is not a 16-char string".into()))
                    }
                };
                return Ok(if binary {
                    Box::new(move |i, out| {
                        out.extend_from_slice(sixteen(i)?);
                        Ok(())
                    })
                } else {
                    Box::new(move |i, out| {
                        uuid_text(sixteen(i)?, out);
                        Ok(())
                    })
                });
            }
            let g = str_getter(arr).ok_or_else(|| unsupported(kind, &dt))?;
            Ok(if binary {
                Box::new(move |i, out| {
                    out.extend_from_slice(&uuid_bytes(g(i))?);
                    Ok(())
                })
            } else {
                Box::new(move |i, out| {
                    out.extend_from_slice(g(i).as_bytes());
                    Ok(())
                })
            })
        }
        Kind::Array(elem) => array_enc(elem, binary, arr),
    }
}

/// The digit zero of each run of Unicode decimal digits (Nd) past ASCII, in Unicode 15.0.0 (Python
/// 3.12's unicodedata): every Nd character is one of these plus 0..9.
const DECIMAL_ZEROS: [u32; 67] = [
    0x660, 0x6f0, 0x7c0, 0x966, 0x9e6, 0xa66, 0xae6, 0xb66, 0xbe6, 0xc66, 0xce6, 0xd66, 0xde6, 0xe50, 0xed0, 0xf20,
    0x1040, 0x1090, 0x17e0, 0x1810, 0x1946, 0x19d0, 0x1a80, 0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0,
    0xa900, 0xa9d0, 0xa9f0, 0xaa50, 0xabf0, 0xff10, 0x104a0, 0x10d30, 0x11066, 0x110f0, 0x11136, 0x111d0, 0x112f0,
    0x11450, 0x114d0, 0x11650, 0x116c0, 0x11730, 0x118e0, 0x11950, 0x11c50, 0x11d50, 0x11da0, 0x11f50, 0x16a60,
    0x16ac0, 0x16b50, 0x1d7ce, 0x1d7d8, 0x1d7e2, 0x1d7ec, 0x1d7f6, 0x1e140, 0x1e2f0, 0x1e4f0, 0x1e950, 0x1fbf0,
];

/// The ASCII byte int() reads a character as (_PyUnicode_TransformDecimalAndSpaceToASCII): Unicode
/// whitespace a space, a decimal digit its ASCII digit, anything else past ASCII invalid.
pub(crate) fn int_ascii(c: char) -> u8 {
    if c.is_ascii() {
        return c as u8;
    }
    if is_py_space(c) {
        return b' ';
    }
    let u = c as u32;
    match DECIMAL_ZEROS.iter().rev().find(|&&z| z <= u) {
        Some(&z) if u - z < 10 => b'0' + (u - z) as u8,
        _ => b'?',
    }
}

/// int(s, 16) of at most 32 characters (PyLong_FromString): ASCII whitespace around it, a `+`, an
/// optional 0x prefix and one underscore after it, then hex digits with single underscores between.
fn int_hex_128(s: &[u8]) -> Option<u128> {
    let is_space = |c: u8| matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r');
    let mut p = 0;
    while p < s.len() && is_space(s[p]) {
        p += 1;
    }
    if s.get(p) == Some(&b'+') {
        p += 1;
    }
    if s.get(p) == Some(&b'0') && matches!(s.get(p + 1), Some(b'x' | b'X')) {
        p += 2;
        if s.get(p) == Some(&b'_') {
            p += 1;
        }
    }
    let mut v: u128 = 0;
    let mut digits = 0;
    let mut prev_underscore = true; // no underscore first
    while p < s.len() {
        let c = s[p];
        if c == b'_' {
            if prev_underscore {
                return None;
            }
            prev_underscore = true;
        } else if let Some(d) = (c as char).to_digit(16) {
            v = (v << 4) | d as u128;
            digits += 1;
            prev_underscore = false;
        } else {
            break;
        }
        p += 1;
    }
    if digits == 0 || prev_underscore {
        return None;
    }
    while p < s.len() && is_space(s[p]) {
        p += 1;
    }
    (p == s.len()).then_some(v)
}

/// uuid.UUID(str(u)).bytes: urn: and uuid: dropped anywhere, braces stripped from the ends, hyphens
/// dropped anywhere, then 32 characters that int(hex, 16) reads.
pub(crate) fn uuid_bytes(s: &str) -> R<[u8; 16]> {
    let bad = || EncErr::Value("badly formed hexadecimal UUID string".into());
    let s = s.replace("urn:", "").replace("uuid:", "");
    let s = s.trim_matches(|c| c == '{' || c == '}').replace('-', "");
    if s.chars().count() != 32 {
        return Err(bad());
    }
    let ascii: Vec<u8> = s.chars().map(int_ascii).collect();
    let v =
        int_hex_128(&ascii).ok_or_else(|| EncErr::Value(format!("invalid literal for int() with base 16: {s:?}")))?;
    Ok(v.to_be_bytes())
}

/// str(uuid.UUID(bytes=b)): lower-case hex in 8-4-4-4-12 groups.
pub(crate) fn uuid_text(b: &[u8], out: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (k, &byte) in b.iter().enumerate() {
        if matches!(k, 4 | 6 | 8 | 10) {
            out.push(b'-');
        }
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 15) as usize]);
    }
}

/// A list column's (start, end) offsets into its child values at a row index.
type Offsets = Box<dyn Fn(usize) -> (usize, usize) + Send + Sync>;

/// The list column's child values and per-row offsets, for any list layout.
fn list_parts(arr: &ArrayRef) -> Option<(ArrayRef, Offsets)> {
    match arr.data_type() {
        DataType::List(_) => {
            let a = typed::<ListArray>(arr);
            let values = a.values().clone();
            Some((
                values,
                Box::new(move |i| {
                    let o = a.value_offsets();
                    (o[i] as usize, o[i + 1] as usize)
                }),
            ))
        }
        DataType::LargeList(_) => {
            let a = typed::<LargeListArray>(arr);
            let values = a.values().clone();
            Some((
                values,
                Box::new(move |i| {
                    let o = a.value_offsets();
                    (o[i] as usize, o[i + 1] as usize)
                }),
            ))
        }
        DataType::FixedSizeList(_, n) => {
            let a = typed::<FixedSizeListArray>(arr);
            let n = *n as usize;
            let values = a.values().clone();
            Some((values, Box::new(move |i| (i * n, i * n + n))))
        }
        _ => None,
    }
}

/// A nesting level of an array column: a list over the next level, or the element values.
enum Level {
    List(ArrayRef, Offsets, Box<Level>),
    Leaf(ArrayRef, Enc),
}

fn build_level(arr: &ArrayRef, elem: Elem, binary: bool) -> R<Level> {
    match list_parts(arr) {
        Some((values, offs)) => {
            let inner = build_level(&values, elem, binary)?;
            Ok(Level::List(arr.clone(), offs, Box::new(inner)))
        }
        None => {
            let enc = scalar_enc(elem.kind(), binary, arr)?;
            Ok(Level::Leaf(arr.clone(), enc))
        }
    }
}

pub(crate) fn is_py_space(c: char) -> bool {
    // str.isspace(), which Python's \s matches in a str pattern.
    matches!(c, '\t'..='\r' | '\x1c'..='\x1f' | ' ' | '\u{85}' | '\u{a0}' | '\u{1680}'
        | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}')
}

/// _array_text_element applied to the element text just written at out[start..].
fn quote_element(out: &mut Vec<u8>, start: usize) {
    let text = std::str::from_utf8(&out[start..]).expect("element text is UTF-8");
    let needs = text.is_empty()
        || text.eq_ignore_ascii_case("NULL")
        || text
            .chars()
            .any(|c| matches!(c, '{' | '}' | '"' | ',' | '\\') || is_py_space(c));
    if !needs {
        return;
    }
    let mut quoted = Vec::with_capacity(text.len() + 2);
    quoted.push(b'"');
    for &b in text.as_bytes() {
        if b == b'\\' || b == b'"' {
            quoted.push(b'\\');
        }
        quoted.push(b);
    }
    quoted.push(b'"');
    out.truncate(start);
    out.extend_from_slice(&quoted);
}

/// Whether the row's array holds nothing at any depth: PostgreSQL writes it {}.
fn level_empty(level: &Level, i: usize) -> bool {
    match level {
        Level::List(_, offs, inner) => {
            let (s, e) = offs(i);
            match inner.as_ref() {
                Level::List(a, _, _) => (s..e).all(|j| !a.is_null(j) && level_empty(inner, j)),
                Level::Leaf(..) => s == e,
            }
        }
        Level::Leaf(..) => false,
    }
}

fn level_text(level: &Level, i: usize, out: &mut Vec<u8>) -> R<()> {
    if level_empty(level, i) {
        out.extend_from_slice(b"{}");
        return Ok(());
    }
    level_items_text(level, i, out)
}

fn level_items_text(level: &Level, i: usize, out: &mut Vec<u8>) -> R<()> {
    match level {
        Level::List(_, offs, inner) => {
            let (s, e) = offs(i);
            out.push(b'{');
            for (n, j) in (s..e).enumerate() {
                if n > 0 {
                    out.push(b',');
                }
                let child_arr = match inner.as_ref() {
                    Level::List(a, _, _) | Level::Leaf(a, _) => a,
                };
                if child_arr.is_null(j) {
                    out.extend_from_slice(b"NULL");
                } else if let Level::Leaf(_, enc) = inner.as_ref() {
                    let start = out.len();
                    enc(j, out)?;
                    quote_element(out, start);
                } else {
                    level_items_text(inner, j, out)?;
                }
            }
            out.push(b'}');
            Ok(())
        }
        Level::Leaf(..) => unreachable!("an array column's top level is a list"),
    }
}

/// _array_shape's dimensions and flattened leaf indices (None for a NULL element).
fn level_shape(level: &Level, i: usize, dims: &mut Vec<usize>, flat: &mut Vec<Option<usize>>) -> R<()> {
    // dimensions: each level's length, read down the first element while it is a list.
    let mut lvl = level;
    let mut idx = Some(i);
    while let (Level::List(_, offs, inner), Some(k)) = (lvl, idx) {
        let (s, e) = offs(k);
        dims.push(e - s);
        let child = match inner.as_ref() {
            Level::List(a, _, _) => a,
            Level::Leaf(..) => break,
        };
        idx = if e > s && !child.is_null(s) { Some(s) } else { None };
        lvl = inner;
    }
    // flatten len(dims)-1 times; a NULL row where a list is expected cannot be iterated.
    let mut rows: Vec<(usize, &Level)> = vec![(i, level)];
    for _ in 1..dims.len() {
        let mut next = Vec::new();
        for (k, lvl) in rows {
            let Level::List(_, offs, inner) = lvl else {
                return Err(EncErr::Type("'int' object is not iterable".into()));
            };
            let (s, e) = offs(k);
            for j in s..e {
                match inner.as_ref() {
                    Level::List(a, _, _) => {
                        if a.is_null(j) {
                            return Err(EncErr::Type("'NoneType' object is not iterable".into()));
                        }
                        next.push((j, inner.as_ref()));
                    }
                    Level::Leaf(..) => {
                        return Err(EncErr::Type("'int' object is not iterable".into()));
                    }
                }
            }
        }
        rows = next;
    }
    // A list left where an element is expected: PostgreSQL holds no such array (the product writes
    // str(list) as the element).
    let mut list_element = false;
    for (k, lvl) in rows {
        let Level::List(_, offs, inner) = lvl else {
            unreachable!()
        };
        let (s, e) = offs(k);
        for j in s..e {
            match inner.as_ref() {
                Level::Leaf(a, _) => flat.push(if a.is_null(j) { None } else { Some(j) }),
                Level::List(a, _, _) => {
                    let null = a.is_null(j);
                    list_element |= !null;
                    flat.push(None.filter(|_: &usize| null));
                }
            }
        }
    }
    let expected: usize = dims.iter().product();
    if flat.len() != expected {
        return Err(EncErr::Value("multidimensional array has unequal sublists".into()));
    }
    if list_element {
        return Err(EncErr::Value(
            "malformed array literal: a sub-array as an element".into(),
        ));
    }
    // An array with no elements has no dimensions (array_send of '{}').
    if expected == 0 {
        dims.clear();
    }
    Ok(())
}

fn leaf_of(level: &Level) -> &Enc {
    match level {
        Level::List(_, _, inner) => leaf_of(inner),
        Level::Leaf(_, enc) => enc,
    }
}

fn array_enc(elem: Elem, binary: bool, arr: &ArrayRef) -> R<Enc> {
    let dt = arr.data_type().clone();
    if list_parts(arr).is_none() {
        return Err(unsupported(Kind::Array(elem), &dt));
    }
    let level = build_level(arr, elem, binary)?;
    let oid = elem.oid();
    if !binary {
        return Ok(Box::new(move |i, out| level_text(&level, i, out)));
    }
    Ok(Box::new(move |i, out| {
        let mut dims = Vec::new();
        let mut flat = Vec::new();
        level_shape(&level, i, &mut dims, &mut flat)?;
        let has_null = flat.iter().any(|v| v.is_none());
        out.extend_from_slice(&(dims.len() as i32).to_be_bytes());
        out.extend_from_slice(&(has_null as i32).to_be_bytes());
        out.extend_from_slice(&oid.to_be_bytes());
        for d in &dims {
            out.extend_from_slice(&(*d as i32).to_be_bytes());
            out.extend_from_slice(&1i32.to_be_bytes());
        }
        let enc = leaf_of(&level);
        for v in flat {
            match v {
                None => out.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(j) => {
                    let p = out.len();
                    out.extend_from_slice(&[0; 4]);
                    enc(j, out)?;
                    let n = (out.len() - p - 4) as i32;
                    out[p..p + 4].copy_from_slice(&n.to_be_bytes());
                }
            }
        }
        Ok(())
    }))
}

/// How one column's cells are written.
enum Col {
    /// An Arrow null column: every cell NULL.
    Null,
    /// A direct Arrow writer.
    Fast(ArrayRef, Enc),
    /// The cell's value (column_values), its coercers, then its BVType's writer.
    Values {
        reader: pyval::Reader,
        coercers: Vec<Coercer>,
        bv: Bv,
        binary: bool,
    },
}

/// A column's types and coercers, as the encoder is told them.
pub struct Plan {
    bvs: Vec<Bv>,
    kinds: Vec<Option<Kind>>,
    coercers: Vec<Vec<Coercer>>,
}

impl Plan {
    fn new(types: &[String], coercers: Option<Vec<Vec<String>>>) -> R<Plan> {
        let coercers = match coercers {
            Some(c) => {
                if c.len() != types.len() {
                    return Err(EncErr::Value(format!(
                        "{} coercer lists for {} columns",
                        c.len(),
                        types.len()
                    )));
                }
                c.iter()
                    .map(|names| names.iter().map(|n| coerce::coercer_of(n)).collect::<R<Vec<_>>>())
                    .collect::<R<Vec<_>>>()?
            }
            None => vec![Vec::new(); types.len()],
        };
        Ok(Plan {
            bvs: types.iter().map(|t| pgtypes::bv_of(t)).collect(),
            kinds: types.iter().map(|t| kind_of(t)).collect(),
            coercers,
        })
    }
}

fn column(plan: &Plan, j: usize, arr: &ArrayRef, ext: pyval::Ext, format: i32) -> R<Col> {
    if *arr.data_type() == DataType::Null {
        return Ok(Col::Null);
    }
    let bv = plan.bvs[j];
    // send_data_rows writes binary when the format asks for it and the type has a binary writer.
    let binary = format == 1 && pgtypes::has_binary(bv);
    if plan.coercers[j].is_empty() {
        // An arrow.uuid column's values are UUIDs: only the uuid writer reads its bytes directly.
        // DuckDB's opaque types are read from their own bytes, never written as those bytes.
        if let Some(kind) = plan.kinds[j].filter(|k| match ext {
            pyval::Ext::Plain => true,
            pyval::Ext::Uuid => *k == Kind::Uuid,
            _ => false,
        }) {
            match scalar_enc(kind, binary, arr) {
                Ok(enc) => return Ok(Col::Fast(arr.clone(), enc)),
                Err(EncErr::NoDirectWriter(_)) => {} // an Arrow type the direct writer does not take
                Err(e) => return Err(e),
            }
        }
    }
    Ok(Col::Values {
        reader: pyval::Reader::new(arr, ext)?,
        coercers: plan.coercers[j].clone(),
        bv,
        binary,
    })
}

fn encode(batch: &RecordBatch, plan: &Plan, formats: &[i32], out: &mut Vec<u8>) -> R<usize> {
    let ncols = batch.num_columns();
    if plan.bvs.len() != ncols {
        return Err(EncErr::Value(format!("{} types for {} columns", plan.bvs.len(), ncols)));
    }
    let nrows = batch.num_rows();
    if nrows == 0 {
        return Ok(0); // stream_rows_from_arrow skips an empty batch: no converter runs
    }
    let schema = batch.schema();
    let mut cols: Vec<Col> = Vec::with_capacity(ncols);
    for j in 0..ncols {
        let ext = pyval::Ext::of(schema.field(j));
        let format = formats.get(j).copied().unwrap_or(0);
        cols.push(column(plan, j, batch.column(j), ext, format)?);
    }
    out.reserve(nrows * (7 + ncols * 12));
    let ncols16 = ncols as i16;
    for i in 0..nrows {
        let msg = out.len();
        out.push(b'D');
        out.extend_from_slice(&[0; 4]);
        out.extend_from_slice(&ncols16.to_be_bytes());
        for col in &cols {
            let p = out.len();
            out.extend_from_slice(&[0; 4]);
            let written = match col {
                Col::Fast(arr, enc) if !arr.is_null(i) => {
                    enc(i, out)?;
                    true
                }
                Col::Values {
                    reader,
                    coercers,
                    bv,
                    binary,
                } if !reader.is_null(i) => {
                    let mut v = reader.get(i)?;
                    for c in coercers {
                        v = coerce::apply(c, v)?;
                        if v == pyval::PyVal::None {
                            break; // a coercer's None is a NULL cell
                        }
                    }
                    if v == pyval::PyVal::None {
                        false
                    } else {
                        pgtypes::convert(*bv, *binary, &v, out)?;
                        true
                    }
                }
                _ => false,
            };
            if written {
                let n = (out.len() - p - 4) as i32;
                out[p..p + 4].copy_from_slice(&n.to_be_bytes());
            } else {
                out.truncate(p);
                out.extend_from_slice(b"\xff\xff\xff\xff");
            }
        }
        let len = (out.len() - msg - 1) as i32;
        out[msg + 1..msg + 5].copy_from_slice(&len.to_be_bytes());
    }
    Ok(nrows)
}

/// The DataRow messages of every row of `batch`, each column encoded as the BVType named in
/// `types` (BVType member names; "xml[]" for xml's array) in format `formats[i]` (0 text,
/// 1 binary), its values first brought through the coercers named in `coercers[i]`.
#[pyfunction]
#[pyo3(signature = (batch, types, formats=None, coercers=None))]
fn encode_batch<'py>(
    py: Python<'py>,
    batch: &Bound<'py, PyAny>,
    types: Vec<String>,
    formats: Option<Vec<i32>>,
    coercers: Option<Vec<Vec<String>>>,
) -> PyResult<Bound<'py, PyBytes>> {
    let batch = RecordBatch::from_pyarrow_bound(batch)?;
    let plan = Plan::new(&types, coercers)?;
    let formats = formats.unwrap_or_default();
    let out = py.allow_threads(|| {
        let mut out = Vec::new();
        encode(&batch, &plan, &formats, &mut out).map(|_| out)
    })?;
    Ok(PyBytes::new(py, &out))
}

/// Streams every batch of a pyarrow RecordBatchReader (or any iterable of batches) through the
/// encoder, handing each batch's DataRow bytes to `write` — the shape send_data_rows has, one write
/// per chunk. Returns the row count.
#[pyfunction]
#[pyo3(signature = (batches, types, write, formats=None, coercers=None))]
fn encode_stream<'py>(
    py: Python<'py>,
    batches: &Bound<'py, PyAny>,
    types: Vec<String>,
    write: &Bound<'py, PyAny>,
    formats: Option<Vec<i32>>,
    coercers: Option<Vec<Vec<String>>>,
) -> PyResult<usize> {
    let plan = Arc::new(Plan::new(&types, coercers)?);
    let formats = formats.unwrap_or_default();
    let mut rows = 0;
    let mut out = Vec::new();
    for item in batches.try_iter()? {
        let batch = RecordBatch::from_pyarrow_bound(&item?)?;
        out.clear();
        rows += py.allow_threads(|| encode(&batch, &plan, &formats, &mut out))?;
        if !out.is_empty() {
            write.call1((PyBytes::new(py, &out),))?;
        }
    }
    Ok(rows)
}

/// RawDataRowBytes passthrough: rows a PostgreSQL source sent, each already a whole DataRow
/// message, forwarded verbatim in send_data_rows' chunks of 500 rows (one `write` per chunk).
/// Each is checked to be one DataRow message, so a malformed one never reaches the client's
/// stream. Returns the row count.
#[pyfunction]
fn forward_raw<'py>(py: Python<'py>, rows: &Bound<'py, PyAny>, write: &Bound<'py, PyAny>) -> PyResult<usize> {
    const CHUNK: usize = 500;
    let mut chunk: Vec<u8> = Vec::new();
    let (mut n, mut in_chunk) = (0usize, 0usize);
    for item in rows.try_iter()? {
        let item = item?;
        let msg: &[u8] = item.downcast::<PyBytes>()?.as_bytes();
        check_data_row(msg)?;
        chunk.extend_from_slice(msg);
        n += 1;
        in_chunk += 1;
        if in_chunk == CHUNK {
            write.call1((PyBytes::new(py, &chunk),))?;
            chunk.clear();
            in_chunk = 0;
        }
    }
    if in_chunk > 0 {
        write.call1((PyBytes::new(py, &chunk),))?;
    }
    Ok(n)
}

fn check_data_row(msg: &[u8]) -> R<()> {
    let bad = |why: &str| EncErr::Value(format!("not a DataRow message: {why}"));
    if msg.len() < 7 || msg[0] != b'D' {
        return Err(bad("no 'D' header"));
    }
    let len = i32::from_be_bytes(msg[1..5].try_into().unwrap());
    if len as i64 != msg.len() as i64 - 1 {
        return Err(bad("its length word"));
    }
    let ncols = i16::from_be_bytes(msg[5..7].try_into().unwrap());
    let mut p = 7usize;
    for _ in 0..ncols.max(0) {
        let n = i32::from_be_bytes(
            msg.get(p..p + 4)
                .ok_or_else(|| bad("a cell past its end"))?
                .try_into()
                .unwrap(),
        );
        p += 4;
        if n >= 0 {
            p += n as usize;
        } else if n != -1 {
            return Err(bad("a cell length"));
        }
    }
    if p != msg.len() || ncols < 0 {
        return Err(bad("its cells"));
    }
    Ok(())
}

/// The catalog names the reg* writers read (server._register_reg_types): pg_proc's names and
/// pg_type's, each as (oid, name) in catalog order. Called once at import.
#[pyfunction]
fn register_catalog_names(procs: Vec<(i64, String)>, types: Vec<(i64, String)>) {
    let mut names = pgtypes::CatalogNames::default();
    for (oid, name) in procs {
        names.procs.insert(oid as i128, name);
    }
    for (oid, name) in types {
        names.types.insert(oid as i128, name.clone());
        names.type_oids.insert(name, oid as i128);
    }
    pgtypes::set_catalog_names(names);
}

/// The offsets (seconds east of UTC) of a named zone at instants (seconds after the epoch, within
/// years 1..=9999) as every encoder reads them, or None for a zone the tables do not hold.
#[pyfunction]
fn zone_offsets(name: &str, secs: Vec<i64>) -> Option<Vec<i32>> {
    let zone = tz::Zone::named(name)?;
    Some(secs.into_iter().map(|s| zone.offset_at(s)).collect())
}

#[pymodule]
fn pgwire_encoder(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_batch, m)?)?;
    m.add_function(wrap_pyfunction!(encode_stream, m)?)?;
    m.add_function(wrap_pyfunction!(forward_raw, m)?)?;
    m.add_function(wrap_pyfunction!(register_catalog_names, m)?)?;
    m.add_function(wrap_pyfunction!(zone_offsets, m)?)?;
    m.add("UNICODE_VERSION", unicode_tables::UNICODE_VERSION)?;
    // The IANA release chrono-tz's tables and tz_rules.rs's rules were built from.
    m.add("TZDB_VERSION", chrono_tz::IANA_TZDB_VERSION)?;
    m.add("TZ_RULES_VERSION", tz_rules::TZDB_VERSION)?;
    // The zones every encoder takes (the same tables serve pgwire and the JSON encoders).
    m.add("ZONE_NAMES", tz::Zone::names())?;
    Ok(())
}
