//! An Arrow cell as the Python value the product holds for it: `column_values` for a top-level
//! column (`to_pylist`, an interval as the IR interval, PostgreSQL's infinite date or µs timestamp
//! as the IR infinity) and plain `to_pylist` below it. Every Arrow type pyarrow converts is read;
//! the conversions `to_pylist` refuses (a date past year 9999, a duration past a billion days, a
//! struct with repeated field names) fail as it fails, for every row of the column.

use std::borrow::Cow;

use arrow::array::*;
use arrow::buffer::NullBuffer;
use arrow::datatypes::*;

use crate::fmt;
use crate::pytz_data;

/// Why an answer cannot be written. `Value` is an error the product's path raises (a ValueError,
/// OverflowError, KeyError: a 500); the two refusals are REQ-1867's, answered by name.
#[derive(Debug)]
pub enum Error {
    Value(String),
    /// `JsonValueRefused(field, reason)`.
    ValueRefused(String, String),
    /// `JsonResponseRefused(reason)`.
    ResponseRefused(String),
    /// OData's `QueryOptionRefused(status, code, message)`.
    ODataRefused(u16, String, String),
}

pub type Res<T> = Result<T, Error>;

pub fn err<T>(m: impl Into<String>) -> Res<T> {
    Err(Error::Value(m.into()))
}

/// A Python `Decimal` that is finite: `(-1)^neg × coef × 10^exp`, `coef` its digits as Python
/// keeps them (no sign; "0" for zero; leading zeros dropped).
#[derive(Clone, Debug)]
pub struct Dec<'a> {
    pub neg: bool,
    pub coef: Cow<'a, str>,
    pub exp: i32,
}

/// A `datetime` (or a pandas `Timestamp`, for a nanosecond column): its wall-clock time.
#[derive(Clone, Copy, Debug)]
pub struct Dt {
    /// Microseconds from 1970-01-01T00:00 of the local wall clock.
    pub local_us: i64,
    /// The UTC offset in seconds; None for a naive value.
    pub offset: Option<i32>,
    /// A pandas `Timestamp`: its nanoseconds below the microsecond.
    pub nanos: Option<u16>,
    /// Its tzinfo, for its repr.
    pub tz: Tz,
}

/// The tzinfo of a datetime.
#[derive(Clone, Copy, Debug)]
pub enum Tz {
    Naive,
    /// `pytz.FixedOffset(minutes)` (pytz's `utc` for 0).
    Fixed(i32),
    /// A pytz zone (index into `ZONES`) at one of its transitions (index into `TRANS`).
    Named(u32, u32),
    /// `datetime.timezone(timedelta(seconds=...))`.
    Py(i32),
}

/// A Python value as the product's path holds one.
#[derive(Clone, Debug)]
pub enum P<'a> {
    None,
    Bool(bool),
    Int(i128),
    /// An int past i128, as its decimal digits (with a leading `-` when negative).
    BigInt(Cow<'a, str>),
    Float(f64),
    /// `serialize.JsonNumber`: a float that carries the text it was written as.
    JNum(Cow<'a, str>),
    /// A `str` as WTF-8 (a lone surrogate from a JSON escape is kept as its three bytes).
    Str(Cow<'a, [u8]>),
    Bytes(Cow<'a, [u8]>),
    Dec(Dec<'a>),
    /// A Decimal NaN, sNaN or infinity (OData's exact read of JSON text): `NaN`, `sNaN`,
    /// `Infinity`, `-Infinity`, ...
    DecSpecial(Cow<'a, str>),
    /// A `date`: days from 1970-01-01, year within 1..=9999.
    Date(i32),
    /// The IR infinite date (`ir_types.infinite_date`), 1 or -1.
    InfDate(i8),
    Dt(Dt),
    /// The IR infinite timestamp: its sign, and whether it is aware (in UTC).
    InfDt(i8, bool),
    /// pandas `NaT` (a nanosecond timestamp or duration holding i64::MIN).
    NaT,
    /// A `time` of day in microseconds.
    Time(i64),
    /// A `timedelta` in microseconds; `nanos` for a pandas `Timedelta`, its nanoseconds below
    /// the microsecond (the microseconds then floor the whole).
    Td(i128, Option<u16>),
    /// An IR interval (`ir_types.interval_value`): months, days, microseconds.
    Iv(i64, i64, i64),
    /// pyarrow's `MonthDayNano` named tuple (an interval below the top level).
    Mdn(i32, i32, i64),
    Uuid([u8; 16]),
    List(Vec<P<'a>>),
    Tuple(Vec<P<'a>>),
    /// A dict with str keys, in insertion order.
    Dict(Vec<(Cow<'a, [u8]>, P<'a>)>),
}

impl<'a> P<'a> {
    pub fn is_none(&self) -> bool {
        matches!(self, P::None)
    }
    pub fn into_owned(self) -> P<'static> {
        match self {
            P::None => P::None,
            P::Bool(b) => P::Bool(b),
            P::Int(i) => P::Int(i),
            P::BigInt(s) => P::BigInt(Cow::Owned(s.into_owned())),
            P::Float(f) => P::Float(f),
            P::JNum(s) => P::JNum(Cow::Owned(s.into_owned())),
            P::Str(s) => P::Str(Cow::Owned(s.into_owned())),
            P::Bytes(s) => P::Bytes(Cow::Owned(s.into_owned())),
            P::Dec(d) => P::Dec(Dec { neg: d.neg, coef: Cow::Owned(d.coef.into_owned()), exp: d.exp }),
            P::DecSpecial(s) => P::DecSpecial(Cow::Owned(s.into_owned())),
            P::Date(d) => P::Date(d),
            P::InfDate(s) => P::InfDate(s),
            P::Dt(d) => P::Dt(d),
            P::InfDt(s, a) => P::InfDt(s, a),
            P::NaT => P::NaT,
            P::Time(t) => P::Time(t),
            P::Td(t, n) => P::Td(t, n),
            P::Iv(m, d, u) => P::Iv(m, d, u),
            P::Mdn(m, d, n) => P::Mdn(m, d, n),
            P::Uuid(u) => P::Uuid(u),
            P::List(v) => P::List(v.into_iter().map(P::into_owned).collect()),
            P::Tuple(v) => P::Tuple(v.into_iter().map(P::into_owned).collect()),
            P::Dict(v) => P::Dict(v.into_iter().map(|(k, x)| (Cow::Owned(k.into_owned()), x.into_owned())).collect()),
        }
    }
    pub fn str(s: &'a str) -> P<'a> {
        P::Str(Cow::Borrowed(s.as_bytes()))
    }
    pub fn owned_str(s: String) -> P<'static> {
        P::Str(Cow::Owned(s.into_bytes()))
    }
    /// `d[k] = v` on a dict being built: a repeated key keeps its first position.
    pub fn dict_set(entries: &mut Vec<(Cow<'a, [u8]>, P<'a>)>, k: Cow<'a, [u8]>, v: P<'a>) {
        match entries.iter_mut().find(|e| e.0 == k) {
            Some(e) => e.1 = v,
            None => entries.push((k, v)),
        }
    }
}

// ---------------------------------------------------------------- time zones and ranges

const DATE_INF: i32 = i32::MAX;
const TS_INF: i64 = i64::MAX;
const TD_MAX_DAYS: i128 = 999_999_999;
const DAY_US: i64 = 86_400_000_000;
/// Microseconds from 1970 of 0001-01-01T00:00 and of 10000-01-01T00:00.
const MIN_US: i64 = -62_135_596_800_000_000;
const END_US: i64 = 253_402_300_800_000_000;

/// The zone of an aware timestamp column.
#[derive(Clone)]
pub enum Zone {
    Fixed(i32),
    /// A pytz zone (index into `ZONES`): its transitions, as pytz localizes with them.
    Named(u32),
}

impl Zone {
    pub fn parse(tz: &str) -> Res<Zone> {
        let b = tz.as_bytes();
        if (b.len() == 6 && b[3] == b':' || b.len() == 5) && (b[0] == b'+' || b[0] == b'-') {
            let (h, m) = if b.len() == 6 { (&tz[1..3], &tz[4..6]) } else { (&tz[1..3], &tz[3..5]) };
            if let (Ok(h), Ok(m)) = (h.parse::<i32>(), m.parse::<i32>()) {
                let off = (h * 60 + m) * 60;
                return Ok(Zone::Fixed(if b[0] == b'-' { -off } else { off }));
            }
        }
        match pytz_data::ZONES.binary_search_by(|z| z.0.cmp(tz)) {
            Ok(i) => Ok(Zone::Named(i as u32)),
            Err(_) => err(format!("Cannot locate timezone '{tz}'")),
        }
    }

    /// The offset in seconds at the UTC instant `utc_us` (pytz's `fromutc`: the last transition at
    /// or before it), and the tzinfo it gives.
    pub fn offset_at(&self, utc_us: i64) -> (i32, Tz) {
        match self {
            Zone::Fixed(o) => (*o, Tz::Fixed(*o / 60)),
            Zone::Named(z) => {
                let (_, start, n, _) = pytz_data::ZONES[*z as usize];
                let t = &pytz_data::TRANS[start as usize..(start + n) as usize];
                let secs = utc_us.div_euclid(1_000_000);
                let i = t.partition_point(|e| e.0 <= secs).saturating_sub(1);
                (t[i].1, Tz::Named(*z, start + i as u32))
            }
        }
    }
}

pub fn date_ok(days: i64) -> Res<()> {
    let (y, _, _) = fmt::civil(days);
    if !(1..=9999).contains(&y) {
        return err(format!("year {} is out of range", y));
    }
    Ok(())
}

fn dt_of(utc_us: i64, zone: Option<&Zone>, nanos: Option<u16>) -> Res<Dt> {
    let (offset, tz) = match zone.map(|z| z.offset_at(utc_us)) {
        Some((o, tz)) => (Some(o), tz),
        None => (None, Tz::Naive),
    };
    let local = utc_us as i128 + offset.unwrap_or(0) as i128 * 1_000_000;
    if nanos.is_none() && !(MIN_US as i128..END_US as i128).contains(&local) {
        return err("year is out of range");
    }
    Ok(Dt { local_us: local as i64, offset, nanos, tz })
}

/// A `timedelta` of `us` microseconds, which Python holds within a billion days (floor days).
fn td_of(us: i128) -> Res<P<'static>> {
    if us.div_euclid(DAY_US as i128).abs() > TD_MAX_DAYS {
        return err("days=…; must have magnitude <= 999999999");
    }
    Ok(P::Td(us, None))
}

/// `interval_value`'s OverflowError: a month-free interval within a billion days is built as a
/// timedelta, which normalizes its microseconds into days.
fn iv_of(months: i32, days: i32, micros: i64) -> Res<P<'static>> {
    if months == 0 && (days as i128).abs() <= TD_MAX_DAYS {
        let nd = days as i128 + (micros as i128).div_euclid(DAY_US as i128);
        if nd.abs() > TD_MAX_DAYS {
            return err("days=…; must have magnitude <= 999999999");
        }
    }
    Ok(P::Iv(months as i64, days as i64, micros))
}

// ---------------------------------------------------------------- readers

#[derive(Clone, Copy, PartialEq)]
pub enum Unit {
    S,
    Ms,
    Us,
    Ns,
}

impl Unit {
    fn of(u: &TimeUnit) -> Unit {
        match u {
            TimeUnit::Second => Unit::S,
            TimeUnit::Millisecond => Unit::Ms,
            TimeUnit::Microsecond => Unit::Us,
            TimeUnit::Nanosecond => Unit::Ns,
        }
    }
    fn us_mult(self) -> i64 {
        match self {
            Unit::S => 1_000_000,
            Unit::Ms => 1_000,
            Unit::Us | Unit::Ns => 1,
        }
    }
}

/// How one Arrow array is read as Python values.
pub enum Reader {
    Null,
    Bool(BooleanArray),
    Int(PrimitiveArray<Int64Type>),
    UInt(PrimitiveArray<UInt64Type>),
    Float(PrimitiveArray<Float64Type>),
    Dec128(PrimitiveArray<Decimal128Type>, i32),
    Dec256(PrimitiveArray<Decimal256Type>, i32),
    Utf8(StringArray),
    LUtf8(LargeStringArray),
    VUtf8(StringViewArray),
    Bin(BinaryArray),
    LBin(LargeBinaryArray),
    VBin(BinaryViewArray),
    FBin(FixedSizeBinaryArray),
    Uuid(FixedSizeBinaryArray),
    /// days; `top`: ±(2^31-1) is the IR infinity
    Date32(PrimitiveArray<Date32Type>, bool),
    Date64(PrimitiveArray<Date64Type>),
    /// raw time-of-day in `unit`
    Time(PrimitiveArray<Int64Type>, Unit),
    /// raw instants, unit, zone; `infinities` for a top-level µs column
    Ts(PrimitiveArray<Int64Type>, Unit, Option<Zone>, bool),
    Dur(PrimitiveArray<Int64Type>, Unit),
    /// month_day_nano; `top`: the IR interval, else pyarrow's MonthDayNano
    Iv(IntervalMonthDayNanoArray, bool),
    List(OffsetBuffer32, Option<NullBuffer>, Box<Reader>),
    LList(OffsetBuffer64, Option<NullBuffer>, Box<Reader>),
    FList(i32, usize, Option<NullBuffer>, Box<Reader>),
    ListView(Vec<(usize, usize)>, Option<NullBuffer>, Box<Reader>),
    Struct(Option<NullBuffer>, Vec<(String, Reader)>, bool),
    Map(OffsetBuffer32, Option<NullBuffer>, Box<Reader>, Box<Reader>),
    Dict(Vec<usize>, Option<NullBuffer>, Box<Reader>),
    Union(Vec<i8>, Option<Vec<i32>>, Vec<(i8, Reader)>),
    Ree(Vec<usize>, Box<Reader>),
}

type OffsetBuffer32 = arrow::buffer::OffsetBuffer<i32>;
type OffsetBuffer64 = arrow::buffer::OffsetBuffer<i64>;

fn is_uuid(f: &Field) -> bool {
    f.metadata().get("ARROW:extension:name").is_some_and(|n| n == "arrow.uuid")
}

fn i64s<T: ArrowPrimitiveType>(a: &dyn Array) -> PrimitiveArray<Int64Type>
where
    T::Native: Into<i64>,
{
    a.as_primitive::<T>().unary(|v| v.into())
}

fn u64s<T: ArrowPrimitiveType>(a: &dyn Array) -> PrimitiveArray<UInt64Type>
where
    T::Native: Into<u64>,
{
    a.as_primitive::<T>().unary(|v| v.into())
}

impl Reader {
    /// The reader of `a`; `top` for a result column (`column_values`), false below it.
    pub fn new(field: &Field, a: &ArrayRef, top: bool) -> Res<Reader> {
        let uuid = is_uuid(field);
        Ok(match a.data_type() {
            DataType::Null => Reader::Null,
            DataType::Boolean => Reader::Bool(a.as_boolean().clone()),
            DataType::Int8 => Reader::Int(i64s::<Int8Type>(a)),
            DataType::Int16 => Reader::Int(i64s::<Int16Type>(a)),
            DataType::Int32 => Reader::Int(i64s::<Int32Type>(a)),
            DataType::Int64 => Reader::Int(a.as_primitive::<Int64Type>().clone()),
            DataType::UInt8 => Reader::UInt(u64s::<UInt8Type>(a)),
            DataType::UInt16 => Reader::UInt(u64s::<UInt16Type>(a)),
            DataType::UInt32 => Reader::UInt(u64s::<UInt32Type>(a)),
            DataType::UInt64 => Reader::UInt(a.as_primitive::<UInt64Type>().clone()),
            DataType::Float16 => Reader::Float(a.as_primitive::<Float16Type>().unary(|v| v.to_f64())),
            DataType::Float32 => Reader::Float(a.as_primitive::<Float32Type>().unary(|v| v as f64)),
            DataType::Float64 => Reader::Float(a.as_primitive::<Float64Type>().clone()),
            DataType::Decimal32(_, s) => Reader::Dec128(a.as_primitive::<Decimal32Type>().unary(|v| v as i128), *s as i32),
            DataType::Decimal64(_, s) => Reader::Dec128(a.as_primitive::<Decimal64Type>().unary(|v| v as i128), *s as i32),
            DataType::Decimal128(_, s) => Reader::Dec128(a.as_primitive::<Decimal128Type>().clone(), *s as i32),
            DataType::Decimal256(_, s) => Reader::Dec256(a.as_primitive::<Decimal256Type>().clone(), *s as i32),
            DataType::Utf8 => Reader::Utf8(a.as_string::<i32>().clone()),
            DataType::LargeUtf8 => Reader::LUtf8(a.as_string::<i64>().clone()),
            DataType::Utf8View => Reader::VUtf8(a.as_string_view().clone()),
            DataType::Binary => Reader::Bin(a.as_binary::<i32>().clone()),
            DataType::LargeBinary => Reader::LBin(a.as_binary::<i64>().clone()),
            DataType::BinaryView => Reader::VBin(a.as_binary_view().clone()),
            DataType::FixedSizeBinary(16) if uuid => Reader::Uuid(a.as_fixed_size_binary().clone()),
            DataType::FixedSizeBinary(_) => Reader::FBin(a.as_fixed_size_binary().clone()),
            DataType::Date32 => Reader::Date32(a.as_primitive::<Date32Type>().clone(), top),
            DataType::Date64 => Reader::Date64(a.as_primitive::<Date64Type>().clone()),
            DataType::Time32(TimeUnit::Second) => Reader::Time(i64s::<Time32SecondType>(a), Unit::S),
            DataType::Time32(_) => Reader::Time(i64s::<Time32MillisecondType>(a), Unit::Ms),
            DataType::Time64(TimeUnit::Microsecond) => {
                Reader::Time(a.as_primitive::<Time64MicrosecondType>().reinterpret_cast::<Int64Type>(), Unit::Us)
            }
            DataType::Time64(_) => Reader::Time(a.as_primitive::<Time64NanosecondType>().reinterpret_cast::<Int64Type>(), Unit::Ns),
            DataType::Timestamp(u, tz) => {
                let zone = tz.as_deref().map(Zone::parse).transpose()?;
                let raw = match u {
                    TimeUnit::Second => a.as_primitive::<TimestampSecondType>().reinterpret_cast::<Int64Type>(),
                    TimeUnit::Millisecond => a.as_primitive::<TimestampMillisecondType>().reinterpret_cast::<Int64Type>(),
                    TimeUnit::Microsecond => a.as_primitive::<TimestampMicrosecondType>().reinterpret_cast::<Int64Type>(),
                    TimeUnit::Nanosecond => a.as_primitive::<TimestampNanosecondType>().reinterpret_cast::<Int64Type>(),
                };
                Reader::Ts(raw, Unit::of(u), zone, top && *u == TimeUnit::Microsecond)
            }
            DataType::Duration(u) => {
                let raw = match u {
                    TimeUnit::Second => a.as_primitive::<DurationSecondType>().reinterpret_cast::<Int64Type>(),
                    TimeUnit::Millisecond => a.as_primitive::<DurationMillisecondType>().reinterpret_cast::<Int64Type>(),
                    TimeUnit::Microsecond => a.as_primitive::<DurationMicrosecondType>().reinterpret_cast::<Int64Type>(),
                    TimeUnit::Nanosecond => a.as_primitive::<DurationNanosecondType>().reinterpret_cast::<Int64Type>(),
                };
                Reader::Dur(raw, Unit::of(u))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => Reader::Iv(a.as_primitive::<IntervalMonthDayNanoType>().clone(), top),
            DataType::List(f) => {
                let l = a.as_list::<i32>();
                Reader::List(l.offsets().clone(), l.nulls().cloned(), Box::new(Reader::new(f, l.values(), false)?))
            }
            DataType::LargeList(f) => {
                let l = a.as_list::<i64>();
                Reader::LList(l.offsets().clone(), l.nulls().cloned(), Box::new(Reader::new(f, l.values(), false)?))
            }
            DataType::FixedSizeList(f, n) => {
                let l = a.as_fixed_size_list();
                // value(i) is values[(offset + i) * n ..]: the list's own offset is in `values`.
                let off = l.offset();
                Reader::FList(*n, off, l.nulls().cloned(), Box::new(Reader::new(f, l.values(), false)?))
            }
            DataType::ListView(f) => {
                let l = a.as_list_view::<i32>();
                let spans = (0..l.len()).map(|i| (l.value_offsets()[i] as usize, l.value_sizes()[i] as usize)).collect();
                Reader::ListView(spans, l.nulls().cloned(), Box::new(Reader::new(f, l.values(), false)?))
            }
            DataType::LargeListView(f) => {
                let l = a.as_list_view::<i64>();
                let spans = (0..l.len()).map(|i| (l.value_offsets()[i] as usize, l.value_sizes()[i] as usize)).collect();
                Reader::ListView(spans, l.nulls().cloned(), Box::new(Reader::new(f, l.values(), false)?))
            }
            DataType::Struct(fields) => {
                let s = a.as_struct();
                let mut names: Vec<&String> = fields.iter().map(|f| f.name()).collect();
                names.sort();
                let dup = names.windows(2).any(|w| w[0] == w[1]);
                let mut kids = Vec::new();
                for (i, f) in fields.iter().enumerate() {
                    kids.push((f.name().clone(), Reader::new(f, s.column(i), false)?));
                }
                Reader::Struct(s.nulls().cloned(), kids, dup)
            }
            DataType::Map(f, _) => {
                let m = a.as_map();
                let DataType::Struct(kv) = f.data_type() else { return err("a map's entries are not a struct") };
                let ent = m.entries();
                Reader::Map(
                    m.offsets().clone(),
                    m.nulls().cloned(),
                    Box::new(Reader::new(&kv[0], ent.column(0), false)?),
                    Box::new(Reader::new(&kv[1], ent.column(1), false)?),
                )
            }
            DataType::Dictionary(_, vt) => {
                let d = a.as_any_dictionary();
                let vf = Field::new("", (**vt).clone(), true).with_metadata(field.metadata().clone());
                Reader::Dict(d.normalized_keys(), d.keys().logical_nulls(), Box::new(Reader::new(&vf, d.values(), top)?))
            }
            DataType::Union(fields, mode) => {
                let u = a.as_any().downcast_ref::<UnionArray>().unwrap();
                let ids: Vec<i8> = u.type_ids().iter().copied().collect();
                let offs = match mode {
                    UnionMode::Dense => Some(u.offsets().unwrap().iter().copied().collect()),
                    UnionMode::Sparse => None,
                };
                let mut kids = Vec::new();
                for (tid, f) in fields.iter() {
                    kids.push((tid, Reader::new(f, u.child(tid), false)?));
                }
                Reader::Union(ids, offs, kids)
            }
            DataType::RunEndEncoded(_, vf) => {
                let n = a.len();
                let idx: Vec<usize> = match a.data_type() {
                    DataType::RunEndEncoded(rf, _) => match rf.data_type() {
                        DataType::Int16 => ree_index(a.as_any().downcast_ref::<RunArray<Int16Type>>().unwrap(), n),
                        DataType::Int32 => ree_index(a.as_any().downcast_ref::<RunArray<Int32Type>>().unwrap(), n),
                        _ => ree_index(a.as_any().downcast_ref::<RunArray<Int64Type>>().unwrap(), n),
                    },
                    _ => unreachable!(),
                };
                let values = match a.data_type() {
                    DataType::RunEndEncoded(rf, _) => match rf.data_type() {
                        DataType::Int16 => a.as_any().downcast_ref::<RunArray<Int16Type>>().unwrap().values().clone(),
                        DataType::Int32 => a.as_any().downcast_ref::<RunArray<Int32Type>>().unwrap().values().clone(),
                        _ => a.as_any().downcast_ref::<RunArray<Int64Type>>().unwrap().values().clone(),
                    },
                    _ => unreachable!(),
                };
                Reader::Ree(idx, Box::new(Reader::new(vf, &values, false)?))
            }
            other => return err(format!("Arrow type {other} has no Python value")),
        })
    }

    /// The raw value is None (`row[idx] is None`).
    pub fn is_null(&self, r: usize) -> bool {
        match self {
            Reader::Null => true,
            Reader::Bool(a) => a.is_null(r),
            Reader::Int(a) => a.is_null(r),
            Reader::UInt(a) => a.is_null(r),
            Reader::Float(a) => a.is_null(r),
            Reader::Dec128(a, _) => a.is_null(r),
            Reader::Dec256(a, _) => a.is_null(r),
            Reader::Utf8(a) => a.is_null(r),
            Reader::LUtf8(a) => a.is_null(r),
            Reader::VUtf8(a) => a.is_null(r),
            Reader::Bin(a) => a.is_null(r),
            Reader::LBin(a) => a.is_null(r),
            Reader::VBin(a) => a.is_null(r),
            Reader::FBin(a) | Reader::Uuid(a) => a.is_null(r),
            Reader::Date32(a, _) => a.is_null(r),
            Reader::Date64(a) => a.is_null(r),
            Reader::Time(a, _) | Reader::Ts(a, _, _, _) => a.is_null(r),
            Reader::Dur(a, _) => a.is_null(r),
            Reader::Iv(a, _) => a.is_null(r),
            Reader::List(_, n, _) | Reader::LList(_, n, _) | Reader::FList(_, _, n, _) | Reader::ListView(_, n, _) => {
                n.as_ref().is_some_and(|n| n.is_null(r))
            }
            Reader::Struct(n, _, _) => n.as_ref().is_some_and(|n| n.is_null(r)),
            Reader::Map(_, n, _, _) => n.as_ref().is_some_and(|n| n.is_null(r)),
            Reader::Dict(k, n, v) => n.as_ref().is_some_and(|n| n.is_null(r)) || v.is_null(k[r]),
            Reader::Union(ids, offs, kids) => {
                let (child, at) = union_at(ids, offs, kids, r);
                child.is_null(at)
            }
            Reader::Ree(idx, v) => v.is_null(idx[r]),
        }
    }

    /// The value at `r`.
    pub fn get(&self, r: usize) -> Res<P<'_>> {
        if self.is_null(r) {
            return Ok(P::None);
        }
        Ok(match self {
            Reader::Null => P::None,
            Reader::Bool(a) => P::Bool(a.value(r)),
            Reader::Int(a) => P::Int(a.value(r) as i128),
            Reader::UInt(a) => P::Int(a.value(r) as i128),
            Reader::Float(a) => P::Float(a.value(r)),
            Reader::Dec128(a, s) => dec_of(a.value(r), *s),
            Reader::Dec256(a, s) => {
                let c = a.value(r);
                match c.to_i128() {
                    Some(v) => dec_of(v, *s),
                    None => {
                        let t = c.to_string();
                        let (neg, d) = match t.strip_prefix('-') {
                            Some(d) => (true, d.to_string()),
                            None => (false, t),
                        };
                        P::Dec(Dec { neg, coef: Cow::Owned(d), exp: -*s })
                    }
                }
            }
            Reader::Utf8(a) => P::str(a.value(r)),
            Reader::LUtf8(a) => P::str(a.value(r)),
            Reader::VUtf8(a) => P::str(a.value(r)),
            Reader::Bin(a) => P::Bytes(Cow::Borrowed(a.value(r))),
            Reader::LBin(a) => P::Bytes(Cow::Borrowed(a.value(r))),
            Reader::VBin(a) => P::Bytes(Cow::Borrowed(a.value(r))),
            Reader::FBin(a) => P::Bytes(Cow::Borrowed(a.value(r))),
            Reader::Uuid(a) => P::Uuid(a.value(r).try_into().unwrap()),
            Reader::Date32(a, top) => {
                let v = a.value(r);
                if *top && (v == DATE_INF || v == -DATE_INF) {
                    P::InfDate(if v > 0 { 1 } else { -1 })
                } else {
                    date_ok(v as i64)?;
                    P::Date(v)
                }
            }
            Reader::Date64(a) => {
                let d = a.value(r).div_euclid(86_400_000);
                date_ok(d)?;
                P::Date(d as i32)
            }
            Reader::Time(a, u) => {
                let v = a.value(r);
                let us = if *u == Unit::Ns { v.div_euclid(1000).rem_euclid(DAY_US) } else { v * u.us_mult() };
                if !(0..DAY_US).contains(&us) {
                    return err("time out of range");
                }
                P::Time(us)
            }
            Reader::Ts(a, u, zone, inf) => {
                let v = a.value(r);
                if *inf && (v == TS_INF || v == -TS_INF) {
                    P::InfDt(if v > 0 { 1 } else { -1 }, zone.is_some())
                } else if *u == Unit::Ns {
                    if v == i64::MIN {
                        P::NaT
                    } else {
                        let dt = dt_of(v.div_euclid(1000), zone.as_ref(), Some(v.rem_euclid(1000) as u16))?;
                        // pandas shifts the nanoseconds by the offset in int64: past it, it raises.
                        let local = v as i128 + dt.offset.unwrap_or(0) as i128 * 1_000_000_000;
                        if local <= i64::MIN as i128 || local > i64::MAX as i128 {
                            return err("OutOfBoundsDatetime: the local time overflows nanoseconds");
                        }
                        P::Dt(dt)
                    }
                } else {
                    let us = (v as i128) * u.us_mult() as i128;
                    if us < i64::MIN as i128 / 2 || us > i64::MAX as i128 / 2 {
                        return err("year is out of range");
                    }
                    P::Dt(dt_of(us as i64, zone.as_ref(), None)?)
                }
            }
            Reader::Dur(a, u) => {
                let v = a.value(r);
                if *u == Unit::Ns {
                    if v == i64::MIN {
                        P::NaT
                    } else {
                        P::Td(v.div_euclid(1000) as i128, Some(v.rem_euclid(1000) as u16))
                    }
                } else {
                    td_of(v as i128 * u.us_mult() as i128)?
                }
            }
            Reader::Iv(a, top) => {
                let v = a.value(r);
                if *top {
                    iv_of(v.months, v.days, v.nanoseconds.div_euclid(1000))?
                } else {
                    P::Mdn(v.months, v.days, v.nanoseconds)
                }
            }
            Reader::List(o, _, c) => list_of(c, o[r] as usize, o[r + 1] as usize)?,
            Reader::LList(o, _, c) => list_of(c, o[r] as usize, o[r + 1] as usize)?,
            Reader::FList(n, off, _, c) => {
                let n = *n as usize;
                list_of(c, (off + r) * n, (off + r + 1) * n)?
            }
            Reader::ListView(spans, _, c) => {
                let (o, s) = spans[r];
                list_of(c, o, o + s)?
            }
            Reader::Struct(_, kids, dup) => {
                if *dup {
                    return err("Converting to Python dictionary is not supported when duplicate field names are present");
                }
                let mut e = Vec::with_capacity(kids.len());
                for (name, k) in kids {
                    e.push((Cow::Borrowed(name.as_bytes()), k.get(r)?));
                }
                P::Dict(e)
            }
            Reader::Map(o, _, k, v) => {
                let mut items = Vec::new();
                for i in o[r] as usize..o[r + 1] as usize {
                    items.push(P::Tuple(vec![k.get(i)?, v.get(i)?]));
                }
                P::List(items)
            }
            Reader::Dict(k, _, v) => v.get(k[r])?,
            Reader::Union(ids, offs, kids) => {
                let (child, at) = union_at(ids, offs, kids, r);
                child.get(at)?
            }
            Reader::Ree(idx, v) => v.get(idx[r])?,
        })
    }

    /// Whether reading a value can fail (`to_pylist` refuses it): the column is then read in
    /// full before any row is used, as `column_values` reads it.
    pub fn fallible(&self) -> bool {
        match self {
            Reader::Date32(_, _) | Reader::Date64(_) | Reader::Time(_, _) | Reader::Ts(_, _, _, _) | Reader::Dur(_, _) => true,
            Reader::Iv(_, top) => *top,
            Reader::List(_, _, c) | Reader::LList(_, _, c) | Reader::FList(_, _, _, c) | Reader::ListView(_, _, c) => c.fallible(),
            Reader::Struct(_, kids, dup) => *dup || kids.iter().any(|k| k.1.fallible()),
            Reader::Map(_, _, k, v) => k.fallible() || v.fallible(),
            Reader::Dict(_, _, v) | Reader::Ree(_, v) => v.fallible(),
            Reader::Union(_, _, kids) => kids.iter().any(|k| k.1.fallible()),
            _ => false,
        }
    }

    /// `to_pylist`'s eager errors over `n` rows.
    pub fn validate(&self, n: usize) -> Res<()> {
        if self.fallible() {
            for r in 0..n {
                self.get(r)?;
            }
        }
        Ok(())
    }
}

fn ree_index<R: RunEndIndexType>(a: &RunArray<R>, n: usize) -> Vec<usize> {
    (0..n).map(|i| a.get_physical_index(i)).collect()
}

fn union_at<'r>(ids: &[i8], offs: &Option<Vec<i32>>, kids: &'r [(i8, Reader)], r: usize) -> (&'r Reader, usize) {
    let tid = ids[r];
    let child = &kids.iter().find(|k| k.0 == tid).unwrap().1;
    let at = match offs {
        Some(o) => o[r] as usize,
        None => r,
    };
    (child, at)
}

fn list_of(c: &Reader, from: usize, to: usize) -> Res<P<'_>> {
    let mut v = Vec::with_capacity(to - from);
    for i in from..to {
        v.push(c.get(i)?);
    }
    Ok(P::List(v))
}

fn dec_of(c: i128, scale: i32) -> P<'static> {
    let mut b = itoa::Buffer::new();
    P::Dec(Dec { neg: c < 0, coef: Cow::Owned(b.format(c.unsigned_abs()).to_string()), exp: -scale })
}
