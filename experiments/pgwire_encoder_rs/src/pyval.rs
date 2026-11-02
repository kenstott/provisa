// A cell's value as provisa's column_values hands it to the encoders (to_pylist, with the IR's
// intervals and infinities), held in Rust: no Python object is built. A column whose type the
// direct Arrow writers in lib.rs do not take, or whose rows ProvisaQueryResult coerces, is encoded
// from these. str() and repr() are Python's, for the converters that write str(value).

use arrow::array::*;
use arrow::datatypes::*;

use crate::fmt_num::{decimal_str, float_repr};
use crate::temporal::{self, civil, TzSpec, DAY_US};
use crate::unicode_tables::NOT_PRINTABLE;
use crate::{int_ascii, is_py_space, typed, uuid_text, EncErr, R};

/// datetime.timedelta's limit, in days either way.
const TIMEDELTA_MAX_DAYS: i128 = 999_999_999;

/// A Decimal: its sign, coefficient digits and exponent, or one of its special values.
#[derive(Clone, Debug, PartialEq)]
pub struct Dec {
    pub neg: bool,
    pub digits: Vec<u8>,
    pub exp: i32,
    pub special: DecSpecial,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DecSpecial {
    Finite,
    NaN,
    Infinity,
}

impl Dec {
    pub fn is_zero(&self) -> bool {
        self.special == DecSpecial::Finite && self.digits.iter().all(|&d| d == b'0')
    }
}

/// The tzinfo of an aware datetime, by what its repr says.
#[derive(Clone, Debug, PartialEq)]
pub enum TzRepr {
    Naive,
    /// pytz.utc (pyarrow's tzinfo for "UTC" and "+00:00")
    PytzUtc,
    /// pytz.FixedOffset(minutes)
    PytzFixed(i32),
    /// zoneinfo.ZoneInfo(key=...): a named zone, as the differential reads it (RESULTS.md)
    ZoneInfo(String),
    /// datetime.timezone.utc: an infinite timestamptz (ir_types.infinite_timestamp)
    PyUtc,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DateTime {
    /// Wall-clock microseconds since 1970-01-01 (the local time of an aware value).
    pub local: i128,
    /// Seconds east of UTC of an aware value.
    pub offset: Option<i32>,
    pub tz: TzRepr,
    /// PostgreSQL's infinity (1) or -infinity (-1): ir_types._InfiniteDatetime.
    pub inf: i32,
    /// Some for a pandas Timestamp (a nanosecond column): its nanoseconds past the microsecond.
    pub ns: Option<u16>,
}

impl DateTime {
    pub fn utc(&self) -> i128 {
        self.local - self.offset.unwrap_or(0) as i128 * 1_000_000
    }
}

/// An IR interval (ir_types.interval_value) that is not a timedelta: a relativedelta, its fields
/// normalized as relativedelta._fix keeps them, or an _ExactInterval carrying its own parts.
#[derive(Clone, Debug, PartialEq)]
pub struct RelDelta {
    pub years: i64,
    pub months: i64,
    pub days: i64,
    pub hours: i64,
    pub minutes: i64,
    pub seconds: i64,
    pub microseconds: i64,
    /// _ExactInterval's pg_parts, None for a plain relativedelta.
    pub exact: Option<(i64, i64, i64)>,
}

impl RelDelta {
    /// relativedelta(months=, days=, microseconds=) after _fix.
    fn of(months: i128, days: i128, micros: i128) -> RelDelta {
        fn carry(v: &mut i128, next: &mut i128, base: i128, limit: i128) {
            if v.abs() > limit {
                let s = v.signum();
                let (div, m) = ((*v * s).div_euclid(base), (*v * s).rem_euclid(base));
                *v = m * s;
                *next += div * s;
            }
        }
        let (mut us, mut secs, mut mins, mut hours, mut d) = (micros, 0i128, 0i128, 0i128, days);
        let (mut mo, mut y) = (months, 0i128);
        carry(&mut us, &mut secs, 1_000_000, 999_999);
        carry(&mut secs, &mut mins, 60, 59);
        carry(&mut mins, &mut hours, 60, 59);
        carry(&mut hours, &mut d, 24, 23);
        carry(&mut mo, &mut y, 12, 11);
        RelDelta {
            years: y as i64,
            months: mo as i64,
            days: d as i64,
            hours: hours as i64,
            minutes: mins as i64,
            seconds: secs as i64,
            microseconds: us as i64,
            exact: None,
        }
    }

    /// ir_types.interval_parts of a relativedelta.
    pub fn parts(&self) -> (i64, i64, i64) {
        if let Some(p) = self.exact {
            return p;
        }
        let secs = (self.hours * 60 + self.minutes) * 60 + self.seconds;
        (self.years * 12 + self.months, self.days, secs * 1_000_000 + self.microseconds)
    }
}

/// An IPv4Interface or IPv6Interface (the value _duckdb_inet makes of DuckDB's INET).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Inet {
    pub v6: bool,
    pub addr: u128,
    pub bits: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PyVal {
    None,
    Bool(bool),
    Int(i128),
    Float(f64),
    Dec(Dec),
    Str(String),
    Bytes(Vec<u8>),
    /// A date, days since 1970-01-01; `inf` marks ir_types._InfiniteDate.
    Date {
        days: i64,
        inf: i32,
    },
    DateTime(DateTime),
    /// datetime.time: microseconds since midnight.
    Time(i64),
    /// datetime.timedelta: microseconds.
    TimeDelta(i128),
    /// pandas.Timedelta: nanoseconds, None for NaT.
    PdTimedelta(Option<i128>),
    /// pandas.NaT of a timestamp column.
    NaT,
    Interval(RelDelta),
    List(Vec<PyVal>),
    Tuple(Vec<PyVal>),
    /// A dict, its keys in insertion order.
    Dict(Vec<(PyVal, PyVal)>),
    Uuid([u8; 16]),
    Inet(Inet),
}

impl PyVal {
    pub fn type_name(&self) -> &'static str {
        match self {
            PyVal::None => "NoneType",
            PyVal::Bool(_) => "bool",
            PyVal::Int(_) => "int",
            PyVal::Float(_) => "float",
            PyVal::Dec(_) => "decimal.Decimal",
            PyVal::Str(_) => "str",
            PyVal::Bytes(_) => "bytes",
            PyVal::Date { .. } => "datetime.date",
            PyVal::DateTime(d) if d.ns.is_some() => "Timestamp",
            PyVal::DateTime(_) => "datetime.datetime",
            PyVal::Time(_) => "datetime.time",
            PyVal::TimeDelta(_) => "datetime.timedelta",
            PyVal::PdTimedelta(_) => "Timedelta",
            PyVal::NaT => "NaTType",
            PyVal::Interval(_) => "relativedelta",
            PyVal::List(_) => "list",
            PyVal::Tuple(_) => "tuple",
            PyVal::Dict(_) => "dict",
            PyVal::Uuid(_) => "UUID",
            PyVal::Inet(_) => "IPv4Interface",
        }
    }
}

pub fn type_error(what: &str, v: &PyVal) -> EncErr {
    EncErr::Type(format!("{what}: '{}'", v.type_name()))
}

// --- from Arrow ---

/// Reads one Arrow column's values as column_values hands them on.
pub struct Reader {
    arr: ArrayRef,
    uuid_ext: bool,
    tz: Option<(TzSpec, TzRepr)>,
}

fn tz_repr(name: &str, spec: &TzSpec) -> TzRepr {
    match spec {
        TzSpec::Fixed(0) => TzRepr::PytzUtc,
        TzSpec::Fixed(off) => TzRepr::PytzFixed(off / 60),
        TzSpec::Named(_) => TzRepr::ZoneInfo(name.to_string()),
        TzSpec::Naive => TzRepr::Naive,
    }
}

impl Reader {
    pub fn new(arr: &ArrayRef, uuid_ext: bool) -> R<Reader> {
        let arr = match arr.data_type() {
            DataType::Dictionary(_, value_type) => {
                arrow::compute::cast(arr, value_type).map_err(|e| EncErr::Value(e.to_string()))?
            }
            _ => arr.clone(),
        };
        let tz = match arr.data_type() {
            DataType::Timestamp(_, Some(name)) => {
                let spec = TzSpec::parse(name)?;
                let repr = tz_repr(name, &spec);
                Some((spec, repr))
            }
            _ => None,
        };
        Ok(Reader { arr, uuid_ext, tz })
    }

    pub fn is_null(&self, i: usize) -> bool {
        self.arr.is_null(i)
    }

    pub fn get(&self, i: usize) -> R<PyVal> {
        value_at(&self.arr, i, self.uuid_ext, self.tz.as_ref(), true)
    }
}

fn dec_of(text: &str, scale: i8) -> PyVal {
    let neg = text.starts_with('-');
    let digits = text.trim_start_matches('-').as_bytes().to_vec();
    PyVal::Dec(Dec {
        neg,
        digits,
        exp: -(scale as i32),
        special: DecSpecial::Finite,
    })
}

/// A timestamp column's value: `top` is a column (not a list element), the level column_values
/// marks infinities at.
fn timestamp_at(raw: i64, unit: TimeUnit, tz: Option<&(TzSpec, TzRepr)>, top: bool) -> R<PyVal> {
    if top && unit == TimeUnit::Microsecond {
        let inf = temporal::timestamp_infinity(raw);
        if inf != 0 {
            // infinite_timestamp: datetime.max or min, in datetime.timezone.utc when aware
            let edge = if inf > 0 { 2_932_897 * DAY_US - 1 } else { -719_162 * DAY_US };
            return Ok(PyVal::DateTime(DateTime {
                local: edge,
                offset: tz.map(|_| 0),
                tz: if tz.is_some() { TzRepr::PyUtc } else { TzRepr::Naive },
                inf,
                ns: None,
            }));
        }
    }
    if unit == TimeUnit::Nanosecond && raw == i64::MIN {
        return Ok(PyVal::NaT);
    }
    let micros = temporal::micros_of(raw, unit)?;
    let spec = tz.map(|t| t.0).unwrap_or(TzSpec::Naive);
    if unit == TimeUnit::Nanosecond {
        temporal::check_ns_local(raw, &spec)?;
    }
    temporal::check_timestamp(micros, &spec)?;
    let offset = spec.offset_at(micros);
    Ok(PyVal::DateTime(DateTime {
        local: micros + offset.unwrap_or(0) as i128 * 1_000_000,
        offset,
        tz: tz.map(|t| t.1.clone()).unwrap_or(TzRepr::Naive),
        inf: 0,
        ns: (unit == TimeUnit::Nanosecond).then(|| raw.rem_euclid(1000) as u16),
    }))
}

/// ir_types.interval_value(months, days, microseconds)
pub fn interval_value(months: i128, days: i128, micros: i128) -> PyVal {
    if months == 0 && days.abs() <= TIMEDELTA_MAX_DAYS {
        let total = days * DAY_US + micros;
        let said = if total < 0 {
            (-((-total) / DAY_US), -((-total) % DAY_US))
        } else {
            (total / DAY_US, total % DAY_US)
        };
        if said == (days, micros) && total.div_euclid(DAY_US).abs() <= TIMEDELTA_MAX_DAYS {
            return PyVal::TimeDelta(total);
        }
    }
    let mut rd = RelDelta::of(months, days, micros);
    let parts = (months as i64, days as i64, micros as i64);
    if rd.parts() != parts {
        rd.exact = Some(parts);
    }
    PyVal::Interval(rd)
}

fn value_at(arr: &ArrayRef, i: usize, uuid_ext: bool, tz: Option<&(TzSpec, TzRepr)>, top: bool) -> R<PyVal> {
    if arr.is_null(i) {
        return Ok(PyVal::None);
    }
    macro_rules! int {
        ($t:ty) => {
            PyVal::Int(typed::<PrimitiveArray<$t>>(arr).value(i) as i128)
        };
    }
    Ok(match arr.data_type() {
        DataType::Null => PyVal::None,
        DataType::Boolean => PyVal::Bool(typed::<BooleanArray>(arr).value(i)),
        DataType::Int8 => int!(Int8Type),
        DataType::Int16 => int!(Int16Type),
        DataType::Int32 => int!(Int32Type),
        DataType::Int64 => int!(Int64Type),
        DataType::UInt8 => int!(UInt8Type),
        DataType::UInt16 => int!(UInt16Type),
        DataType::UInt32 => int!(UInt32Type),
        DataType::UInt64 => int!(UInt64Type),
        DataType::Float16 => PyVal::Float(typed::<Float16Array>(arr).value(i).to_f64()),
        DataType::Float32 => PyVal::Float(typed::<Float32Array>(arr).value(i) as f64),
        DataType::Float64 => PyVal::Float(typed::<Float64Array>(arr).value(i)),
        DataType::Decimal128(_, s) => dec_of(&typed::<Decimal128Array>(arr).value(i).to_string(), *s),
        DataType::Decimal256(_, s) => dec_of(&typed::<Decimal256Array>(arr).value(i).to_string(), *s),
        DataType::Utf8 => PyVal::Str(typed::<StringArray>(arr).value(i).to_string()),
        DataType::LargeUtf8 => PyVal::Str(typed::<LargeStringArray>(arr).value(i).to_string()),
        DataType::Utf8View => PyVal::Str(typed::<StringViewArray>(arr).value(i).to_string()),
        DataType::Binary => PyVal::Bytes(typed::<BinaryArray>(arr).value(i).to_vec()),
        DataType::LargeBinary => PyVal::Bytes(typed::<LargeBinaryArray>(arr).value(i).to_vec()),
        DataType::BinaryView => PyVal::Bytes(typed::<BinaryViewArray>(arr).value(i).to_vec()),
        DataType::FixedSizeBinary(16) if uuid_ext => {
            PyVal::Uuid(typed::<FixedSizeBinaryArray>(arr).value(i).try_into().expect("16 bytes"))
        }
        DataType::FixedSizeBinary(_) => PyVal::Bytes(typed::<FixedSizeBinaryArray>(arr).value(i).to_vec()),
        DataType::Date32 => {
            let v = typed::<Date32Array>(arr).value(i);
            let inf = if top { temporal::date_infinity(v) } else { 0 };
            match inf {
                0 => {
                    temporal::check_date(v as i64)?;
                    PyVal::Date { days: v as i64, inf: 0 }
                }
                1 => PyVal::Date { days: 2_932_896, inf },
                _ => PyVal::Date { days: -719_162, inf },
            }
        }
        DataType::Date64 => {
            let days = typed::<Date64Array>(arr).value(i).div_euclid(86_400_000);
            temporal::check_date(days)?;
            PyVal::Date { days, inf: 0 }
        }
        DataType::Timestamp(unit, _) => {
            let raw = typed::<Int64Array>(&arrow::compute::cast(arr, &DataType::Int64).expect("timestamp storage"))
                .value(i);
            timestamp_at(raw, *unit, tz, top)?
        }
        DataType::Time32(unit) => {
            let raw = typed::<Int32Array>(&arrow::compute::cast(arr, &DataType::Int32).expect("time storage")).value(i)
                as i64;
            time_of(raw, *unit)?
        }
        DataType::Time64(unit) => {
            let raw = typed::<Int64Array>(&arrow::compute::cast(arr, &DataType::Int64).expect("time storage")).value(i);
            time_of(raw, *unit)?
        }
        DataType::Duration(unit) => {
            let raw = typed::<Int64Array>(&arrow::compute::cast(arr, &DataType::Int64).expect("duration storage"))
                .value(i);
            if *unit == TimeUnit::Nanosecond {
                PyVal::PdTimedelta((raw != i64::MIN).then_some(raw as i128))
            } else {
                let us = temporal::micros_of(raw, *unit)?;
                if us.div_euclid(DAY_US).abs() > TIMEDELTA_MAX_DAYS {
                    return Err(EncErr::Overflow("days is out of range".into()));
                }
                PyVal::TimeDelta(us)
            }
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let v = typed::<IntervalMonthDayNanoArray>(arr).value(i);
            if top {
                interval_value(v.months as i128, v.days as i128, v.nanoseconds.div_euclid(1000) as i128)
            } else {
                return Err(EncErr::Type("a list of month-day-nano intervals is not a row value".into()));
            }
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => {
            let (values, s, e) = list_range(arr, i);
            let mut items = Vec::with_capacity(e - s);
            for j in s..e {
                items.push(value_at(&values, j, false, nested_tz(&values)?.as_ref(), false)?);
            }
            PyVal::List(items)
        }
        DataType::Struct(fields) => {
            let a = typed::<StructArray>(arr);
            let mut items: Vec<(PyVal, PyVal)> = Vec::with_capacity(fields.len());
            for (k, f) in fields.iter().enumerate() {
                let col = a.column(k).clone();
                let v = value_at(&col, i, is_uuid_field(f), nested_tz(&col)?.as_ref(), false)?;
                let key = PyVal::Str(f.name().clone());
                // to_pylist folds a repeated field name into one key, the last value kept
                match items.iter_mut().find(|(n, _)| *n == key) {
                    Some(slot) => slot.1 = v,
                    None => items.push((key, v)),
                }
            }
            PyVal::Dict(items)
        }
        DataType::Map(..) => {
            let a = typed::<MapArray>(arr);
            let o = a.value_offsets();
            let (keys, values) = (a.keys().clone(), a.values().clone());
            let mut items = Vec::new();
            for j in o[i] as usize..o[i + 1] as usize {
                let k = value_at(&keys, j, false, nested_tz(&keys)?.as_ref(), false)?;
                let v = value_at(&values, j, false, nested_tz(&values)?.as_ref(), false)?;
                items.push(PyVal::Tuple(vec![k, v]));
            }
            PyVal::List(items)
        }
        DataType::Dictionary(_, value_type) => {
            let decoded = arrow::compute::cast(arr, value_type).map_err(|e| EncErr::Value(e.to_string()))?;
            return value_at(&decoded, i, uuid_ext, tz, top);
        }
        other => return Err(EncErr::Type(format!("no row value for Arrow type {other}"))),
    })
}

fn nested_tz(arr: &ArrayRef) -> R<Option<(TzSpec, TzRepr)>> {
    Ok(match arr.data_type() {
        DataType::Timestamp(_, Some(name)) => {
            let spec = TzSpec::parse(name)?;
            Some((spec, tz_repr(name, &spec)))
        }
        _ => None,
    })
}

pub fn is_uuid_field(f: &Field) -> bool {
    f.metadata().get("ARROW:extension:name").map(String::as_str) == Some("arrow.uuid")
}

fn list_range(arr: &ArrayRef, i: usize) -> (ArrayRef, usize, usize) {
    match arr.data_type() {
        DataType::List(_) => {
            let a = typed::<ListArray>(arr);
            let o = a.value_offsets();
            (a.values().clone(), o[i] as usize, o[i + 1] as usize)
        }
        DataType::LargeList(_) => {
            let a = typed::<LargeListArray>(arr);
            let o = a.value_offsets();
            (a.values().clone(), o[i] as usize, o[i + 1] as usize)
        }
        DataType::FixedSizeList(_, n) => {
            let a = typed::<FixedSizeListArray>(arr);
            let n = *n as usize;
            (a.values().clone(), i * n, i * n + n)
        }
        _ => unreachable!("a list type"),
    }
}

fn time_of(raw: i64, unit: TimeUnit) -> R<PyVal> {
    let us = match unit {
        // pandas.Timestamp(v, unit="ns").time(): the time of day of that instant
        TimeUnit::Nanosecond => temporal::micros_of(raw, unit)?.rem_euclid(DAY_US) as i64,
        _ => temporal::micros_of(raw, unit)? as i64,
    };
    if !(0..86_400_000_000).contains(&us) {
        return Err(EncErr::Value("hour must be in 0..23".into()));
    }
    Ok(PyVal::Time(us))
}

// --- str() and repr() ---

fn push_num<T: itoa::Integer>(out: &mut String, v: T) {
    out.push_str(itoa::Buffer::new().format(v));
}

fn push_pad(out: &mut String, v: i64, width: usize) {
    let s = itoa::Buffer::new().format(v.unsigned_abs()).to_string();
    if v < 0 {
        out.push('-');
    }
    for _ in s.len()..width {
        out.push('0');
    }
    out.push_str(&s);
}

fn push_float(out: &mut String, x: f64) {
    if x.is_nan() {
        out.push_str("nan");
    } else if x.is_infinite() {
        out.push_str(if x > 0.0 { "inf" } else { "-inf" });
    } else {
        let mut b = Vec::new();
        float_repr(x, &mut b);
        out.push_str(std::str::from_utf8(&b).expect("ascii"));
    }
}

/// str(Decimal)
pub fn dec_str(d: &Dec, out: &mut String) {
    match d.special {
        DecSpecial::NaN => out.push_str(if d.neg { "-NaN" } else { "NaN" }),
        DecSpecial::Infinity => out.push_str(if d.neg { "-Infinity" } else { "Infinity" }),
        DecSpecial::Finite => {
            let mut b = Vec::new();
            decimal_str(d.neg, &d.digits, d.exp, &mut b);
            out.push_str(std::str::from_utf8(&b).expect("ascii"));
        }
    }
}

fn ymd(days: i64) -> (i64, u32, u32) {
    civil(days)
}

fn push_date(out: &mut String, days: i64) {
    let (y, m, d) = ymd(days);
    push_pad(out, y, 4);
    out.push('-');
    push_pad(out, m as i64, 2);
    out.push('-');
    push_pad(out, d as i64, 2);
}

/// time.isoformat(): HH:MM:SS, then .ffffff when there are microseconds.
fn push_clock(out: &mut String, us: i64, ns: Option<u16>) {
    let secs = us / 1_000_000;
    push_pad(out, secs / 3600, 2);
    out.push(':');
    push_pad(out, secs / 60 % 60, 2);
    out.push(':');
    push_pad(out, secs % 60, 2);
    let frac = us % 1_000_000;
    match ns {
        Some(n) if n != 0 => {
            // pandas Timestamp.isoformat: the nanoseconds after the microseconds
            out.push('.');
            push_pad(out, frac, 6);
            push_pad(out, n as i64, 3);
        }
        _ if frac != 0 => {
            out.push('.');
            push_pad(out, frac, 6);
        }
        _ => {}
    }
}

/// The utcoffset as datetime.isoformat writes it: +HH:MM, then :SS when there are seconds.
fn push_iso_offset(out: &mut String, seconds: i32) {
    out.push(if seconds < 0 { '-' } else { '+' });
    let a = seconds.unsigned_abs() as i64;
    push_pad(out, a / 3600, 2);
    out.push(':');
    push_pad(out, a / 60 % 60, 2);
    if a % 60 != 0 {
        out.push(':');
        push_pad(out, a % 60, 2);
    }
}

/// datetime.isoformat(sep): pandas' for a nanosecond value.
pub fn datetime_iso(d: &DateTime, sep: char, out: &mut String) {
    push_date(out, d.local.div_euclid(DAY_US) as i64);
    out.push(sep);
    push_clock(out, d.local.rem_euclid(DAY_US) as i64, d.ns);
    if let Some(off) = d.offset {
        push_iso_offset(out, off);
    }
}

/// str(timedelta)
fn timedelta_str(us: i128, out: &mut String) {
    let days = us.div_euclid(DAY_US) as i64;
    let rest = us.rem_euclid(DAY_US) as i64;
    if days != 0 {
        push_num(out, days);
        out.push_str(if days.abs() != 1 { " days, " } else { " day, " });
    }
    let secs = rest / 1_000_000;
    push_num(out, secs / 3600);
    out.push(':');
    push_pad(out, secs / 60 % 60, 2);
    out.push(':');
    push_pad(out, secs % 60, 2);
    if rest % 1_000_000 != 0 {
        out.push('.');
        push_pad(out, rest % 1_000_000, 6);
    }
}

/// str(pandas.Timedelta): Timedelta._repr_base(format="long").
fn pd_timedelta_str(ns: i128, out: &mut String) {
    let day_ns = DAY_US * 1000;
    let days = ns.div_euclid(day_ns) as i64;
    let rest = ns.rem_euclid(day_ns) as i64;
    push_num(out, days);
    out.push_str(" days");
    out.push_str(if days < 0 { " +" } else { " " });
    let secs = rest / 1_000_000_000;
    push_pad(out, secs / 3600, 2);
    out.push(':');
    push_pad(out, secs / 60 % 60, 2);
    out.push(':');
    push_pad(out, secs % 60, 2);
    let frac = rest % 1_000_000_000;
    if frac != 0 {
        out.push('.');
        if frac % 1000 != 0 {
            push_pad(out, frac, 9);
        } else {
            push_pad(out, frac / 1000, 6);
        }
    }
}

/// format(n, "+g") of an int: six significant digits, correctly rounded.
fn plus_g(n: i64, out: &mut String) {
    out.push(if n < 0 { '-' } else { '+' });
    let digits = n.unsigned_abs().to_string();
    let exp = digits.len() as i32 - 1;
    if digits.len() <= 6 {
        out.push_str(&digits);
        return;
    }
    // round to six digits, half to even on the exact value
    let mut kept: Vec<u8> = digits.as_bytes()[..6].to_vec();
    let first = digits.as_bytes()[6];
    let rest_zero = digits.as_bytes()[7..].iter().all(|&c| c == b'0');
    let up = first > b'5' || (first == b'5' && (!rest_zero || (kept[5] - b'0') % 2 == 1));
    let mut exp = exp;
    if up {
        let mut k = 6;
        loop {
            if k == 0 {
                kept.insert(0, b'1');
                kept.pop();
                exp += 1;
                break;
            }
            k -= 1;
            if kept[k] == b'9' {
                kept[k] = b'0';
            } else {
                kept[k] += 1;
                break;
            }
        }
    }
    while kept.len() > 1 && kept[kept.len() - 1] == b'0' {
        kept.pop();
    }
    if exp < 6 {
        // fixed: the rounded value's digits then zeros up to the point
        let s = std::str::from_utf8(&kept).unwrap();
        out.push_str(s);
        for _ in kept.len() as i32..exp + 1 {
            out.push('0');
        }
        return;
    }
    out.push(kept[0] as char);
    if kept.len() > 1 {
        out.push('.');
        out.push_str(std::str::from_utf8(&kept[1..]).unwrap());
    }
    out.push_str("e+");
    push_pad(out, exp as i64, 2);
}

/// relativedelta.__repr__ (an _ExactInterval names its own class)
fn reldelta_repr(r: &RelDelta, out: &mut String) {
    out.push_str(if r.exact.is_some() { "_ExactInterval(" } else { "relativedelta(" });
    let mut first = true;
    for (name, v) in [
        ("years", r.years),
        ("months", r.months),
        ("days", r.days),
        ("hours", r.hours),
        ("minutes", r.minutes),
        ("seconds", r.seconds),
        ("microseconds", r.microseconds),
    ] {
        if v != 0 {
            if !first {
                out.push_str(", ");
            }
            out.push_str(name);
            out.push('=');
            plus_g(v, out);
            first = false;
        }
    }
    out.push(')');
}

pub fn is_printable(c: char) -> bool {
    let u = c as u32;
    if u < 0x80 {
        return (0x20..0x7f).contains(&u);
    }
    NOT_PRINTABLE
        .binary_search_by(|&(lo, hi)| {
            if hi < u {
                std::cmp::Ordering::Less
            } else if lo > u {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .is_err()
}

/// repr(str)
pub fn str_repr(s: &str, out: &mut String) {
    let quote = if s.contains('\'') && !s.contains('"') { '"' } else { '\'' };
    out.push(quote);
    for c in s.chars() {
        match c {
            _ if c == quote || c == '\\' => {
                out.push('\\');
                out.push(c);
            }
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ if (c as u32) < 0x20 || c as u32 == 0x7f => out.push_str(&format!("\\x{:02x}", c as u32)),
            _ if c.is_ascii() || is_printable(c) => out.push(c),
            _ if (c as u32) <= 0xff => out.push_str(&format!("\\x{:02x}", c as u32)),
            _ if (c as u32) <= 0xffff => out.push_str(&format!("\\u{:04x}", c as u32)),
            _ => out.push_str(&format!("\\U{:08x}", c as u32)),
        }
    }
    out.push(quote);
}

/// repr(bytes)
pub fn bytes_repr(b: &[u8], out: &mut String) {
    let quote = if b.contains(&b'\'') && !b.contains(&b'"') { b'"' } else { b'\'' };
    out.push('b');
    out.push(quote as char);
    for &c in b {
        match c {
            _ if c == quote || c == b'\\' => {
                out.push('\\');
                out.push(c as char);
            }
            b'\t' => out.push_str("\\t"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            0x20..=0x7e => out.push(c as char),
            _ => out.push_str(&format!("\\x{c:02x}")),
        }
    }
    out.push(quote as char);
}

fn uuid_str(b: &[u8; 16], out: &mut String) {
    let mut v = Vec::with_capacity(36);
    uuid_text(b, &mut v);
    out.push_str(std::str::from_utf8(&v).expect("ascii"));
}

/// The text of an address as PostgreSQL's inet_net_ntop writes it (`pg`: an IPv6 address holding
/// an IPv4 one, mapped or compatible, ends in dotted quad), or as Python 3.12's ipaddress writes
/// it (all hextets).
pub fn inet_addr_text(v: &Inet, pg: bool, out: &mut String) {
    if !v.v6 {
        let a = v.addr as u32;
        out.push_str(&format!("{}.{}.{}.{}", a >> 24, (a >> 16) & 255, (a >> 8) & 255, a & 255));
        return;
    }
    let words: Vec<u16> = (0..8).map(|k| (v.addr >> (112 - 16 * k)) as u16).collect();
    // the longest run of two or more zero words, the first of equal runs
    let (mut best, mut cur): ((i32, i32), (i32, i32)) = ((-1, 0), (-1, 0));
    for (k, &w) in words.iter().enumerate() {
        if w == 0 {
            if cur.0 == -1 {
                cur = (k as i32, 1);
            } else {
                cur.1 += 1;
            }
        } else if cur.0 != -1 {
            if best.0 == -1 || cur.1 > best.1 {
                best = cur;
            }
            cur = (-1, 0);
        }
    }
    if cur.0 != -1 && (best.0 == -1 || cur.1 > best.1) {
        best = cur;
    }
    if best.0 != -1 && best.1 < 2 {
        best = (-1, 0);
    }
    let mut k = 0i32;
    while k < 8 {
        if best.0 != -1 && k >= best.0 && k < best.0 + best.1 {
            if k == best.0 {
                out.push(':');
            }
            k += 1;
            continue;
        }
        if k != 0 {
            out.push(':');
        }
        // an encapsulated IPv4 address
        if pg && k == 6 && best.0 == 0 && (best.1 == 6 || (best.1 == 5 && words[5] == 0xffff)) {
            let a = v.addr as u32;
            out.push_str(&format!("{}.{}.{}.{}", a >> 24, (a >> 16) & 255, (a >> 8) & 255, a & 255));
            return;
        }
        out.push_str(&format!("{:x}", words[k as usize]));
        k += 1;
    }
    if best.0 != -1 && best.0 + best.1 == 8 {
        out.push(':');
    }
}

/// str(value), Python's.
pub fn py_str(v: &PyVal, out: &mut String) {
    match v {
        PyVal::Str(s) => out.push_str(s),
        PyVal::Dec(d) => dec_str(d, out),
        PyVal::Date { days, .. } => push_date(out, *days),
        PyVal::DateTime(d) => datetime_iso(d, ' ', out),
        PyVal::Time(us) => push_clock(out, *us, None),
        PyVal::TimeDelta(us) => timedelta_str(*us, out),
        PyVal::PdTimedelta(Some(ns)) => pd_timedelta_str(*ns, out),
        PyVal::PdTimedelta(None) | PyVal::NaT => out.push_str("NaT"),
        PyVal::Uuid(b) => uuid_str(b, out),
        PyVal::Inet(a) => {
            // str of an interface: the address and its prefix
            inet_addr_text(a, false, out);
            out.push('/');
            push_num(out, a.bits);
        }
        _ => py_repr(v, out),
    }
}

pub fn str_of(v: &PyVal) -> String {
    match v {
        PyVal::Str(s) => s.clone(),
        _ => {
            let mut s = String::new();
            py_str(v, &mut s);
            s
        }
    }
}

fn datetime_args(d: &DateTime, out: &mut String) {
    let days = d.local.div_euclid(DAY_US) as i64;
    let tod = d.local.rem_euclid(DAY_US) as i64;
    let (y, m, dd) = ymd(days);
    let secs = tod / 1_000_000;
    let us = tod % 1_000_000;
    out.push_str(&format!("{y}, {m}, {dd}, {}, {}", secs / 3600, secs / 60 % 60));
    if us != 0 {
        out.push_str(&format!(", {}, {us}", secs % 60));
    } else if secs % 60 != 0 {
        out.push_str(&format!(", {}", secs % 60));
    }
}

fn tz_repr_text(tz: &TzRepr) -> String {
    match tz {
        TzRepr::Naive => String::new(),
        TzRepr::PytzUtc => "<UTC>".into(),
        TzRepr::PytzFixed(m) => format!("pytz.FixedOffset({m})"),
        TzRepr::ZoneInfo(key) => format!("zoneinfo.ZoneInfo(key='{key}')"),
        TzRepr::PyUtc => "datetime.timezone.utc".into(),
    }
}

/// repr(value), Python's.
pub fn py_repr(v: &PyVal, out: &mut String) {
    match v {
        PyVal::None => out.push_str("None"),
        PyVal::Bool(b) => out.push_str(if *b { "True" } else { "False" }),
        PyVal::Int(n) => push_num(out, *n),
        PyVal::Float(x) => push_float(out, *x),
        PyVal::Dec(d) => {
            out.push_str("Decimal('");
            dec_str(d, out);
            out.push_str("')");
        }
        PyVal::Str(s) => str_repr(s, out),
        PyVal::Bytes(b) => bytes_repr(b, out),
        PyVal::Date { days, inf } => {
            let (y, m, d) = ymd(*days);
            let name = if *inf != 0 { "_InfiniteDate" } else { "datetime.date" };
            out.push_str(&format!("{name}({y}, {m}, {d})"));
        }
        PyVal::DateTime(d) if d.ns.is_some() => {
            // Timestamp('...'), its zone after: pandas' repr
            out.push_str("Timestamp('");
            datetime_iso(&DateTime { offset: None, ..d.clone() }, ' ', out);
            if let Some(off) = d.offset {
                out.push(if off < 0 { '-' } else { '+' });
                push_pad(out, (off.abs() / 3600) as i64, 2);
                push_pad(out, (off.abs() / 60 % 60) as i64, 2);
                let tz = match &d.tz {
                    TzRepr::PytzUtc | TzRepr::PyUtc => "UTC".to_string(),
                    TzRepr::ZoneInfo(key) => key.clone(),
                    other => tz_repr_text(other),
                };
                out.push_str(&format!("', tz='{tz}')"));
            } else {
                out.push_str("')");
            }
        }
        PyVal::DateTime(d) => {
            out.push_str(if d.inf != 0 { "_InfiniteDatetime(" } else { "datetime.datetime(" });
            datetime_args(d, out);
            if d.tz != TzRepr::Naive {
                out.push_str(", tzinfo=");
                out.push_str(&tz_repr_text(&d.tz));
            }
            out.push(')');
        }
        PyVal::Time(us) => {
            let secs = us / 1_000_000;
            let frac = us % 1_000_000;
            out.push_str(&format!("datetime.time({}, {}", secs / 3600, secs / 60 % 60));
            if frac != 0 {
                out.push_str(&format!(", {}, {frac}", secs % 60));
            } else if secs % 60 != 0 {
                out.push_str(&format!(", {}", secs % 60));
            }
            out.push(')');
        }
        PyVal::TimeDelta(us) => {
            let days = us.div_euclid(DAY_US);
            let rest = us.rem_euclid(DAY_US);
            let (secs, micros) = (rest / 1_000_000, rest % 1_000_000);
            let mut parts = Vec::new();
            if days != 0 {
                parts.push(format!("days={days}"));
            }
            if secs != 0 {
                parts.push(format!("seconds={secs}"));
            }
            if micros != 0 {
                parts.push(format!("microseconds={micros}"));
            }
            if parts.is_empty() {
                parts.push("0".into());
            }
            out.push_str(&format!("datetime.timedelta({})", parts.join(", ")));
        }
        PyVal::PdTimedelta(Some(ns)) => {
            out.push_str("Timedelta('");
            pd_timedelta_str(*ns, out);
            out.push_str("')");
        }
        PyVal::PdTimedelta(None) | PyVal::NaT => out.push_str("NaT"),
        PyVal::Interval(r) => reldelta_repr(r, out),
        PyVal::List(items) => {
            out.push('[');
            for (k, item) in items.iter().enumerate() {
                if k > 0 {
                    out.push_str(", ");
                }
                py_repr(item, out);
            }
            out.push(']');
        }
        PyVal::Tuple(items) => {
            out.push('(');
            for (k, item) in items.iter().enumerate() {
                if k > 0 {
                    out.push_str(", ");
                }
                py_repr(item, out);
            }
            if items.len() == 1 {
                out.push(',');
            }
            out.push(')');
        }
        PyVal::Dict(items) => {
            out.push('{');
            for (k, (key, value)) in items.iter().enumerate() {
                if k > 0 {
                    out.push_str(", ");
                }
                py_repr(key, out);
                out.push_str(": ");
                py_repr(value, out);
            }
            out.push('}');
        }
        PyVal::Uuid(b) => {
            out.push_str("UUID('");
            uuid_str(b, out);
            out.push_str("')");
        }
        PyVal::Inet(a) => {
            out.push_str(if a.v6 { "IPv6Interface('" } else { "IPv4Interface('" });
            py_str(v, out);
            out.push_str("')");
        }
    }
}

// --- Python's int(), float() and Decimal() of a value ---

/// A str as int() and float() read it: Unicode decimal digits and whitespace as their ASCII
/// (_PyUnicode_TransformDecimalAndSpaceToASCII), then stripped of whitespace.
fn ascii_number(s: &str) -> Option<String> {
    let mut t = String::with_capacity(s.len());
    for c in s.chars() {
        let a = int_ascii(c);
        if a == b'?' && c != '?' {
            return None;
        }
        t.push(a as char);
    }
    Some(t.trim_matches(|c: char| c.is_ascii_whitespace()).to_string())
}

/// Digits with single underscores between them, the underscores dropped; None when malformed.
fn without_underscores(s: &str) -> Option<String> {
    if s.starts_with('_') || s.ends_with('_') || s.contains("__") {
        return None;
    }
    Some(s.replace('_', ""))
}

/// int(s) of a str, base 10.
pub fn int_of_str(s: &str) -> R<i128> {
    let bad = || EncErr::Value(format!("invalid literal for int() with base 10: {s:?}"));
    let t = ascii_number(s).ok_or_else(bad)?;
    let (neg, body) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, &t[..]),
    };
    let digits = without_underscores(body).ok_or_else(bad)?;
    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return Err(bad());
    }
    let v: i128 = digits
        .parse()
        .map_err(|_| EncErr::Overflow("int too large for the encoder".into()))?;
    Ok(if neg { -v } else { v })
}

/// float(s) of a str.
pub fn float_of_str(s: &str) -> R<f64> {
    let bad = || EncErr::Value(format!("could not convert string to float: {s:?}"));
    let t = ascii_number(s).ok_or_else(bad)?;
    let (sign, body) = match t.as_bytes().first() {
        Some(b'-') => (-1.0, &t[1..]),
        Some(b'+') => (1.0, &t[1..]),
        _ => (1.0, &t[..]),
    };
    let lower = body.to_ascii_lowercase();
    if lower == "inf" || lower == "infinity" {
        return Ok(sign * f64::INFINITY);
    }
    if lower == "nan" {
        return Ok(f64::NAN.copysign(sign));
    }
    // digits [. digits] [e [sign] digits], underscores only between digits
    let (mant, exp) = match body.find(['e', 'E']) {
        Some(k) => (&body[..k], Some(&body[k + 1..])),
        None => (body, None),
    };
    let (int_part, frac_part) = match mant.find('.') {
        Some(k) => (&mant[..k], Some(&mant[k + 1..])),
        None => (mant, None),
    };
    let clean = |p: &str| -> Option<String> {
        if p.is_empty() {
            return Some(String::new());
        }
        let d = without_underscores(p)?;
        d.bytes().all(|c| c.is_ascii_digit()).then_some(d)
    };
    let ip = clean(int_part).ok_or_else(bad)?;
    let fp = match frac_part {
        Some(f) => clean(f).ok_or_else(bad)?,
        None => String::new(),
    };
    if ip.is_empty() && fp.is_empty() {
        return Err(bad());
    }
    let mut text = format!("{ip}.{fp}");
    if let Some(e) = exp {
        let (es, eb) = match e.as_bytes().first() {
            Some(b'-') => ("-", &e[1..]),
            Some(b'+') => ("", &e[1..]),
            _ => ("", e),
        };
        let ed = clean(eb).ok_or_else(bad)?;
        if ed.is_empty() {
            return Err(bad());
        }
        text = format!("{text}e{es}{ed}");
    }
    let v: f64 = text.parse().map_err(|_| bad())?;
    Ok(sign * v)
}

/// Decimal(s) of a str.
pub fn dec_of_str(s: &str) -> R<Dec> {
    let bad = || EncErr::Value(format!("[<class 'decimal.ConversionSyntax'>] {s:?}"));
    let trimmed: String = s.trim_matches(is_py_space).to_string();
    let mut t = String::with_capacity(trimmed.len());
    for c in trimmed.chars() {
        let a = int_ascii(c);
        if a == b'?' && c != '?' {
            return Err(bad());
        }
        t.push(a as char);
    }
    let (neg, body) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, &t[..]),
    };
    let lower = body.to_ascii_lowercase();
    if lower == "inf" || lower == "infinity" {
        return Ok(Dec { neg, digits: vec![b'0'], exp: 0, special: DecSpecial::Infinity });
    }
    if let Some(payload) = lower.strip_prefix("nan") {
        if payload.bytes().all(|c| c.is_ascii_digit()) {
            return Ok(Dec { neg, digits: vec![b'0'], exp: 0, special: DecSpecial::NaN });
        }
        return Err(bad());
    }
    let (mant, exp) = match body.find(['e', 'E']) {
        Some(k) => (&body[..k], Some(&body[k + 1..])),
        None => (body, None),
    };
    let (ip, fp) = match mant.find('.') {
        Some(k) => (&mant[..k], &mant[k + 1..]),
        None => (mant, ""),
    };
    let clean = |p: &str| -> Option<String> {
        if p.is_empty() {
            return Some(String::new());
        }
        let d = without_underscores(p)?;
        d.bytes().all(|c| c.is_ascii_digit()).then_some(d)
    };
    let ip = clean(ip).ok_or_else(bad)?;
    let fp = clean(fp).ok_or_else(bad)?;
    if ip.is_empty() && fp.is_empty() {
        return Err(bad());
    }
    let mut e: i64 = 0;
    if let Some(x) = exp {
        let (es, eb) = match x.as_bytes().first() {
            Some(b'-') => (-1, &x[1..]),
            Some(b'+') => (1, &x[1..]),
            _ => (1, x),
        };
        let ed = clean(eb).ok_or_else(bad)?;
        if ed.is_empty() {
            return Err(bad());
        }
        e = es * ed.parse::<i64>().map_err(|_| bad())?;
    }
    let mut digits: Vec<u8> = format!("{ip}{fp}").into_bytes();
    let first = digits.iter().position(|&c| c != b'0').unwrap_or(digits.len() - 1);
    digits.drain(..first);
    let exp = i32::try_from(e - fp.len() as i64).map_err(|_| bad())?;
    Ok(Dec { neg, digits, exp, special: DecSpecial::Finite })
}

/// Decimal(repr(x)) of a float: Decimal(str(float)).
pub fn dec_of_float(x: f64) -> Dec {
    let mut s = String::new();
    push_float(&mut s, x);
    dec_of_str(&s).expect("a float's repr is a Decimal")
}

/// float(value)
pub fn to_float(v: &PyVal) -> R<f64> {
    match v {
        PyVal::Float(x) => Ok(*x),
        PyVal::Bool(b) => Ok(*b as i32 as f64),
        PyVal::Int(n) => Ok(*n as f64),
        PyVal::Dec(d) => Ok(match d.special {
            DecSpecial::NaN => f64::NAN.copysign(if d.neg { -1.0 } else { 1.0 }),
            DecSpecial::Infinity => {
                if d.neg {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                }
            }
            DecSpecial::Finite => {
                let text = format!(
                    "{}{}e{}",
                    if d.neg { "-" } else { "" },
                    std::str::from_utf8(&d.digits).unwrap(),
                    d.exp
                );
                text.parse::<f64>().expect("decimal digits")
            }
        }),
        PyVal::Str(s) => float_of_str(s),
        PyVal::Bytes(b) => float_of_str(std::str::from_utf8(b).map_err(|_| type_error("float()", v))?),
        _ => Err(type_error("float() argument must be a string or a real number, not", v)),
    }
}

/// struct.pack("!d" / "!f", value): a float, or what has __float__ or __index__.
pub fn packable_float(v: &PyVal) -> R<f64> {
    match v {
        PyVal::Float(_) | PyVal::Bool(_) | PyVal::Int(_) | PyVal::Dec(_) => to_float(v),
        _ => Err(EncErr::Value("required argument is not a float".into())),
    }
}

/// int(value)
pub fn to_int(v: &PyVal) -> R<i128> {
    match v {
        PyVal::Int(n) => Ok(*n),
        PyVal::Bool(b) => Ok(*b as i128),
        PyVal::Float(x) => {
            if x.is_nan() {
                return Err(EncErr::Value("cannot convert float NaN to integer".into()));
            }
            if x.is_infinite() {
                return Err(EncErr::Overflow("cannot convert float infinity to integer".into()));
            }
            let t = x.trunc();
            if t.abs() >= 1.7e38 {
                return Err(EncErr::Overflow("int too large for the encoder".into()));
            }
            Ok(t as i128)
        }
        PyVal::Dec(d) => match d.special {
            DecSpecial::NaN => Err(EncErr::Value("cannot convert NaN to integer".into())),
            DecSpecial::Infinity => Err(EncErr::Overflow("cannot convert Infinity to integer".into())),
            DecSpecial::Finite => {
                let n = d.digits.len() as i32;
                let keep = n + d.exp;
                if keep <= 0 {
                    return Ok(0);
                }
                let mut s = String::from_utf8(d.digits[..keep.min(n) as usize].to_vec()).unwrap();
                for _ in n..keep {
                    s.push('0');
                }
                let v: i128 = s
                    .parse()
                    .map_err(|_| EncErr::Overflow("int too large for the encoder".into()))?;
                Ok(if d.neg { -v } else { v })
            }
        },
        PyVal::Str(s) => int_of_str(s),
        PyVal::Bytes(b) => int_of_str(std::str::from_utf8(b).map_err(|_| type_error("int()", v))?),
        _ => Err(type_error("int() argument must be a string, a bytes-like object or a real number, not", v)),
    }
}

/// Decimal(value) as server._to_decimal and cast_types._to_decimal make it: a Decimal kept, any
/// other value Decimal(str(value)).
pub fn to_decimal(v: &PyVal) -> R<Dec> {
    match v {
        PyVal::Dec(d) => Ok(d.clone()),
        PyVal::Float(x) => Ok(dec_of_float(*x)),
        _ => dec_of_str(&str_of(v)),
    }
}

/// bool(value)
pub fn truthy(v: &PyVal) -> bool {
    match v {
        PyVal::None => false,
        PyVal::Bool(b) => *b,
        PyVal::Int(n) => *n != 0,
        PyVal::Float(x) => *x != 0.0,
        PyVal::Dec(d) => !d.is_zero(),
        PyVal::Str(s) => !s.is_empty(),
        PyVal::Bytes(b) => !b.is_empty(),
        PyVal::TimeDelta(us) => *us != 0,
        PyVal::PdTimedelta(ns) => ns.is_none_or(|n| n != 0),
        PyVal::Interval(r) => r.parts() != (0, 0, 0) || r.exact.is_some(),
        PyVal::List(items) | PyVal::Tuple(items) => !items.is_empty(),
        PyVal::Dict(items) => !items.is_empty(),
        _ => true,
    }
}

/// The items iterating the value gives: a list's or tuple's elements, a str's characters, a
/// dict's keys, a bytes' ints.
pub fn iter_items(v: &PyVal) -> R<Vec<PyVal>> {
    Ok(match v {
        PyVal::List(items) | PyVal::Tuple(items) => items.clone(),
        PyVal::Str(s) => s.chars().map(|c| PyVal::Str(c.to_string())).collect(),
        PyVal::Dict(items) => items.iter().map(|(k, _)| k.clone()).collect(),
        PyVal::Bytes(b) => b.iter().map(|&c| PyVal::Int(c as i128)).collect(),
        _ => return Err(type_error("object is not iterable", v)),
    })
}

/// The days since 1970-01-01 and the clock, of a date's or datetime's .toordinal() and fields.
pub fn date_days(v: &PyVal) -> Option<(i64, i32)> {
    match v {
        PyVal::Date { days, inf } => Some((*days, *inf)),
        PyVal::DateTime(d) => Some((d.local.div_euclid(DAY_US) as i64, d.inf)),
        _ => None,
    }
}
