//! One converter per Arrow column: a cell's value as `column_values` reads it and `_convert_value`
//! converts it, written as orjson writes it, with the key `_to_hashable` gives it.

use arrow::array::*;
use arrow::datatypes::*;

use crate::fmt::{self, DecVal};
use crate::jtext::{self, J};

/// What writing a cell produced. A leaf's bytes were appended to the caller's buffer.
pub enum Out {
    Null,
    /// JSON bytes of a scalar.
    Leaf,
    /// A value orjson refuses when it is written (the message orjson raises).
    Poison(&'static str),
    /// A dict or list (a parsed JSON text, a struct or list value): Python's own mutable object,
    /// which the serializer may merge into, append to or walk through.
    Json(J),
}

/// Key atoms. Python compares the converted values with `==`: a float NaN is unequal to every
/// other (each is its own object), -0.0 equals 0.0, a Decimal's int equals the same float.
pub const K_NULL: u8 = 0;
pub const K_BYTES: u8 = 1;
pub const K_NUM: u8 = 2;
pub const K_NAN: u8 = 3;
pub const K_CANON: u8 = 4;

pub struct Keyer {
    nan: u64,
}

impl Keyer {
    pub fn new() -> Self {
        Keyer { nan: 0 }
    }
    pub fn bytes(&self, key: &mut Vec<u8>, b: &[u8]) {
        key.push(K_BYTES);
        key.extend_from_slice(&(b.len() as u64).to_le_bytes());
        key.extend_from_slice(b);
    }
    pub fn num(&mut self, key: &mut Vec<u8>, f: f64) {
        if f.is_nan() {
            self.nan += 1;
            key.push(K_NAN);
            key.extend_from_slice(&self.nan.to_le_bytes());
        } else {
            key.push(K_NUM);
            let f = if f == 0.0 { 0.0 } else { f };
            key.extend_from_slice(&f.to_bits().to_le_bytes());
        }
    }
    pub fn canon(&self, key: &mut Vec<u8>, j: &J) {
        key.push(K_CANON);
        let at = key.len();
        key.extend_from_slice(&[0u8; 8]);
        j.canon(key);
        let n = (key.len() - at - 8) as u64;
        key[at..at + 8].copy_from_slice(&n.to_le_bytes());
    }
}

pub type Res<T> = Result<T, String>;

pub trait Cell: Send {
    /// The raw value is None (what `all(row[idx] is None ...)` tests).
    fn is_null(&self, row: usize) -> bool;
    /// Append the converted value's JSON to `buf` and, when asked, its key atom to `key`.
    fn write(&self, row: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out>;
    /// The key atom alone (`scratch` is the caller's spare buffer).
    fn key(&self, row: usize, key: &mut Vec<u8>, k: &mut Keyer, scratch: &mut Vec<u8>) -> Res<()> {
        scratch.clear();
        self.write(row, scratch, Some(key), k)?;
        Ok(())
    }
    /// The errors `column_values` (to_pylist) raises for this column, whichever rows are used.
    fn validate(&self) -> Res<()> {
        Ok(())
    }
}

fn not_impl(what: &str) -> String {
    format!("NotImplemented: {}", what)
}

// ---------------------------------------------------------------- time zones and units

/// The offset in minutes `to_pylist` gives a timestamp of this zone, if the zone is ported (UTC and
/// fixed offsets).
pub fn tz_offset(tz: &str) -> Res<i32> {
    match tz {
        "UTC" | "Etc/UTC" | "utc" => return Ok(0),
        _ => {}
    }
    let b = tz.as_bytes();
    if b.len() == 6 && (b[0] == b'+' || b[0] == b'-') && b[3] == b':' {
        let h: i32 = tz[1..3].parse().map_err(|_| not_impl(&format!("time zone {tz}")))?;
        let m: i32 = tz[4..6].parse().map_err(|_| not_impl(&format!("time zone {tz}")))?;
        let off = h * 60 + m;
        return Ok(if b[0] == b'-' { -off } else { off });
    }
    Err(not_impl(&format!("time zone {tz}")))
}

fn unit_micros(u: &TimeUnit) -> Res<i64> {
    match u {
        TimeUnit::Second => Ok(1_000_000),
        TimeUnit::Millisecond => Ok(1_000),
        TimeUnit::Microsecond => Ok(1),
        TimeUnit::Nanosecond => Err(not_impl("nanosecond unit")),
    }
}

const DATE_INF: i32 = i32::MAX;
const TS_INF: i64 = i64::MAX;
const TD_MAX_DAYS: i128 = 999_999_999;

fn ts_value(raw: i64, mult: i64, offset: Option<i32>, buf: &mut Vec<u8>) -> Res<()> {
    let utc = (raw as i128) * (mult as i128);
    let local = utc + offset.unwrap_or(0) as i128 * 60_000_000;
    if local.abs() > (i64::MAX as i128) {
        return Err("timestamp out of range".into());
    }
    buf.push(b'"');
    fmt::timestamp_text(buf, local as i64, offset)?;
    buf.push(b'"');
    Ok(())
}

/// A duration's (days, micros) as `interval_parts` takes a timedelta apart: the magnitude's, signed.
/// The timedelta itself (floor days, positive seconds) must lie within a billion days.
fn duration_parts(v: i128) -> Res<(i64, i64)> {
    if v.div_euclid(86_400_000_000).abs() > TD_MAX_DAYS {
        return Err("days=…; must have magnitude <= 999999999".into());
    }
    let a = v.abs();
    let days = a / 86_400_000_000;
    let rem = a % 86_400_000_000;
    Ok(if v < 0 { (-(days as i64), -(rem as i64)) } else { (days as i64, rem as i64) })
}

/// `interval_value`'s OverflowError: a month-free interval within a billion days is built as a
/// timedelta, which normalizes its microseconds into days.
fn interval_check(months: i32, days: i32, micros: i64) -> Res<()> {
    if months == 0 && (days as i128).abs() <= TD_MAX_DAYS {
        let nd = days as i128 + (micros as i128).div_euclid(86_400_000_000);
        if nd.abs() > TD_MAX_DAYS {
            return Err("days=…; must have magnitude <= 999999999".into());
        }
    }
    Ok(())
}

fn hex(buf: &mut Vec<u8>, b: &[u8]) {
    const H: &[u8; 16] = b"0123456789abcdef";
    // JSON of "\x…": the backslash escaped.
    buf.extend_from_slice(b"\"\\\\x");
    for &c in b {
        buf.push(H[(c >> 4) as usize]);
        buf.push(H[(c & 15) as usize]);
    }
    buf.push(b'"');
}

fn uuid_text(buf: &mut Vec<u8>, b: &[u8]) {
    const H: &[u8; 16] = b"0123456789abcdef";
    buf.push(b'"');
    for (i, &c) in b.iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            buf.push(b'-');
        }
        buf.push(H[(c >> 4) as usize]);
        buf.push(H[(c & 15) as usize]);
    }
    buf.push(b'"');
}

/// The key atom of a JSON string written at `start` in `buf`: its bytes as written.
fn key_written(k: &Keyer, key: Option<&mut Vec<u8>>, buf: &[u8], start: usize) {
    if let Some(key) = key {
        k.bytes(key, &buf[start..]);
    }
}

fn json_out(j: J, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Out {
    if let Some(key) = key {
        k.canon(key, &j);
    }
    Out::Json(j)
}

/// A text value: `_convert_value` parses one that looks like JSON.
fn write_text(s: &[u8], buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Out {
    if let Some(j) = jtext::parse_json_text(s) {
        return json_out(j, key, k);
    }
    if let Some(key) = key {
        k.bytes(key, s);
    }
    fmt::write_str(buf, s);
    Out::Leaf
}

fn dec_out(neg: bool, digits: &str, small: Option<i128>, scale: i32, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Out {
    let (v, f) = fmt::decimal_value(neg, digits, small, scale);
    if let Some(key) = key {
        k.num(key, f);
    }
    match v {
        DecVal::Int(i) => {
            let mut b = itoa::Buffer::new();
            buf.extend_from_slice(b.format(i).as_bytes());
            Out::Leaf
        }
        DecVal::BigInt => Out::Poison(fmt::BIG_INT),
        DecVal::Float(f) => {
            fmt::write_f64(buf, f);
            Out::Leaf
        }
    }
}

// ---------------------------------------------------------------- scalar columns

struct NullCell;
impl Cell for NullCell {
    fn is_null(&self, _: usize) -> bool {
        true
    }
    fn write(&self, _: usize, _: &mut Vec<u8>, key: Option<&mut Vec<u8>>, _: &mut Keyer) -> Res<Out> {
        if let Some(key) = key {
            key.push(K_NULL);
        }
        Ok(Out::Null)
    }
}

macro_rules! null_guard {
    ($self:ident, $row:ident, $key:ident) => {
        if $self.a.is_null($row) {
            if let Some(key) = $key {
                key.push(K_NULL);
            }
            return Ok(Out::Null);
        }
    };
}

struct BoolCell {
    a: BooleanArray,
}
impl Cell for BoolCell {
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        null_guard!(self, r, key);
        let s = buf.len();
        buf.extend_from_slice(if self.a.value(r) { b"true" } else { b"false" });
        key_written(k, key, buf, s);
        Ok(Out::Leaf)
    }
}

struct IntCell<T: ArrowPrimitiveType> {
    a: PrimitiveArray<T>,
}
impl<T: ArrowPrimitiveType> Cell for IntCell<T>
where
    T::Native: itoa::Integer,
{
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        null_guard!(self, r, key);
        let s = buf.len();
        let mut b = itoa::Buffer::new();
        buf.extend_from_slice(b.format(self.a.value(r)).as_bytes());
        key_written(k, key, buf, s);
        Ok(Out::Leaf)
    }
}

struct FloatCell<T: ArrowPrimitiveType> {
    a: PrimitiveArray<T>,
}
impl<T: ArrowPrimitiveType> Cell for FloatCell<T>
where
    T::Native: Into<f64>,
{
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        null_guard!(self, r, key);
        let f: f64 = self.a.value(r).into();
        if let Some(key) = key {
            k.num(key, f);
        }
        fmt::write_f64(buf, f);
        Ok(Out::Leaf)
    }
    fn key(&self, r: usize, key: &mut Vec<u8>, k: &mut Keyer, _: &mut Vec<u8>) -> Res<()> {
        if self.a.is_null(r) {
            key.push(K_NULL);
        } else {
            k.num(key, self.a.value(r).into());
        }
        Ok(())
    }
}

struct Dec128Cell {
    a: Decimal128Array,
    scale: i32,
}
impl Cell for Dec128Cell {
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        null_guard!(self, r, key);
        let c = self.a.value(r);
        let mut b = itoa::Buffer::new();
        let digits = b.format(c.unsigned_abs());
        Ok(dec_out(c < 0, digits, Some(c), self.scale, buf, key, k))
    }
    fn key(&self, r: usize, key: &mut Vec<u8>, k: &mut Keyer, _: &mut Vec<u8>) -> Res<()> {
        if self.a.is_null(r) {
            key.push(K_NULL);
        } else {
            k.num(key, self.f(r));
        }
        Ok(())
    }
}

impl Dec128Cell {
    fn f(&self, r: usize) -> f64 {
        let c = self.a.value(r);
        let mut b = itoa::Buffer::new();
        fmt::decimal_value(c < 0, b.format(c.unsigned_abs()), Some(c), self.scale).1
    }
}

struct Dec256Cell {
    a: Decimal256Array,
    scale: i32,
}
impl Cell for Dec256Cell {
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        null_guard!(self, r, key);
        let c = self.a.value(r);
        let s = c.to_string();
        let (neg, digits) = match s.strip_prefix('-') {
            Some(d) => (true, d),
            None => (false, s.as_str()),
        };
        Ok(dec_out(neg, digits, c.to_i128(), self.scale, buf, key, k))
    }
}

struct StrCell<A> {
    a: A,
    /// The last JSON-looking text keyed and its key atom: equal text, equal parse, equal key.
    last: std::cell::RefCell<(Vec<u8>, Vec<u8>)>,
}
impl<A> Cell for StrCell<A>
where
    A: Array + Send + Sync,
    for<'b> &'b A: ArrayAccessor<Item = &'b str>,
{
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        null_guard!(self, r, key);
        Ok(write_text((&self.a).value(r).as_bytes(), buf, key, k))
    }
    fn key(&self, r: usize, key: &mut Vec<u8>, k: &mut Keyer, _: &mut Vec<u8>) -> Res<()> {
        if self.a.is_null(r) {
            key.push(K_NULL);
            return Ok(());
        }
        let s = (&self.a).value(r).as_bytes();
        if s.len() < 2 || (s[0] != b'{' && s[0] != b'[') {
            k.bytes(key, s);
            return Ok(());
        }
        let mut last = self.last.borrow_mut();
        if last.1.is_empty() || last.0 != s {
            let mut atom = Vec::new();
            match jtext::parse_json_text(s) {
                Some(j) => k.canon(&mut atom, &j),
                None => k.bytes(&mut atom, s),
            }
            last.0.clear();
            last.0.extend_from_slice(s);
            last.1 = atom;
        }
        key.extend_from_slice(&last.1);
        Ok(())
    }
}

/// An enum (DuckDB's ENUM, Arrow's dictionary of strings): its value's text.
struct DictStrCell {
    keys: Vec<usize>,
    nulls: Option<arrow::buffer::NullBuffer>,
    values: StringArray,
}
impl Cell for DictStrCell {
    fn is_null(&self, r: usize) -> bool {
        self.nulls.as_ref().is_some_and(|n| n.is_null(r)) || self.values.is_null(self.keys[r])
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        if self.is_null(r) {
            if let Some(key) = key {
                key.push(K_NULL);
            }
            return Ok(Out::Null);
        }
        Ok(write_text(self.values.value(self.keys[r]).as_bytes(), buf, key, k))
    }
}

struct BinCell<A> {
    a: A,
    uuid: bool,
}
impl<A> Cell for BinCell<A>
where
    A: Array + Send + Sync,
    for<'b> &'b A: ArrayAccessor<Item = &'b [u8]>,
{
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        null_guard!(self, r, key);
        let s = buf.len();
        if self.uuid {
            uuid_text(buf, (&self.a).value(r));
        } else {
            hex(buf, (&self.a).value(r));
        }
        key_written(k, key, buf, s);
        Ok(Out::Leaf)
    }
}

struct Date32Cell {
    a: Date32Array,
}
impl Date32Cell {
    fn text(&self, r: usize, buf: &mut Vec<u8>) -> Res<()> {
        let v = self.a.value(r);
        buf.push(b'"');
        if v == DATE_INF {
            buf.extend_from_slice(b"9999-12-31");
        } else if v == -DATE_INF {
            buf.extend_from_slice(b"0001-01-01");
        } else {
            fmt::date_text(buf, v as i64)?;
        }
        buf.push(b'"');
        Ok(())
    }
}
impl Cell for Date32Cell {
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        null_guard!(self, r, key);
        let s = buf.len();
        self.text(r, buf)?;
        key_written(k, key, buf, s);
        Ok(Out::Leaf)
    }
    fn validate(&self) -> Res<()> {
        let mut scratch = Vec::new();
        for r in 0..self.a.len() {
            if !self.a.is_null(r) {
                scratch.clear();
                self.text(r, &mut scratch)?;
            }
        }
        Ok(())
    }
}

struct TsCell {
    a: PrimitiveArray<Int64Type>,
    mult: i64,
    offset: Option<i32>,
    /// A µs column: `column_values` reads ±(2^63-1) as the IR infinity.
    infinities: bool,
}
impl TsCell {
    fn text(&self, r: usize, buf: &mut Vec<u8>) -> Res<()> {
        let v = self.a.value(r);
        if self.infinities && (v == TS_INF || v == -TS_INF) {
            buf.extend_from_slice(if v > 0 { b"\"9999-12-31T23:59:59.999999" } else { b"\"0001-01-01T00:00:00" });
            if self.offset.is_some() {
                buf.extend_from_slice(b"+00:00");
            }
            buf.push(b'"');
            return Ok(());
        }
        ts_value(v, self.mult, self.offset, buf)
    }
}
impl Cell for TsCell {
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        null_guard!(self, r, key);
        let s = buf.len();
        self.text(r, buf)?;
        key_written(k, key, buf, s);
        Ok(Out::Leaf)
    }
    fn validate(&self) -> Res<()> {
        let mut scratch = Vec::new();
        for r in 0..self.a.len() {
            if !self.a.is_null(r) {
                scratch.clear();
                self.text(r, &mut scratch)?;
            }
        }
        Ok(())
    }
}

/// time32/time64 as micros of the day.
struct TimeCell {
    a: PrimitiveArray<Int64Type>,
    mult: i64,
}
impl Cell for TimeCell {
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        null_guard!(self, r, key);
        let s = buf.len();
        let us = self.a.value(r) * self.mult;
        if !(0..86_400_000_000).contains(&us) {
            return Err("time out of range".into());
        }
        buf.push(b'"');
        fmt::clock_text(buf, us);
        buf.push(b'"');
        key_written(k, key, buf, s);
        Ok(Out::Leaf)
    }
    fn validate(&self) -> Res<()> {
        for r in 0..self.a.len() {
            if !self.a.is_null(r) && !(0..86_400_000_000).contains(&(self.a.value(r) * self.mult)) {
                return Err("time out of range".into());
            }
        }
        Ok(())
    }
}

struct DurationCell {
    a: PrimitiveArray<Int64Type>,
    mult: i64,
}
impl Cell for DurationCell {
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        null_guard!(self, r, key);
        let s = buf.len();
        let (d, u) = duration_parts(self.a.value(r) as i128 * self.mult as i128)?;
        buf.push(b'"');
        fmt::iso8601_duration(buf, 0, d, u);
        buf.push(b'"');
        key_written(k, key, buf, s);
        Ok(Out::Leaf)
    }
    fn validate(&self) -> Res<()> {
        for r in 0..self.a.len() {
            if !self.a.is_null(r) {
                duration_parts(self.a.value(r) as i128 * self.mult as i128)?;
            }
        }
        Ok(())
    }
}

struct IntervalCell {
    a: IntervalMonthDayNanoArray,
}
impl Cell for IntervalCell {
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        null_guard!(self, r, key);
        let s = buf.len();
        let v = self.a.value(r);
        let micros = v.nanoseconds.div_euclid(1000);
        interval_check(v.months, v.days, micros)?;
        buf.push(b'"');
        fmt::iso8601_duration(buf, v.months as i64, v.days as i64, micros);
        buf.push(b'"');
        key_written(k, key, buf, s);
        Ok(Out::Leaf)
    }
    fn validate(&self) -> Res<()> {
        for r in 0..self.a.len() {
            if !self.a.is_null(r) {
                let v = self.a.value(r);
                interval_check(v.months, v.days, v.nanoseconds.div_euclid(1000))?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------- nested columns

/// A list or struct column: `to_pylist`, then `_convert_value` of the list or dict.
struct NestedCell {
    a: ArrayRef,
    uuid: bool,
}
impl Cell for NestedCell {
    fn is_null(&self, r: usize) -> bool {
        self.a.is_null(r)
    }
    fn write(&self, r: usize, buf: &mut Vec<u8>, key: Option<&mut Vec<u8>>, k: &mut Keyer) -> Res<Out> {
        if self.a.is_null(r) {
            if let Some(key) = key {
                key.push(K_NULL);
            }
            return Ok(Out::Null);
        }
        let mut j = to_j(self.a.as_ref(), r, self.uuid)?;
        j.convert_value();
        let _ = buf;
        Ok(json_out(j, key, k))
    }
    fn validate(&self) -> Res<()> {
        for r in 0..self.a.len() {
            if !self.a.is_null(r) {
                to_j(self.a.as_ref(), r, self.uuid)?;
            }
        }
        Ok(())
    }
}

fn is_uuid_field(f: &Field) -> bool {
    f.metadata().get("ARROW:extension:name").is_some_and(|n| n == "arrow.uuid")
}

/// A nested element as `to_pylist` gives it and `_convert_value` converts it (strings are left
/// for `convert_value`, which parses those that look like JSON).
fn to_j(a: &dyn Array, i: usize, uuid: bool) -> Res<J> {
    if a.is_null(i) {
        return Ok(J::Null);
    }
    let mut b = Vec::new();
    let text = |b: Vec<u8>| -> J {
        // bytes written as a JSON string literal; recover the str for the tree
        let s = &b[1..b.len() - 1];
        J::str(s.to_vec())
    };
    Ok(match a.data_type() {
        DataType::Null => J::Null,
        DataType::Boolean => J::Bool(a.as_boolean().value(i)),
        DataType::Int8 => J::Int(a.as_primitive::<Int8Type>().value(i).to_string()),
        DataType::Int16 => J::Int(a.as_primitive::<Int16Type>().value(i).to_string()),
        DataType::Int32 => J::Int(a.as_primitive::<Int32Type>().value(i).to_string()),
        DataType::Int64 => J::Int(a.as_primitive::<Int64Type>().value(i).to_string()),
        DataType::UInt8 => J::Int(a.as_primitive::<UInt8Type>().value(i).to_string()),
        DataType::UInt16 => J::Int(a.as_primitive::<UInt16Type>().value(i).to_string()),
        DataType::UInt32 => J::Int(a.as_primitive::<UInt32Type>().value(i).to_string()),
        DataType::UInt64 => J::Int(a.as_primitive::<UInt64Type>().value(i).to_string()),
        DataType::Float32 => J::Float(a.as_primitive::<Float32Type>().value(i) as f64),
        DataType::Float64 => J::Float(a.as_primitive::<Float64Type>().value(i)),
        DataType::Utf8 => J::str(a.as_string::<i32>().value(i).as_bytes().to_vec()),
        DataType::LargeUtf8 => J::str(a.as_string::<i64>().value(i).as_bytes().to_vec()),
        DataType::Utf8View => J::str(a.as_string_view().value(i).as_bytes().to_vec()),
        DataType::Decimal128(_, s) => {
            let c = a.as_primitive::<Decimal128Type>().value(i);
            let digits = c.unsigned_abs().to_string();
            dec_j(fmt::decimal_value(c < 0, &digits, Some(c), *s as i32))
        }
        DataType::Decimal256(_, s) => {
            let c = a.as_primitive::<Decimal256Type>().value(i);
            let st = c.to_string();
            let (neg, d) = match st.strip_prefix('-') {
                Some(d) => (true, d),
                None => (false, st.as_str()),
            };
            dec_j(fmt::decimal_value(neg, d, c.to_i128(), *s as i32))
        }
        DataType::Date32 => {
            b.push(b'"');
            fmt::date_text(&mut b, a.as_primitive::<Date32Type>().value(i) as i64)?;
            b.push(b'"');
            text(b)
        }
        DataType::Timestamp(u, tz) => {
            let off = tz.as_deref().map(tz_offset).transpose()?;
            let v = timestamp_raw(a, u, i);
            ts_value(v, unit_micros(u)?, off, &mut b)?;
            text(b)
        }
        DataType::Time32(u) | DataType::Time64(u) => {
            let mult = unit_micros(u)?;
            let raw = match a.data_type() {
                DataType::Time32(TimeUnit::Second) => a.as_primitive::<Time32SecondType>().value(i) as i64,
                DataType::Time32(_) => a.as_primitive::<Time32MillisecondType>().value(i) as i64,
                _ => a.as_primitive::<Time64MicrosecondType>().value(i),
            };
            b.push(b'"');
            fmt::clock_text(&mut b, raw * mult);
            b.push(b'"');
            text(b)
        }
        DataType::Duration(u) => {
            let mult = unit_micros(u)?;
            let raw = match u {
                TimeUnit::Second => a.as_primitive::<DurationSecondType>().value(i),
                TimeUnit::Millisecond => a.as_primitive::<DurationMillisecondType>().value(i),
                _ => a.as_primitive::<DurationMicrosecondType>().value(i),
            };
            let (d, us) = duration_parts(raw as i128 * mult as i128)?;
            b.push(b'"');
            fmt::iso8601_duration(&mut b, 0, d, us);
            b.push(b'"');
            text(b)
        }
        DataType::Binary => {
            hex(&mut b, a.as_binary::<i32>().value(i));
            unescape_hex(b)
        }
        DataType::LargeBinary => {
            hex(&mut b, a.as_binary::<i64>().value(i));
            unescape_hex(b)
        }
        DataType::FixedSizeBinary(_) => {
            let v = a.as_fixed_size_binary().value(i);
            if uuid {
                uuid_text(&mut b, v);
                text(b)
            } else {
                hex(&mut b, v);
                unescape_hex(b)
            }
        }
        DataType::List(f) => list_j(a.as_list::<i32>().value(i).as_ref(), is_uuid_field(f))?,
        DataType::LargeList(f) => list_j(a.as_list::<i64>().value(i).as_ref(), is_uuid_field(f))?,
        DataType::FixedSizeList(f, _) => list_j(a.as_fixed_size_list().value(i).as_ref(), is_uuid_field(f))?,
        DataType::Struct(fields) => {
            let mut names: Vec<&String> = fields.iter().map(|f| f.name()).collect();
            names.sort();
            if names.windows(2).any(|w| w[0] == w[1]) {
                return Err("Converting to Python dictionary is not supported when duplicate field names are present".into());
            }
            let s = a.as_struct();
            let mut entries = Vec::new();
            for (fi, f) in fields.iter().enumerate() {
                let v = to_j(s.column(fi).as_ref(), i, is_uuid_field(f))?;
                J::obj_set(&mut entries, f.name().as_bytes().to_vec(), v);
            }
            J::Obj(entries)
        }
        other => return Err(not_impl(&format!("{other} inside a list or struct"))),
    })
}

fn unescape_hex(b: Vec<u8>) -> J {
    // "\\x…" as JSON → the str \x…
    J::str([b"\\x".as_slice(), &b[4..b.len() - 1]].concat())
}

fn dec_j((v, f): (DecVal, f64)) -> J {
    match v {
        DecVal::Int(i) => J::Int(i.to_string()),
        DecVal::Float(f) => J::Float(f),
        // int(f): the float's exact integer, which orjson refuses (J::poison).
        DecVal::BigInt => J::Int(format!("{:.0}", f)),
    }
}

fn timestamp_raw(a: &dyn Array, u: &TimeUnit, i: usize) -> i64 {
    match u {
        TimeUnit::Second => a.as_primitive::<TimestampSecondType>().value(i),
        TimeUnit::Millisecond => a.as_primitive::<TimestampMillisecondType>().value(i),
        TimeUnit::Microsecond => a.as_primitive::<TimestampMicrosecondType>().value(i),
        TimeUnit::Nanosecond => a.as_primitive::<TimestampNanosecondType>().value(i),
    }
}

fn list_j(v: &dyn Array, uuid: bool) -> Res<J> {
    let mut out = Vec::with_capacity(v.len());
    for i in 0..v.len() {
        out.push(to_j(v, i, uuid)?);
    }
    Ok(J::Arr(out))
}

// ---------------------------------------------------------------- the column's converter

fn as_i64<T: ArrowPrimitiveType>(a: &dyn Array) -> PrimitiveArray<Int64Type>
where
    T::Native: Into<i64>,
{
    a.as_primitive::<T>().unary(|v| v.into())
}

pub fn cell_for(field: &Field, a: &ArrayRef) -> Res<Box<dyn Cell>> {
    let uuid = is_uuid_field(field);
    Ok(match a.data_type() {
        DataType::Null => Box::new(NullCell),
        DataType::Boolean => Box::new(BoolCell { a: a.as_boolean().clone() }),
        DataType::Int8 => Box::new(IntCell { a: a.as_primitive::<Int8Type>().clone() }),
        DataType::Int16 => Box::new(IntCell { a: a.as_primitive::<Int16Type>().clone() }),
        DataType::Int32 => Box::new(IntCell { a: a.as_primitive::<Int32Type>().clone() }),
        DataType::Int64 => Box::new(IntCell { a: a.as_primitive::<Int64Type>().clone() }),
        DataType::UInt8 => Box::new(IntCell { a: a.as_primitive::<UInt8Type>().clone() }),
        DataType::UInt16 => Box::new(IntCell { a: a.as_primitive::<UInt16Type>().clone() }),
        DataType::UInt32 => Box::new(IntCell { a: a.as_primitive::<UInt32Type>().clone() }),
        DataType::UInt64 => Box::new(IntCell { a: a.as_primitive::<UInt64Type>().clone() }),
        DataType::Float32 => Box::new(FloatCell { a: a.as_primitive::<Float32Type>().clone() }),
        DataType::Float64 => Box::new(FloatCell { a: a.as_primitive::<Float64Type>().clone() }),
        DataType::Decimal128(_, s) => Box::new(Dec128Cell { a: a.as_primitive::<Decimal128Type>().clone(), scale: *s as i32 }),
        DataType::Decimal256(_, s) => Box::new(Dec256Cell { a: a.as_primitive::<Decimal256Type>().clone(), scale: *s as i32 }),
        DataType::Utf8 => Box::new(StrCell { a: a.as_string::<i32>().clone(), last: Default::default() }),
        DataType::LargeUtf8 => Box::new(StrCell { a: a.as_string::<i64>().clone(), last: Default::default() }),
        DataType::Utf8View => Box::new(StrCell { a: a.as_string_view().clone(), last: Default::default() }),
        DataType::Dictionary(_, v) if **v == DataType::Utf8 => {
            let d = a.as_any_dictionary();
            Box::new(DictStrCell {
                keys: d.normalized_keys(),
                nulls: d.keys().nulls().cloned(),
                values: d.values().as_string::<i32>().clone(),
            })
        }
        DataType::Binary => Box::new(BinCell { a: a.as_binary::<i32>().clone(), uuid: false }),
        DataType::LargeBinary => Box::new(BinCell { a: a.as_binary::<i64>().clone(), uuid: false }),
        DataType::FixedSizeBinary(16) if uuid => Box::new(BinCell { a: a.as_fixed_size_binary().clone(), uuid: true }),
        DataType::FixedSizeBinary(_) => Box::new(BinCell { a: a.as_fixed_size_binary().clone(), uuid: false }),
        DataType::Date32 => Box::new(Date32Cell { a: a.as_primitive::<Date32Type>().clone() }),
        DataType::Timestamp(u, tz) => {
            let offset = tz.as_deref().map(tz_offset).transpose()?;
            let mult = unit_micros(u)?;
            let raw = match u {
                TimeUnit::Second => a.as_primitive::<TimestampSecondType>().reinterpret_cast::<Int64Type>(),
                TimeUnit::Millisecond => a.as_primitive::<TimestampMillisecondType>().reinterpret_cast::<Int64Type>(),
                _ => a.as_primitive::<TimestampMicrosecondType>().reinterpret_cast::<Int64Type>(),
            };
            Box::new(TsCell { a: raw, mult, offset, infinities: *u == TimeUnit::Microsecond })
        }
        DataType::Time32(TimeUnit::Second) => Box::new(TimeCell { a: as_i64::<Time32SecondType>(a.as_ref()), mult: 1_000_000 }),
        DataType::Time32(TimeUnit::Millisecond) => Box::new(TimeCell { a: as_i64::<Time32MillisecondType>(a.as_ref()), mult: 1_000 }),
        DataType::Time64(TimeUnit::Microsecond) => Box::new(TimeCell { a: a.as_primitive::<Time64MicrosecondType>().reinterpret_cast::<Int64Type>(), mult: 1 }),
        DataType::Duration(u) => {
            let mult = unit_micros(u)?;
            let raw = match u {
                TimeUnit::Second => a.as_primitive::<DurationSecondType>().reinterpret_cast::<Int64Type>(),
                TimeUnit::Millisecond => a.as_primitive::<DurationMillisecondType>().reinterpret_cast::<Int64Type>(),
                _ => a.as_primitive::<DurationMicrosecondType>().reinterpret_cast::<Int64Type>(),
            };
            Box::new(DurationCell { a: raw, mult })
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => Box::new(IntervalCell { a: a.as_primitive::<IntervalMonthDayNanoType>().clone() }),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) | DataType::Struct(_) => {
            Box::new(NestedCell { a: a.clone(), uuid })
        }
        other => return Err(not_impl(&format!("Arrow type {other}"))),
    })
}
