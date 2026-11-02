//! A Python value written as the product writes it: the response body (`json_response.dumps`:
//! orjson, `_encode_unknown` for what orjson does not know, every int with all its digits), the
//! dedup key (`serialize._to_hashable`: Python `==` on the value, or the text of
//! `json.dumps(sort_keys=True, default=str)` for a dict or list), and `str()` / `repr()`.

use crate::fmt;
use crate::pyval::{Dt, Error, Res, Tz, P};
use crate::text::{self, IvKind, Rd};

/// orjson refuses a non-empty container nested deeper than this.
pub const MAX_DEPTH: usize = 254;

pub fn recursion() -> Error {
    Error::ResponseRefused("nests deeper than the 255 levels a JSON response is written to".into())
}

pub fn surrogate() -> Error {
    Error::ResponseRefused("holds a lone UTF-16 surrogate, which is no character UTF-8 can carry".into())
}

fn quoted(out: &mut Vec<u8>, f: impl FnOnce(&mut Vec<u8>)) {
    let mut t = Vec::new();
    f(&mut t);
    fmt::write_str(out, &t);
}

pub fn write_text(out: &mut Vec<u8>, s: &[u8]) -> Res<()> {
    if crate::json::has_surrogate(s) {
        return Err(surrogate());
    }
    fmt::write_str(out, s);
    Ok(())
}

/// `iso8601_duration` of a timedelta / pandas Timedelta / IR interval.
fn duration(out: &mut Vec<u8>, p: &P) {
    match p {
        P::Td(us) => {
            let (d, u) = text::td_parts(*us);
            fmt::iso8601_duration(out, 0, d, u)
        }
        P::Iv(m, d, u) => fmt::iso8601_duration(out, *m, *d, *u),
        _ => unreachable!(),
    }
}

/// `temporal_text`: `isoformat` with the fraction's trailing zeros dropped; the IR infinities as
/// `infinity` / `-infinity`.
pub fn temporal_text(out: &mut Vec<u8>, p: &P) {
    match p {
        P::Date(d) => text::date_iso(out, *d as i64),
        P::Dt(dt) => text::dt_iso(out, dt, b'T', true),
        P::Time(us) => text::time_iso(out, *us, true),
        P::InfDate(s) | P::InfDt(s, _) => out.extend_from_slice(if *s > 0 { b"infinity" } else { b"-infinity" }),
        _ => unreachable!(),
    }
}

/// The JSON bytes of `p` as `json_response.dumps` writes it; `depth` is the nesting level of the
/// container holding it.
pub fn write(out: &mut Vec<u8>, p: &P, depth: usize) -> Res<()> {
    match p {
        P::None => out.extend_from_slice(b"null"),
        P::Bool(true) => out.extend_from_slice(b"true"),
        P::Bool(false) => out.extend_from_slice(b"false"),
        P::Int(i) => {
            let mut b = itoa::Buffer::new();
            out.extend_from_slice(b.format(*i).as_bytes());
        }
        P::BigInt(s) => out.extend_from_slice(s.as_bytes()),
        P::Float(f) => fmt::write_f64(out, *f),
        P::JNum(s) => out.extend_from_slice(s.as_bytes()),
        P::Str(s) => write_text(out, s)?,
        P::Bytes(b) => quoted(out, |t| text::hex_text(t, b)),
        P::Dec(d) => quoted(out, |t| text::decimal_f(t, d)),
        P::DecSpecial(s) => fmt::write_str(out, special_f(s).as_bytes()),
        P::Date(d) => quoted(out, |t| text::date_iso(t, *d as i64)),
        P::InfDate(s) | P::InfDt(s, _) => out.extend_from_slice(if *s > 0 { b"\"infinity\"" } else { b"\"-infinity\"" }),
        P::Dt(dt) => quoted(out, |t| text::dt_iso(t, dt, b'T', false)),
        P::Time(us) => quoted(out, |t| text::time_iso(t, *us, false)),
        P::Td(..) | P::Iv(..) => quoted(out, |t| duration(t, p)),
        P::Mdn(m, d, n) => {
            out.push(b'[');
            let mut b = itoa::Buffer::new();
            out.extend_from_slice(b.format(*m).as_bytes());
            out.push(b',');
            out.extend_from_slice(b.format(*d).as_bytes());
            out.push(b',');
            out.extend_from_slice(b.format(*n).as_bytes());
            out.push(b']');
        }
        P::Uuid(u) => quoted(out, |t| text::uuid_text(t, u)),
        P::List(v) | P::Tuple(v) => {
            if depth + 1 > MAX_DEPTH && !v.is_empty() {
                return Err(recursion());
            }
            out.push(b'[');
            for (i, e) in v.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write(out, e, depth + 1)?;
            }
            out.push(b']');
        }
        P::Dict(e) => {
            if depth + 1 > MAX_DEPTH && !e.is_empty() {
                return Err(recursion());
            }
            out.push(b'{');
            for (i, (k, v)) in e.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_text(out, k)?;
                out.push(b':');
                write(out, v, depth + 1)?;
            }
            out.push(b'}');
        }
    }
    Ok(())
}

/// `format(Decimal(special), "f")`.
fn special_f(s: &str) -> &str {
    s
}

// ---------------------------------------------------------------- str and repr

/// `str(p)`.
pub fn py_str(out: &mut Vec<u8>, p: &P) {
    match p {
        P::Str(s) => out.extend_from_slice(s),
        P::Bool(b) => out.extend_from_slice(if *b { b"True" } else { b"False" }),
        P::None => out.extend_from_slice(b"None"),
        P::Dec(d) => text::decimal_str(out, d),
        P::DecSpecial(s) => out.extend_from_slice(s.as_bytes()),
        P::Date(d) => text::date_iso(out, *d as i64),
        P::InfDate(s) => text::inf_date_iso(out, *s),
        P::Dt(dt) => text::dt_iso(out, dt, b' ', false),
        P::InfDt(s, a) => text::dt_iso(out, &text::inf_dt(*s, *a), b' ', false),
        P::Time(us) => text::time_iso(out, *us, false),
        P::Td(us) => text::td_str(out, *us),
        P::Iv(m, d, u) => match text::iv_kind(*m, *d, *u) {
            IvKind::Td(us) => text::td_str(out, us),
            IvKind::Rd => Rd::new(*m, *d, *u).repr(out, "relativedelta"),
            IvKind::Exact => Rd::new(*m, *d, *u).repr(out, "_ExactInterval"),
        },
        P::Uuid(u) => text::uuid_text(out, u),
        _ => py_repr(out, p),
    }
}

/// `repr(p)`.
pub fn py_repr(out: &mut Vec<u8>, p: &P) {
    match p {
        P::None => out.extend_from_slice(b"None"),
        P::Bool(b) => out.extend_from_slice(if *b { b"True" } else { b"False" }),
        P::Int(i) => {
            let mut b = itoa::Buffer::new();
            out.extend_from_slice(b.format(*i).as_bytes());
        }
        P::BigInt(s) => out.extend_from_slice(s.as_bytes()),
        P::Float(f) => text::float_repr(out, *f),
        P::JNum(s) => text::float_repr(out, s.parse::<f64>().unwrap()),
        P::Str(s) => text::str_repr(out, s),
        P::Bytes(b) => text::bytes_repr(out, b),
        P::Dec(_) | P::DecSpecial(_) => {
            out.extend_from_slice(b"Decimal('");
            py_str(out, p);
            out.extend_from_slice(b"')");
        }
        P::Date(d) => {
            let (y, m, dd) = fmt::civil(*d as i64);
            out.extend_from_slice(format!("datetime.date({y}, {m}, {dd})").as_bytes());
        }
        P::InfDate(s) => out.extend_from_slice(if *s > 0 { b"_InfiniteDate(9999, 12, 31)" } else { b"_InfiniteDate(1, 1, 1)" }),
        P::Dt(dt) => dt_repr(out, dt),
        P::InfDt(s, a) => {
            let mut t = Vec::new();
            dt_repr(&mut t, &text::inf_dt(*s, *a));
            out.extend_from_slice(b"_InfiniteDatetime");
            out.extend_from_slice(&t[b"datetime.datetime".len()..]);
        }
        P::Time(us) => {
            let secs = us / 1_000_000;
            let (h, m, s, f) = (secs / 3600, (secs / 60) % 60, secs % 60, us % 1_000_000);
            let mut parts = vec![h.to_string(), m.to_string()];
            if s != 0 || f != 0 {
                parts.push(s.to_string());
            }
            if f != 0 {
                parts.push(f.to_string());
            }
            out.extend_from_slice(format!("datetime.time({})", parts.join(", ")).as_bytes());
        }
        P::Td(us) => {
            let days = us.div_euclid(86_400_000_000);
            let rem = us.rem_euclid(86_400_000_000);
            let (s, f) = (rem / 1_000_000, rem % 1_000_000);
            let mut parts = Vec::new();
            if days != 0 {
                parts.push(format!("days={days}"));
            }
            if s != 0 {
                parts.push(format!("seconds={s}"));
            }
            if f != 0 {
                parts.push(format!("microseconds={f}"));
            }
            if parts.is_empty() {
                out.extend_from_slice(b"datetime.timedelta(0)");
            } else {
                out.extend_from_slice(format!("datetime.timedelta({})", parts.join(", ")).as_bytes());
            }
        }
        P::Iv(m, d, u) => match text::iv_kind(*m, *d, *u) {
            IvKind::Td(us) => py_repr(out, &P::Td(us)),
            _ => py_str(out, p),
        },
        P::Mdn(m, d, n) => out.extend_from_slice(format!("MonthDayNano(months={m}, days={d}, nanoseconds={n})").as_bytes()),
        P::Uuid(u) => {
            out.extend_from_slice(b"UUID('");
            text::uuid_text(out, u);
            out.extend_from_slice(b"')");
        }
        P::List(v) => {
            out.push(b'[');
            for (i, e) in v.iter().enumerate() {
                if i > 0 {
                    out.extend_from_slice(b", ");
                }
                py_repr(out, e);
            }
            out.push(b']');
        }
        P::Tuple(v) => {
            out.push(b'(');
            for (i, e) in v.iter().enumerate() {
                if i > 0 {
                    out.extend_from_slice(b", ");
                }
                py_repr(out, e);
            }
            if v.len() == 1 {
                out.push(b',');
            }
            out.push(b')');
        }
        P::Dict(e) => {
            out.push(b'{');
            for (i, (k, v)) in e.iter().enumerate() {
                if i > 0 {
                    out.extend_from_slice(b", ");
                }
                text::str_repr(out, k);
                out.extend_from_slice(b": ");
                py_repr(out, v);
            }
            out.push(b'}');
        }
    }
}

fn td_repr(secs: i64) -> String {
    let (d, s) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    match (d, s) {
        (0, 0) => "datetime.timedelta(0)".into(),
        (0, s) => format!("datetime.timedelta(seconds={s})"),
        (d, 0) => format!("datetime.timedelta(days={d})"),
        (d, s) => format!("datetime.timedelta(days={d}, seconds={s})"),
    }
}

/// The repr of a datetime's tzinfo.
fn tz_repr(tz: Tz) -> String {
    match tz {
        Tz::Naive => String::new(),
        Tz::Py(0) => "datetime.timezone.utc".into(),
        Tz::Py(o) => format!("datetime.timezone({})", td_repr(o as i64)),
        Tz::Named(name) => {
            let mut q = Vec::new();
            text::str_repr(&mut q, name.as_bytes());
            format!("zoneinfo.ZoneInfo(key={})", String::from_utf8(q).unwrap())
        }
    }
}

fn dt_repr(out: &mut Vec<u8>, dt: &Dt) {
    let day = dt.local_us.div_euclid(86_400_000_000);
    let tod = dt.local_us.rem_euclid(86_400_000_000);
    let (y, mo, d) = fmt::civil(day);
    let secs = tod / 1_000_000;
    let (h, mi, s, f) = (secs / 3600, (secs / 60) % 60, secs % 60, tod % 1_000_000);
    let mut parts = vec![y.to_string(), mo.to_string(), d.to_string(), h.to_string(), mi.to_string()];
    if s != 0 || f != 0 {
        parts.push(s.to_string());
    }
    if f != 0 {
        parts.push(f.to_string());
    }
    let tz = match dt.tz {
        Tz::Naive => String::new(),
        t => format!(", tzinfo={}", tz_repr(t)),
    };
    out.extend_from_slice(format!("datetime.datetime({}{})", parts.join(", "), tz).as_bytes());
}

// ---------------------------------------------------------------- the dedup key

/// Key atoms: two values have one atom exactly when they are the same typed value (REQ-1867): a
/// string is never a number, `1` is not `1.0` nor `true` (a JSON number keeps its digits), a JSON
/// array is never the text of one, and NaN is NaN. Rows (and list elements) are one when every
/// atom of their key is.
pub const K_NULL: u8 = 0;
pub const K_STR: u8 = 1;
pub const K_INT: u8 = 2;
pub const K_FLOAT: u8 = 3;
pub const K_BOOL: u8 = 4;
pub const K_JNUM: u8 = 5;
pub const K_LIST: u8 = 6;
pub const K_TUPLE: u8 = 7;
pub const K_DICT: u8 = 8;
pub const K_OTHER: u8 = 9;

#[derive(Default)]
pub struct Keyer;

impl Keyer {
    pub fn new() -> Self {
        Keyer
    }

    fn bytes(key: &mut Vec<u8>, tag: u8, b: &[u8]) {
        key.push(tag);
        key.extend_from_slice(&(b.len() as u64).to_le_bytes());
        key.extend_from_slice(b);
    }

    pub fn str_atom(&self, key: &mut Vec<u8>, s: &[u8]) {
        Self::bytes(key, K_STR, s);
    }

    pub fn int_atom(&self, key: &mut Vec<u8>, i: i128) {
        let mut b = itoa::Buffer::new();
        Self::bytes(key, K_INT, b.format(i).as_bytes());
    }

    pub fn bool_atom(&self, key: &mut Vec<u8>, b: bool) {
        key.push(K_BOOL);
        key.push(b as u8);
    }

    pub fn float_atom(&self, key: &mut Vec<u8>, f: f64) {
        key.push(K_FLOAT);
        // Every NaN is the one NaN, as PostgreSQL compares them.
        let bits = if f.is_nan() { f64::NAN.to_bits() } else { f.to_bits() };
        key.extend_from_slice(&bits.to_le_bytes());
    }

    /// The atom of a converted value, appended to `key`.
    pub fn atom(&self, key: &mut Vec<u8>, p: &P) {
        match p {
            P::None => key.push(K_NULL),
            P::Str(s) => self.str_atom(key, s),
            P::Bool(b) => self.bool_atom(key, *b),
            P::Int(i) => self.int_atom(key, *i),
            P::BigInt(s) => Self::bytes(key, K_INT, s.as_bytes()),
            P::Float(f) => self.float_atom(key, *f),
            P::JNum(s) => Self::bytes(key, K_JNUM, s.as_bytes()),
            P::List(v) | P::Tuple(v) => {
                key.push(if matches!(p, P::List(_)) { K_LIST } else { K_TUPLE });
                key.extend_from_slice(&(v.len() as u64).to_le_bytes());
                for e in v {
                    self.atom(key, e);
                }
            }
            P::Dict(e) => {
                key.push(K_DICT);
                key.extend_from_slice(&(e.len() as u64).to_le_bytes());
                for (k, v) in e {
                    self.str_atom(key, k);
                    self.atom(key, v);
                }
            }
            other => {
                // Any other value (a decimal, a date, a uuid, ...): its kind and its exact value.
                let mut t = Vec::new();
                py_repr(&mut t, other);
                Self::bytes(key, K_OTHER, &t);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;

    fn key(p: &P) -> Vec<u8> {
        let mut k = Vec::new();
        Keyer::new().atom(&mut k, p);
        k
    }

    #[test]
    fn a_value_is_its_typed_value() {
        let one = key(&P::Int(1));
        assert_ne!(one, key(&P::JNum(Cow::Borrowed("1.0"))));
        assert_ne!(one, key(&P::Bool(true)));
        assert_ne!(one, key(&P::str("1")));
        assert_ne!(key(&P::JNum(Cow::Borrowed("1"))), key(&P::JNum(Cow::Borrowed("1.0"))));
        assert_eq!(key(&P::JNum(Cow::Borrowed("1.50"))), key(&P::JNum(Cow::Borrowed("1.50"))));
    }

    #[test]
    fn a_container_is_never_its_text() {
        let list = P::List(vec![P::Int(1)]);
        assert_ne!(key(&list), key(&P::str("[1]")));
        assert_ne!(key(&list), key(&P::Tuple(vec![P::Int(1)])));
        assert_eq!(key(&list), key(&P::List(vec![P::Int(1)])));
    }

    #[test]
    fn nan_is_nan() {
        assert_eq!(key(&P::Float(f64::NAN)), key(&P::Float(-f64::NAN)));
        assert_ne!(key(&P::Float(f64::NAN)), key(&P::None));
        assert_ne!(key(&P::Float(0.0)), key(&P::Float(-0.0)));
    }
}
