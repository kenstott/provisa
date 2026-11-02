//! A Python value written as the product writes it: the response body (`json_response.dumps`:
//! orjson, `_encode_unknown` for what orjson does not know, every int with all its digits), the
//! dedup key (`serialize._to_hashable`: Python `==` on the value, or the text of
//! `json.dumps(sort_keys=True, default=str)` for a dict or list), and `str()` / `repr()`.

use crate::fmt;
use crate::pytz_data;
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
        P::Td(us, None) => {
            let (d, u) = text::td_parts(*us);
            fmt::iso8601_duration(out, 0, d, u)
        }
        P::Td(us, Some(n)) => {
            let (d, u) = text::pd_td_parts(*us, *n);
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
        P::NaT => out.extend_from_slice(b"NaT"),
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
        // orjson's own spelling; a pandas Timestamp is no datetime to orjson: isoformat.
        P::Dt(dt) => quoted(out, |t| text::dt_iso(t, dt, b'T', false)),
        P::NaT => out.extend_from_slice(b"\"NaT\""),
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
        P::NaT => out.extend_from_slice(b"NaT"),
        P::Time(us) => text::time_iso(out, *us, false),
        P::Td(us, None) => text::td_str(out, *us),
        P::Td(us, Some(n)) => text::pd_td_str(out, *us, *n),
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
        P::NaT => out.extend_from_slice(b"NaT"),
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
        P::Td(us, None) => {
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
        P::Td(us, Some(n)) => {
            out.extend_from_slice(b"Timedelta('");
            text::pd_td_str(out, *us, *n);
            out.extend_from_slice(b"')");
        }
        P::Iv(m, d, u) => match text::iv_kind(*m, *d, *u) {
            IvKind::Td(us) => py_repr(out, &P::Td(us, None)),
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
        Tz::Fixed(0) => "<UTC>".into(),
        Tz::Fixed(m) => format!("pytz.FixedOffset({m})"),
        Tz::Py(0) => "datetime.timezone.utc".into(),
        Tz::Py(o) => format!("datetime.timezone({})", td_repr(o as i64)),
        Tz::Named(z, t) => {
            let (name, _, _, fixed) = pytz_data::ZONES[z as usize];
            if let Some(r) = fixed {
                return r.into();
            }
            let (abbr, dst) = pytz_data::INFO[t as usize];
            let mut off = Vec::new();
            text::td_str(&mut off, pytz_data::TRANS[t as usize].1 as i128 * 1_000_000);
            let mut q = Vec::new();
            text::str_repr(&mut q, name.as_bytes());
            format!(
                "<DstTzInfo {} {}{} {}>",
                String::from_utf8(q).unwrap(),
                abbr,
                String::from_utf8(off).unwrap(),
                if dst { "DST" } else { "STD" }
            )
        }
    }
}

/// pandas' `tz=` in a Timestamp's repr.
fn pd_tz_name(tz: Tz) -> String {
    match tz {
        Tz::Fixed(0) | Tz::Py(0) => "UTC".into(),
        Tz::Fixed(m) => format!("pytz.FixedOffset({m})"),
        Tz::Named(z, _) => pytz_data::ZONES[z as usize].0.into(),
        Tz::Py(o) => format!("UTC{}{:02}:{:02}", if o < 0 { '-' } else { '+' }, o.abs() / 3600, o.abs() / 60 % 60),
        Tz::Naive => String::new(),
    }
}

fn dt_repr(out: &mut Vec<u8>, dt: &Dt) {
    let day = dt.local_us.div_euclid(86_400_000_000);
    let tod = dt.local_us.rem_euclid(86_400_000_000);
    if dt.nanos.is_some() {
        // pandas: Timestamp('YYYY-MM-DD HH:MM:SS[.f]+HHMM', tz='zone')
        let mut t = Vec::new();
        let naive = Dt { offset: None, ..*dt };
        text::dt_iso(&mut t, &naive, b' ', false);
        if let Some(o) = dt.offset {
            let a = o.unsigned_abs();
            t.extend_from_slice(format!("{}{:02}{:02}", if o < 0 { '-' } else { '+' }, a / 3600, a / 60 % 60).as_bytes());
        }
        out.extend_from_slice(b"Timestamp('");
        out.extend_from_slice(&t);
        out.push(b'\'');
        if dt.offset.is_some() {
            out.extend_from_slice(format!(", tz='{}'", pd_tz_name(dt.tz)).as_bytes());
        }
        out.push(b')');
        return;
    }
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

/// `json.dumps(_exact_numbers(p), sort_keys=True, default=str)`.
pub fn dumps(out: &mut Vec<u8>, p: &P) {
    match p {
        P::None => out.extend_from_slice(b"null"),
        P::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        P::Int(_) | P::BigInt(_) => py_repr(out, p),
        P::Float(f) => {
            if f.is_nan() {
                out.extend_from_slice(b"NaN");
            } else if f.is_infinite() {
                out.extend_from_slice(if *f > 0.0 { b"Infinity" } else { b"-Infinity" });
            } else {
                text::float_repr(out, *f);
            }
        }
        P::JNum(s) => {
            let d = text::decimal_of_number(s);
            let mut t = Vec::new();
            text::decimal_str(&mut t, &d);
            ascii_str(out, &t);
        }
        P::Str(s) => ascii_str(out, s),
        P::List(v) | P::Tuple(v) => {
            out.push(b'[');
            for (i, e) in v.iter().enumerate() {
                if i > 0 {
                    out.extend_from_slice(b", ");
                }
                dumps(out, e);
            }
            out.push(b']');
        }
        P::Mdn(..) => {
            let mut t = Vec::new();
            write(&mut t, p, 0).unwrap();
            // [m,d,n] with json.dumps' separators
            let s = String::from_utf8(t).unwrap().replace(',', ", ");
            out.extend_from_slice(s.as_bytes());
        }
        P::Dict(e) => {
            let mut idx: Vec<usize> = (0..e.len()).collect();
            idx.sort_by(|&a, &b| e[a].0.cmp(&e[b].0));
            out.push(b'{');
            for (i, j) in idx.into_iter().enumerate() {
                if i > 0 {
                    out.extend_from_slice(b", ");
                }
                ascii_str(out, &e[j].0);
                out.extend_from_slice(b": ");
                dumps(out, &e[j].1);
            }
            out.push(b'}');
        }
        other => {
            let mut t = Vec::new();
            py_str(&mut t, other);
            ascii_str(out, &t);
        }
    }
}

/// A JSON string as `json.dumps` writes it (`ensure_ascii`).
fn ascii_str(out: &mut Vec<u8>, s: &[u8]) {
    out.push(b'"');
    let mut i = 0;
    while i < s.len() {
        let (cp, n) = text::wtf8_char(&s[i..]);
        i += n;
        match cp {
            0x22 => out.extend_from_slice(b"\\\""),
            0x5C => out.extend_from_slice(b"\\\\"),
            0x0A => out.extend_from_slice(b"\\n"),
            0x0D => out.extend_from_slice(b"\\r"),
            0x09 => out.extend_from_slice(b"\\t"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x0C => out.extend_from_slice(b"\\f"),
            c if (0x20..0x7F).contains(&c) => out.push(c as u8),
            c if c < 0x10000 => out.extend_from_slice(format!("\\u{:04x}", c).as_bytes()),
            c => {
                let v = c - 0x10000;
                out.extend_from_slice(format!("\\u{:04x}\\u{:04x}", 0xD800 + (v >> 10), 0xDC00 + (v & 0x3FF)).as_bytes());
            }
        }
    }
    out.push(b'"');
}

/// Key atoms: equal exactly when Python finds the hashable values equal.
pub const K_NULL: u8 = 0;
pub const K_STR: u8 = 1;
pub const K_NUM: u8 = 2;
pub const K_NAN: u8 = 3;
pub const K_FLOAT: u8 = 4;
pub const K_OTHER: u8 = 5;

pub struct Keyer {
    nan: u64,
}

impl Default for Keyer {
    fn default() -> Self {
        Self::new()
    }
}

impl Keyer {
    pub fn new() -> Self {
        Keyer { nan: 0 }
    }

    pub fn str_atom(&self, key: &mut Vec<u8>, s: &[u8]) {
        key.push(K_STR);
        key.extend_from_slice(&(s.len() as u64).to_le_bytes());
        key.extend_from_slice(s);
    }

    fn num_atom(&self, key: &mut Vec<u8>, neg: bool, digits: &str, exp: i64) {
        key.push(K_NUM);
        key.push(neg as u8);
        key.extend_from_slice(&exp.to_le_bytes());
        key.extend_from_slice(&(digits.len() as u64).to_le_bytes());
        key.extend_from_slice(digits.as_bytes());
    }

    pub fn int_atom(&self, key: &mut Vec<u8>, i: i128) {
        let mut b = itoa::Buffer::new();
        let s = b.format(i.unsigned_abs());
        let t = s.trim_end_matches('0');
        if t.is_empty() {
            self.num_atom(key, false, "0", 0);
        } else {
            self.num_atom(key, i < 0, t, (s.len() - t.len()) as i64);
        }
    }

    pub fn float_atom(&mut self, key: &mut Vec<u8>, f: f64) {
        if f.is_nan() {
            // Each NaN is its own object: unequal to every other.
            self.nan += 1;
            key.push(K_NAN);
            key.extend_from_slice(&self.nan.to_le_bytes());
        } else if f.is_finite() && f.fract() == 0.0 && f.abs() < 1e38 {
            self.int_atom(key, f as i128);
        } else {
            key.push(K_FLOAT);
            key.extend_from_slice(&f.to_bits().to_le_bytes());
        }
    }

    /// `_to_hashable(p)` of a converted value, appended as an atom.
    pub fn atom(&mut self, key: &mut Vec<u8>, p: &P) {
        match p {
            P::None => key.push(K_NULL),
            P::Str(s) => self.str_atom(key, s),
            P::Bool(b) => self.int_atom(key, *b as i128),
            P::Int(i) => self.int_atom(key, *i),
            P::BigInt(s) => {
                let (neg, d) = match s.strip_prefix('-') {
                    Some(d) => (true, d),
                    None => (false, &s[..]),
                };
                let t = d.trim_end_matches('0');
                self.num_atom(key, neg, t, (d.len() - t.len()) as i64);
            }
            P::Float(f) => self.float_atom(key, *f),
            P::JNum(s) => {
                let d = text::decimal_of_number(s);
                let (neg, digits, exp) = text::decimal_canon(&d);
                self.num_atom(key, neg, &digits, exp);
            }
            P::Dec(d) => {
                let (neg, digits, exp) = text::decimal_canon(d);
                self.num_atom(key, neg, &digits, exp);
            }
            P::List(_) | P::Tuple(_) | P::Dict(_) => {
                let mut t = Vec::new();
                dumps(&mut t, p);
                self.str_atom(key, &t);
            }
            other => {
                // A raw value (an integer-typed column's non-integer, a uuid): equal by its kind
                // and its value as written.
                key.push(K_OTHER);
                let mut t = Vec::new();
                py_repr(&mut t, other);
                key.extend_from_slice(&(t.len() as u64).to_le_bytes());
                key.extend_from_slice(&t);
            }
        }
    }
}
