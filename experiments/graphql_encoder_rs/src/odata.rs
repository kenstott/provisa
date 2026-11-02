//! OData (REQ-2193): a read's rows as entities, each property by its Edm type (`edm.edm_value`)
//! from the statement's own values, each expansion from the JSON its subquery wrote
//! (`expand._decoded`, numbers exact), a list cut at its page with a nextLink to the rest; the
//! document `@odata.context`, `@odata.count`, `@Provisa.warnings`, `value`, `@odata.nextLink`.

use std::borrow::Cow;
use std::collections::HashMap;

use crate::fmt;
use crate::json;
use crate::nest::Batch;
use crate::pyval::{err, Dec, Dt, Error, Res, P};
use crate::pywrite::{self, MAX_DEPTH};
use crate::text;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Edm {
    Binary,
    Boolean,
    Date,
    DateTimeOffset,
    Decimal,
    Double,
    Guid,
    Int,
    Single,
    String,
    TimeOfDay,
}

impl Edm {
    pub fn parse(name: &str) -> Res<Edm> {
        Ok(match name {
            "Edm.Binary" => Edm::Binary,
            "Edm.Boolean" => Edm::Boolean,
            "Edm.Date" => Edm::Date,
            "Edm.DateTimeOffset" => Edm::DateTimeOffset,
            "Edm.Decimal" => Edm::Decimal,
            "Edm.Double" => Edm::Double,
            "Edm.Guid" => Edm::Guid,
            "Edm.Int16" | "Edm.Int32" | "Edm.Int64" | "Edm.SByte" => Edm::Int,
            "Edm.Single" => Edm::Single,
            "Edm.String" => Edm::String,
            "Edm.TimeOfDay" => Edm::TimeOfDay,
            other => return err(format!("KeyError: {other:?}")),
        })
    }
}

/// An entity set as one level of a read writes it.
pub struct Entity {
    pub name: String,
    pub types: HashMap<String, Edm>,
    pub fields: Vec<String>,
    pub plans: Vec<Nav>,
}

/// One expansion (`expand.Plan`).
pub struct Nav {
    pub name: String,
    pub collection: bool,
    pub page: usize,
    pub parent_key: Vec<String>,
    /// `page_link`'s query: `?` and the expansion's own options, its `$top` and `$skip` moved on.
    pub link_query: String,
    pub target: Entity,
}

pub struct Read {
    pub context: String,
    pub count: Option<i64>,
    pub warnings: Option<Vec<u8>>,
    pub page: usize,
    pub next_link: String,
    pub root: String,
    pub entity: Entity,
}

// ---------------------------------------------------------------- edm_value

fn base64url(b: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut s = String::with_capacity(b.len().div_ceil(3) * 4);
    for c in b.chunks(3) {
        let n = (c[0] as u32) << 16 | (*c.get(1).unwrap_or(&0) as u32) << 8 | *c.get(2).unwrap_or(&0) as u32;
        s.push(A[(n >> 18) as usize & 63] as char);
        s.push(A[(n >> 12) as usize & 63] as char);
        s.push(if c.len() > 1 { A[(n >> 6) as usize & 63] as char } else { '=' });
        s.push(if c.len() > 2 { A[n as usize & 63] as char } else { '=' });
    }
    s
}

/// `bytes.fromhex`: pairs of hex digits, whitespace allowed between them.
fn fromhex(s: &[u8]) -> Res<Vec<u8>> {
    let mut out = Vec::new();
    let mut i = 0;
    let hv = |c: u8| -> Option<u8> { (c as char).to_digit(16).map(|d| d as u8) };
    while i < s.len() {
        if s[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let (Some(h), Some(l)) = (hv(s[i]), s.get(i + 1).and_then(|&c| hv(c))) else {
            return err(format!("non-hexadecimal number found in fromhex() arg at position {i}"));
        };
        out.push(h << 4 | l);
        i += 2;
    }
    Ok(out)
}

fn type_error<T>(what: &str, p: &P) -> Res<T> {
    let mut t = Vec::new();
    pywrite::py_repr(&mut t, p);
    err(format!("{what}: {}", String::from_utf8_lossy(&t)))
}

/// Python's numeric text for `float()`, `int()` and `Decimal()`: whitespace stripped, `_`
/// between digits removed.
fn numeric_text(s: &[u8]) -> Option<String> {
    let t = std::str::from_utf8(s).ok()?.trim();
    if t.is_empty() {
        return None;
    }
    let b = t.as_bytes();
    let mut out = String::with_capacity(t.len());
    for (i, &c) in b.iter().enumerate() {
        if c == b'_' {
            let ok = i > 0 && i + 1 < b.len() && b[i - 1].is_ascii_digit() && b[i + 1].is_ascii_digit();
            if !ok {
                return None;
            }
            continue;
        }
        out.push(c as char);
    }
    Some(out)
}

/// `float(s)` of text.
fn float_of_text(s: &[u8]) -> Res<f64> {
    let bad = || err(format!("could not convert string to float: {:?}", String::from_utf8_lossy(s)));
    let Some(t) = numeric_text(s) else { return bad() };
    let l = t.to_ascii_lowercase();
    let (sign, body) = match l.strip_prefix('-') {
        Some(b) => (-1.0, b),
        None => (1.0, l.strip_prefix('+').unwrap_or(&l)),
    };
    match body {
        "nan" => return Ok(f64::NAN),
        "inf" | "infinity" => return Ok(sign * f64::INFINITY),
        _ => {}
    }
    let valid = {
        let b = body.as_bytes();
        let mut i = 0;
        let mut digits = 0;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            digits += 1;
        }
        if i < b.len() && b[i] == b'.' {
            i += 1;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
                digits += 1;
            }
        }
        let mut ok = digits > 0;
        if ok && i < b.len() && b[i] == b'e' {
            i += 1;
            if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
                i += 1;
            }
            let s0 = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            ok = i > s0;
        }
        ok && i == b.len()
    };
    if !valid {
        return bad();
    }
    Ok(sign * body.parse::<f64>().unwrap())
}

/// `int(s)` of text.
fn int_of_text(s: &[u8]) -> Res<P<'static>> {
    let bad = || err(format!("invalid literal for int() with base 10: {:?}", String::from_utf8_lossy(s)));
    let Some(t) = numeric_text(s) else { return bad() };
    let (neg, body) = match t.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, t.strip_prefix('+').unwrap_or(&t)),
    };
    if body.is_empty() || !body.bytes().all(|c| c.is_ascii_digit()) {
        return bad();
    }
    let d = body.trim_start_matches('0');
    let d = if d.is_empty() { "0" } else { d };
    let text = if neg && d != "0" { format!("-{d}") } else { d.to_string() };
    Ok(match text.parse::<i128>() {
        Ok(i) => P::Int(i),
        Err(_) => P::BigInt(Cow::Owned(text)),
    })
}

/// `Decimal(s)` of text: finite, or the special value's canonical text.
fn decimal_of_text(s: &[u8]) -> Res<P<'static>> {
    let bad = || err("decimal.InvalidOperation: [<class 'decimal.ConversionSyntax'>]");
    let Some(t) = numeric_text(s) else { return bad() };
    let l = t.to_ascii_lowercase();
    let (neg, body) = match l.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, l.strip_prefix('+').unwrap_or(&l)),
    };
    let sign = if neg { "-" } else { "" };
    match body {
        "inf" | "infinity" => return Ok(P::DecSpecial(Cow::Owned(format!("{sign}Infinity")))),
        "nan" => return Ok(P::DecSpecial(Cow::Owned(format!("{sign}NaN")))),
        "snan" => return Ok(P::DecSpecial(Cow::Owned(format!("{sign}sNaN")))),
        _ => {}
    }
    let b = body.as_bytes();
    let mut i = 0;
    let mut digits = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
        digits += 1;
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return bad();
    }
    if i < b.len() && b[i] == b'e' {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let s0 = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == s0 {
            return bad();
        }
    }
    if i != b.len() {
        return bad();
    }
    let mut d = text::decimal_of_number(body);
    d.neg = neg;
    Ok(P::Dec(d))
}

fn dec_to_f64(d: &Dec) -> f64 {
    let s = format!("{}{}e{}", if d.neg { "-" } else { "" }, d.coef, d.exp);
    s.parse::<f64>().unwrap()
}

/// `float(v)`.
fn to_float(p: &P) -> Res<f64> {
    Ok(match p {
        P::Int(i) => *i as f64,
        P::BigInt(s) => {
            let f: f64 = s.parse().unwrap();
            if f.is_infinite() {
                return err("OverflowError: int too large to convert to float");
            }
            f
        }
        P::Float(f) => *f,
        P::Bool(b) => *b as i32 as f64,
        P::Dec(d) => dec_to_f64(d),
        P::DecSpecial(s) => match s.as_ref() {
            "NaN" | "-NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            _ => return err("ValueError: cannot convert signaling NaN to float"),
        },
        P::Str(s) | P::Bytes(s) => float_of_text(s)?,
        other => return type_error("TypeError: float() argument", other),
    })
}

/// `int(v)`.
fn to_int(p: &P) -> Res<P<'static>> {
    Ok(match p {
        P::Int(i) => P::Int(*i),
        P::BigInt(s) => P::BigInt(Cow::Owned(s.to_string())),
        P::Bool(b) => P::Int(*b as i128),
        P::Float(f) => {
            if f.is_nan() {
                return err("ValueError: cannot convert float NaN to integer");
            }
            if f.is_infinite() {
                return err("OverflowError: cannot convert float infinity to integer");
            }
            float_trunc(*f)
        }
        P::Dec(d) => {
            let (neg, digits, exp) = text::decimal_canon(d);
            let s: String = if exp >= 0 {
                if digits == "0" {
                    "0".into()
                } else {
                    format!("{digits}{}", "0".repeat(exp as usize))
                }
            } else {
                let cut = digits.len() as i64 + exp;
                if cut <= 0 {
                    "0".into()
                } else {
                    digits[..cut as usize].to_string()
                }
            };
            let s = if neg && s != "0" { format!("-{s}") } else { s };
            match s.parse::<i128>() {
                Ok(i) => P::Int(i),
                Err(_) => P::BigInt(Cow::Owned(s)),
            }
        }
        P::DecSpecial(_) => return err("ValueError: cannot convert NaN to integer"),
        P::Str(s) | P::Bytes(s) => int_of_text(s)?,
        other => return type_error("TypeError: int() argument", other),
    })
}

fn float_trunc(f: f64) -> P<'static> {
    let t = f.trunc();
    if t.abs() < 1e38 {
        return P::Int(t as i128);
    }
    // the float's exact integer
    let s = format!("{:.0}", t);
    P::BigInt(Cow::Owned(s))
}

/// `Decimal(str(v))` (the Decimal itself when it is one).
fn to_decimal(p: &P) -> Res<P<'static>> {
    Ok(match p {
        P::Dec(d) => P::Dec(Dec { neg: d.neg, coef: Cow::Owned(d.coef.to_string()), exp: d.exp }),
        P::DecSpecial(s) => P::DecSpecial(Cow::Owned(s.to_string())),
        other => {
            let mut t = Vec::new();
            pywrite::py_str(&mut t, other);
            decimal_of_text(&t)?
        }
    })
}

fn floating(f: f64) -> P<'static> {
    if f.is_nan() {
        P::owned_str("NaN".into())
    } else if f.is_infinite() {
        P::owned_str(if f > 0.0 { "INF".into() } else { "-INF".into() })
    } else {
        P::Float(f)
    }
}

/// Python truthiness.
fn truthy(p: &P) -> bool {
    match p {
        P::None => false,
        P::Bool(b) => *b,
        P::Int(i) => *i != 0,
        P::BigInt(_) => true,
        P::Float(f) => *f != 0.0,
        P::JNum(s) => s.parse::<f64>().unwrap() != 0.0,
        P::Str(s) | P::Bytes(s) => !s.is_empty(),
        P::Dec(d) => text::decimal_canon(d).1 != "0",
        P::Td(us, n) => *us != 0 || n.is_some_and(|n| n != 0),
        P::Iv(m, d, u) => (*m, *d, *u) != (0, 0, 0),
        P::List(v) | P::Tuple(v) => !v.is_empty(),
        P::Dict(e) => !e.is_empty(),
        _ => true,
    }
}

/// A datetime as `datetime(v.year, v.month, v.day)` or itself.
fn as_datetime(p: &P) -> Res<Option<Dt>> {
    Ok(Some(match p {
        P::Dt(dt) => *dt,
        P::InfDt(s, a) => text::inf_dt(*s, *a),
        P::NaT => return Ok(None),
        P::Date(d) => Dt { local_us: *d as i64 * 86_400_000_000, offset: None, nanos: None },
        P::InfDate(s) => {
            let d = if *s > 0 { 2_932_896 } else { -719_162 };
            Dt { local_us: d * 86_400_000_000, offset: None, nanos: None }
        }
        P::Str(s) => fromisoformat(s)?,
        other => return type_error("AttributeError: no year", other),
    }))
}

/// `edm.edm_value(value, edm)`, as the value orjson then writes (a Decimal's Fragment as `JNum`).
pub fn edm_value(p: &P, edm: Edm) -> Res<P<'static>> {
    if p.is_none() {
        return Ok(P::None);
    }
    Ok(match edm {
        Edm::Binary => {
            let b: Vec<u8> = match p {
                P::Str(s) if s.starts_with(b"\\x") => fromhex(&s[2..])?,
                P::Str(s) => {
                    if json::has_surrogate(s) {
                        return err("UnicodeEncodeError: surrogates not allowed");
                    }
                    s.to_vec()
                }
                P::Bytes(b) => b.to_vec(),
                P::Int(n) if *n >= 0 => vec![0; *n as usize],
                P::Bool(b) => vec![0; *b as usize],
                P::List(v) => {
                    let mut out = Vec::new();
                    for x in v {
                        match x {
                            P::Int(i) if (0..256).contains(i) => out.push(*i as u8),
                            _ => return err("ValueError: bytes must be in range(0, 256)"),
                        }
                    }
                    out
                }
                other => return type_error("TypeError: cannot convert to bytes", other),
            };
            P::owned_str(base64url(&b))
        }
        Edm::Boolean => P::Bool(truthy(p)),
        Edm::Date => match p {
            P::Dt(dt) => P::owned_str({
                let mut t = Vec::new();
                text::date_iso(&mut t, dt.local_us.div_euclid(86_400_000_000));
                String::from_utf8(t).unwrap()
            }),
            P::InfDt(s, _) => P::owned_str(if *s > 0 { "9999-12-31".into() } else { "0001-01-01".into() }),
            P::NaT => P::owned_str("NaT".into()),
            P::Date(d) => {
                let mut t = Vec::new();
                text::date_iso(&mut t, *d as i64);
                P::Str(Cow::Owned(t))
            }
            P::InfDate(s) => {
                let mut t = Vec::new();
                text::inf_date_iso(&mut t, *s);
                P::Str(Cow::Owned(t))
            }
            other => other.clone().into_owned(),
        },
        Edm::DateTimeOffset => match as_datetime(p)? {
            None => P::owned_str("NaT".into()),
            Some(mut dt) => {
                if dt.offset.is_none() {
                    dt.offset = Some(0);
                }
                let mut t = Vec::new();
                text::dt_iso(&mut t, &dt, b'T', false);
                let s = String::from_utf8(t).unwrap().replace("+00:00", "Z");
                P::owned_str(s)
            }
        },
        Edm::Decimal => match to_decimal(p)? {
            P::Dec(d) => {
                let mut t = Vec::new();
                text::decimal_f(&mut t, &d);
                P::JNum(Cow::Owned(String::from_utf8(t).unwrap()))
            }
            special => floating(to_float(&special)?),
        },
        Edm::Double | Edm::Single => floating(to_float(p)?),
        Edm::Guid => {
            let mut t = Vec::new();
            pywrite::py_str(&mut t, p);
            P::Str(Cow::Owned(t))
        }
        Edm::Int => to_int(p)?,
        Edm::String => match p {
            P::Str(s) => P::Str(Cow::Owned(s.to_vec())),
            P::Dict(_) | P::List(_) => {
                let mut t = Vec::new();
                orjson_default_str(&mut t, p, 0)?;
                P::Str(Cow::Owned(t))
            }
            other => {
                let mut t = Vec::new();
                pywrite::py_str(&mut t, other);
                P::Str(Cow::Owned(t))
            }
        },
        Edm::TimeOfDay => match p {
            P::Time(us) => {
                let mut t = Vec::new();
                text::time_iso(&mut t, *us, false);
                P::Str(Cow::Owned(t))
            }
            other => other.clone().into_owned(),
        },
    })
}

/// `orjson.dumps(v, default=str)`: orjson's own types natively, `str()` of the rest; an int past
/// 64 bits, a lone surrogate or nesting past orjson's limit raise.
fn orjson_default_str(out: &mut Vec<u8>, p: &P, depth: usize) -> Res<()> {
    match p {
        P::None | P::Bool(_) | P::Float(_) | P::Date(_) | P::Time(_) | P::Uuid(_) => pywrite::write(out, p, depth)?,
        P::Int(i) => {
            if *i < i64::MIN as i128 || *i > u64::MAX as i128 {
                return err("JSONEncodeError: Integer exceeds 64-bit range");
            }
            pywrite::write(out, p, depth)?
        }
        P::BigInt(_) => return err("JSONEncodeError: Integer exceeds 64-bit range"),
        P::Dt(dt) if dt.nanos.is_none() => pywrite::write(out, p, depth)?,
        P::Str(s) => {
            if json::has_surrogate(s) {
                return err("JSONEncodeError: str is not valid UTF-8: surrogates not allowed");
            }
            fmt::write_str(out, s)
        }
        P::List(v) | P::Tuple(v) => {
            if depth + 1 > MAX_DEPTH && !v.is_empty() {
                return err("JSONEncodeError: Recursion limit reached");
            }
            out.push(b'[');
            for (i, e) in v.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                orjson_default_str(out, e, depth + 1)?;
            }
            out.push(b']');
        }
        P::Dict(e) => {
            if depth + 1 > MAX_DEPTH && !e.is_empty() {
                return err("JSONEncodeError: Recursion limit reached");
            }
            out.push(b'{');
            for (i, (k, v)) in e.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                if json::has_surrogate(k) {
                    return err("JSONEncodeError: str is not valid UTF-8: surrogates not allowed");
                }
                fmt::write_str(out, k);
                out.push(b':');
                orjson_default_str(out, v, depth + 1)?;
            }
            out.push(b'}');
        }
        other => {
            let mut t = Vec::new();
            pywrite::py_str(&mut t, other);
            fmt::write_str(out, &t);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- datetime.fromisoformat

fn digits(s: &[u8], at: usize, n: usize) -> Option<i64> {
    let d = s.get(at..at + n)?;
    if !d.iter().all(u8::is_ascii_digit) {
        return None;
    }
    Some(d.iter().fold(0i64, |a, &c| a * 10 + (c - b'0') as i64))
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn month_days(y: i64, m: i64) -> i64 {
    match m {
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// The date part: (days, its length). `YYYY-MM-DD`, `YYYYMMDD`, `YYYY-Www[-D]`, `YYYYWww[D]`.
fn iso_date(s: &[u8]) -> Option<(i64, usize)> {
    let y = digits(s, 0, 4)?;
    let ext = s.get(4) == Some(&b'-');
    let at = if ext { 5 } else { 4 };
    if s.get(at) == Some(&b'W') {
        let w = digits(s, at + 1, 2)?;
        let mut len = at + 3;
        let mut wd = 1;
        let dsep = if ext { 1 } else { 0 };
        if (ext && s.get(len) == Some(&b'-') && s.get(len + 1).is_some_and(u8::is_ascii_digit)) || (!ext && s.get(len).is_some_and(u8::is_ascii_digit)) {
            wd = digits(s, len + dsep, 1)?;
            len += dsep + 1;
        }
        if !(1..=53).contains(&w) || !(1..=7).contains(&wd) {
            return None;
        }
        // ISO week 1 holds January 4th.
        let jan4 = days_from_civil(y, 1, 4);
        let wd_jan4 = (jan4 + 3).rem_euclid(7); // Monday 0
        let week1 = jan4 - wd_jan4;
        let d = week1 + (w - 1) * 7 + (wd - 1);
        if w == 53 {
            let next_week1 = {
                let j = days_from_civil(y + 1, 1, 4);
                j - (j + 3).rem_euclid(7)
            };
            if d >= next_week1 {
                return None;
            }
        }
        return Some((d, len));
    }
    let (m, d, len) = if ext {
        if s.get(7) != Some(&b'-') {
            return None;
        }
        (digits(s, 5, 2)?, digits(s, 8, 2)?, 10)
    } else {
        (digits(s, 4, 2)?, digits(s, 6, 2)?, 8)
    };
    if !(1..=9999).contains(&y) || !(1..=12).contains(&m) || d < 1 || d > month_days(y, m) {
        return None;
    }
    Some((days_from_civil(y, m, d), len))
}

/// `HH[:MM[:SS[.f+]]]` or `HH[MM[SS[.f+]]]`: (µs of the day, its length).
fn iso_time(s: &[u8]) -> Option<(i64, usize)> {
    let h = digits(s, 0, 2)?;
    let mut i = 2;
    let ext = s.get(2) == Some(&b':');
    let mut parts = [h, 0, 0];
    for part in parts.iter_mut().skip(1) {
        let at = if ext { i + 1 } else { i };
        if ext && s.get(i) != Some(&b':') {
            break;
        }
        match digits(s, at, 2) {
            Some(v) => {
                *part = v;
                i = at + 2;
            }
            None => {
                if ext {
                    return None;
                }
                break;
            }
        }
    }
    let mut us = 0;
    if matches!(s.get(i), Some(b'.' | b',')) {
        let st = i + 1;
        let mut j = st;
        while s.get(j).is_some_and(u8::is_ascii_digit) {
            j += 1;
        }
        if j == st {
            return None;
        }
        let f = &s[st..j];
        let mut v = 0i64;
        for k in 0..6 {
            v = v * 10 + f.get(k).map_or(0, |c| (c - b'0') as i64);
        }
        us = v;
        i = j;
    }
    let [h, m, sec] = parts;
    if h > 23 || m > 59 || sec > 59 {
        return None;
    }
    Some((((h * 60 + m) * 60 + sec) * 1_000_000 + us, i))
}

/// `datetime.fromisoformat(s)` (Python 3.12).
pub fn fromisoformat(s: &[u8]) -> Res<Dt> {
    let bad = || Error::Value(format!("Invalid isoformat string: {:?}", String::from_utf8_lossy(s)));
    let (days, dlen) = iso_date(s).ok_or_else(bad)?;
    if dlen == s.len() {
        return Ok(Dt { local_us: days * 86_400_000_000, offset: None, nanos: None });
    }
    // any one character separates the date from the time
    let (_, sep) = text::wtf8_char(&s[dlen..]);
    let rest = &s[dlen + sep..];
    let tz_at = rest.iter().position(|&c| c == b'+' || c == b'-' || c == b'Z');
    let (tpart, tz) = match tz_at {
        Some(p) => (&rest[..p], Some(&rest[p..])),
        None => (rest, None),
    };
    let (us, tlen) = iso_time(tpart).ok_or_else(bad)?;
    if tlen != tpart.len() {
        return Err(bad());
    }
    let offset = match tz {
        None => None,
        Some(b"Z") => Some(0),
        Some(z) => {
            let sign = if z[0] == b'-' { -1 } else { 1 };
            let (v, n) = iso_time(&z[1..]).ok_or_else(bad)?;
            if n != z.len() - 1 || v % 1_000_000 != 0 {
                return Err(bad());
            }
            Some(sign * (v / 1_000_000) as i32)
        }
    };
    Ok(Dt { local_us: days * 86_400_000_000 + us, offset, nanos: None })
}

// ---------------------------------------------------------------- entities

/// The expansion's raw value: JSON text read with exact numbers, or what the engine gave.
fn decoded(p: P) -> Res<P> {
    match p {
        P::Str(s) => json::parse_decimal_text(&s),
        other => Ok(other),
    }
}

fn get<'v, 'a>(values: &'v [(Cow<'a, [u8]>, P<'a>)], name: &str) -> Res<&'v P<'a>> {
    values.iter().rev().find(|e| e.0.as_ref() == name.as_bytes()).map(|e| &e.1).ok_or_else(|| Error::Value(format!("KeyError: '{name}'")))
}

/// `expand._key_literal`.
fn key_literal(out: &mut String, value: &P, edm: Edm) -> Res<()> {
    let written = edm_value(value, edm)?;
    let mut t = Vec::new();
    if edm == Edm::String {
        pywrite::py_str(&mut t, &written);
        let s = String::from_utf8_lossy(&t).replace('\'', "''");
        out.push('\'');
        out.push_str(&s);
        out.push('\'');
        return Ok(());
    }
    match &written {
        P::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        P::JNum(_) => {
            let P::Dec(d) = to_decimal(&{
                let mut v = Vec::new();
                pywrite::py_str(&mut v, value);
                P::Str(Cow::Owned(v))
            })?
            else {
                return err("decimal key is not finite");
            };
            text::decimal_f(&mut t, &d);
            out.push_str(std::str::from_utf8(&t).unwrap());
        }
        other => {
            pywrite::py_str(&mut t, other);
            out.push_str(&String::from_utf8_lossy(&t));
        }
    }
    Ok(())
}

/// `urllib.parse.quote(s, safe="(),='")`.
fn quote(s: &str) -> String {
    let mut out = String::new();
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' | b'(' | b')' | b',' | b'=' | b'\'' => out.push(b as char),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

struct Writer<'r> {
    root: &'r str,
}

impl Writer<'_> {
    /// `expand.write`: one row as an entity at nesting level `depth`.
    fn entity(&self, out: &mut Vec<u8>, values: &[(Cow<[u8]>, P)], e: &Entity, depth: usize) -> Res<()> {
        let mut written: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut set = |k: Vec<u8>, v: Vec<u8>| match written.iter_mut().find(|x| x.0 == k) {
            Some(x) => x.1 = v,
            None => written.push((k, v)),
        };
        for name in &e.fields {
            let edm = *e.types.get(name).ok_or_else(|| Error::Value(format!("KeyError: '{name}'")))?;
            let v = edm_value(get(values, name)?, edm)?;
            let mut b = Vec::new();
            pywrite::write(&mut b, &v, depth)?;
            set(name.as_bytes().to_vec(), b);
        }
        for plan in &e.plans {
            let name = &plan.name;
            if !plan.collection {
                let found = decoded(get(values, name)?.clone())?;
                let mut b = Vec::new();
                match found {
                    P::None => b.extend_from_slice(b"null"),
                    P::Dict(entries) => {
                        if depth + 1 > MAX_DEPTH {
                            return Err(pywrite::recursion());
                        }
                        self.entity(&mut b, &entries, &plan.target, depth + 1)?
                    }
                    other => return type_error("TypeError: not a mapping", &other),
                }
                set(name.as_bytes().to_vec(), b);
                continue;
            }
            let rows = if plan.page > 0 {
                match decoded(get(values, name)?.clone())? {
                    P::List(v) => v,
                    other => return type_error("TypeError: not a list", &other),
                }
            } else {
                Vec::new()
            };
            let mut b = Vec::new();
            if depth + 1 > MAX_DEPTH && !rows.is_empty() && plan.page > 0 {
                return Err(pywrite::recursion());
            }
            b.push(b'[');
            for (i, row) in rows.iter().take(plan.page).enumerate() {
                if i > 0 {
                    b.push(b',');
                }
                let P::Dict(entries) = row else { return type_error("TypeError: not a mapping", row) };
                if depth + 2 > MAX_DEPTH {
                    return Err(pywrite::recursion());
                }
                self.entity(&mut b, entries, &plan.target, depth + 2)?;
            }
            b.push(b']');
            set(name.as_bytes().to_vec(), b);
            if rows.len() > plan.page {
                let link = self.link(values, e, plan)?;
                let mut lb = Vec::new();
                fmt::write_str(&mut lb, link.as_bytes());
                set(format!("{name}@odata.nextLink").into_bytes(), lb);
            }
        }
        out.push(b'{');
        for (i, (k, v)) in written.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            pywrite::write_text(out, k)?;
            out.push(b':');
            out.extend_from_slice(v);
        }
        out.push(b'}');
        Ok(())
    }

    /// `expand._link`.
    fn link(&self, values: &[(Cow<[u8]>, P)], e: &Entity, plan: &Nav) -> Res<String> {
        if plan.parent_key.is_empty() {
            return Err(Error::ODataRefused(
                501,
                "not_implemented".into(),
                format!(
                    "{} of {} holds more than {} rows, and a row of {} has no key this role can address the rest by",
                    plan.name, e.name, plan.page, e.name
                ),
            ));
        }
        let mut address = format!("{}(", e.name);
        for (i, k) in plan.parent_key.iter().enumerate() {
            let edm = *e.types.get(k).ok_or_else(|| Error::Value(format!("KeyError: '{k}'")))?;
            if plan.parent_key.len() > 1 {
                if i > 0 {
                    address.push(',');
                }
                address.push_str(k);
                address.push('=');
            }
            key_literal(&mut address, get(values, k)?, edm)?;
        }
        address.push(')');
        Ok(format!("{}/{}/{}{}", self.root, quote(&address), plan.name, plan.link_query))
    }
}

/// The document of an entity set read (`router._read`).
pub fn read(batches: &[Batch], names: &[String], r: &Read) -> Res<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"{\"@odata.context\":");
    pywrite::write_text(&mut out, r.context.as_bytes())?;
    if let Some(c) = r.count {
        out.extend_from_slice(b",\"@odata.count\":");
        out.extend_from_slice(c.to_string().as_bytes());
    }
    if r.page == 0 {
        out.extend_from_slice(b",\"value\":[]}");
        return Ok(out);
    }
    if let Some(w) = &r.warnings {
        out.extend_from_slice(b",\"@Provisa.warnings\":");
        out.extend_from_slice(w);
    }
    out.extend_from_slice(b",\"value\":[");
    let w = Writer { root: &r.root };
    let mut n = 0usize;
    let mut values: Vec<(Cow<[u8]>, P)> = Vec::with_capacity(names.len());
    'outer: for b in batches {
        for row in 0..b.rows {
            if n == r.page {
                n += 1;
                break 'outer;
            }
            values.clear();
            for (name, c) in names.iter().zip(&b.cells) {
                values.push((Cow::Borrowed(name.as_bytes()), c.r.get(row)?));
            }
            if n > 0 {
                out.push(b',');
            }
            w.entity(&mut out, &values, &r.entity, 3)?;
            n += 1;
        }
    }
    out.push(b']');
    if n > r.page {
        out.extend_from_slice(b",\"@odata.nextLink\":");
        pywrite::write_text(&mut out, r.next_link.as_bytes())?;
    }
    out.push(b'}');
    Ok(out)
}
