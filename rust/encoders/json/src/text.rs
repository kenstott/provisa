//! The text Python gives a value: `format(Decimal, "f")` and `str(Decimal)`, `repr(float)`,
//! `isoformat` (and `temporal_text`, which drops a fraction's trailing zeros), orjson's own
//! datetime spelling, `str(timedelta)`, pandas' `Timedelta` and `Timestamp`, `relativedelta`'s
//! repr, and `repr(str)` / `repr(bytes)`.

use crate::fmt::{self, civil};
use crate::printable::is_printable;
use crate::pyval::{Dec, Dt, Tz};

const DAY_US: i64 = 86_400_000_000;

fn push2(out: &mut Vec<u8>, v: u32) {
    out.push(b'0' + (v / 10) as u8);
    out.push(b'0' + (v % 10) as u8);
}

fn push_int(out: &mut Vec<u8>, v: i128) {
    let mut b = itoa::Buffer::new();
    out.extend_from_slice(b.format(v).as_bytes());
}

// ---------------------------------------------------------------- Decimal

/// `format(d, "f")`: positional, never an exponent (`decimal_text`).
pub fn decimal_f(out: &mut Vec<u8>, d: &Dec) {
    if d.neg {
        out.push(b'-');
    }
    let c = d.coef.as_bytes();
    if d.exp >= 0 {
        out.extend_from_slice(c);
        if c != b"0" {
            out.extend(std::iter::repeat(b'0').take(d.exp as usize));
        }
        return;
    }
    let frac = (-d.exp) as usize;
    if c.len() > frac {
        out.extend_from_slice(&c[..c.len() - frac]);
        out.push(b'.');
        out.extend_from_slice(&c[c.len() - frac..]);
    } else {
        out.extend_from_slice(b"0.");
        out.extend(std::iter::repeat(b'0').take(frac - c.len()));
        out.extend_from_slice(c);
    }
}

/// `str(d)`: the to-scientific-string of the decimal specification.
pub fn decimal_str(out: &mut Vec<u8>, d: &Dec) {
    if d.neg {
        out.push(b'-');
    }
    let c = d.coef.as_bytes();
    let n = c.len() as i64;
    let exp = d.exp as i64;
    let adjusted = exp + n - 1;
    if exp <= 0 && adjusted >= -6 {
        if exp == 0 {
            out.extend_from_slice(c);
        } else {
            let frac = (-exp) as usize;
            if c.len() > frac {
                out.extend_from_slice(&c[..c.len() - frac]);
                out.push(b'.');
                out.extend_from_slice(&c[c.len() - frac..]);
            } else {
                out.extend_from_slice(b"0.");
                out.extend(std::iter::repeat(b'0').take(frac - c.len()));
                out.extend_from_slice(c);
            }
        }
        return;
    }
    out.push(c[0]);
    if c.len() > 1 {
        out.push(b'.');
        out.extend_from_slice(&c[1..]);
    }
    out.push(b'E');
    out.push(if adjusted < 0 { b'-' } else { b'+' });
    push_int(out, adjusted.unsigned_abs() as i128);
}

/// A Decimal of number text: `Decimal(text)` for text `json.loads` scanned as a number.
pub fn decimal_of_number(text: &str) -> Dec<'static> {
    let (neg, body) = match text.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, text),
    };
    let (mant, e) = match body.find(['e', 'E']) {
        Some(p) => (&body[..p], body[p + 1..].parse::<i64>().unwrap_or(0)),
        None => (body, 0),
    };
    let (int, frac) = match mant.find('.') {
        Some(p) => (&mant[..p], &mant[p + 1..]),
        None => (mant, ""),
    };
    let mut digits: String = format!("{int}{frac}");
    let trimmed = digits.trim_start_matches('0');
    digits = if trimmed.is_empty() { "0".into() } else { trimmed.into() };
    let exp = e - frac.len() as i64;
    Dec { neg, coef: digits.into(), exp: exp.clamp(i32::MIN as i64, i32::MAX as i64) as i32 }
}

/// The exact value of a Decimal as (sign, digits without trailing zeros, exponent), zero as
/// (false, "0", 0): equal exactly when the values are.
pub fn decimal_canon(d: &Dec) -> (bool, String, i64) {
    let c = d.coef.trim_start_matches('0');
    if c.is_empty() {
        return (false, "0".into(), 0);
    }
    let t = c.trim_end_matches('0');
    (d.neg, t.to_string(), d.exp as i64 + (c.len() - t.len()) as i64)
}

// ---------------------------------------------------------------- float

/// `repr(float)`: the shortest round-trip digits; fixed for a decimal exponent in [-4, 16), else
/// `d[.ddd]e±XX`; `nan`, `inf`, `-inf`.
pub fn float_repr(out: &mut Vec<u8>, f: f64) {
    if f.is_nan() {
        out.extend_from_slice(b"nan");
        return;
    }
    if f.is_infinite() {
        out.extend_from_slice(if f > 0.0 { b"inf" } else { b"-inf" });
        return;
    }
    let (neg, d, e10) = fmt::shortest(f);
    if neg {
        out.push(b'-');
    }
    if d == b"0" {
        out.extend_from_slice(b"0.0");
        return;
    }
    let sci = e10 - 1;
    if (-4..16).contains(&sci) {
        fmt::fixed(out, &d, e10);
    } else {
        out.push(d[0]);
        if d.len() > 1 {
            out.push(b'.');
            out.extend_from_slice(&d[1..]);
        }
        out.push(b'e');
        out.push(if sci < 0 { b'-' } else { b'+' });
        let a = sci.unsigned_abs();
        if a < 10 {
            out.push(b'0');
        }
        push_int(out, a as i128);
    }
}

// ---------------------------------------------------------------- dates and times

pub fn date_iso(out: &mut Vec<u8>, days: i64) {
    let (y, m, d) = civil(days);
    let y = y as u32;
    push2(out, y / 100);
    push2(out, y % 100);
    out.push(b'-');
    push2(out, m);
    out.push(b'-');
    push2(out, d);
}

/// `HH:MM:SS` then the fraction (`.ffffff` when non-zero); `strip` drops its trailing zeros
/// (`temporal_text`).
fn clock(out: &mut Vec<u8>, us_of_day: i64, strip: bool) {
    let secs = us_of_day / 1_000_000;
    let frac = (us_of_day % 1_000_000) as u64;
    push2(out, (secs / 3600) as u32);
    out.push(b':');
    push2(out, ((secs / 60) % 60) as u32);
    out.push(b':');
    push2(out, (secs % 60) as u32);
    let (val, width) = (frac, 6);
    if val == 0 {
        return;
    }
    out.push(b'.');
    let mut ds = [b'0'; 9];
    let mut v = val;
    for i in (0..width).rev() {
        ds[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    let mut end = width;
    if strip {
        while ds[end - 1] == b'0' {
            end -= 1;
        }
    }
    out.extend_from_slice(&ds[..end]);
}

fn offset_text(out: &mut Vec<u8>, off: i32) {
    out.push(if off < 0 { b'-' } else { b'+' });
    let a = off.unsigned_abs();
    push2(out, a / 3600);
    out.push(b':');
    push2(out, (a / 60) % 60);
    if a % 60 != 0 {
        out.push(b':');
        push2(out, a % 60);
    }
}

/// `datetime.isoformat(sep)`; `strip` drops the fraction's trailing zeros as `temporal_text` does.
pub fn dt_iso(out: &mut Vec<u8>, dt: &Dt, sep: u8, strip: bool) {
    let day = dt.local_us.div_euclid(DAY_US);
    let tod = dt.local_us.rem_euclid(DAY_US);
    date_iso(out, day);
    out.push(sep);
    clock(out, tod, strip);
    if let Some(o) = dt.offset {
        offset_text(out, o);
    }
}

pub fn time_iso(out: &mut Vec<u8>, us: i64, strip: bool) {
    clock(out, us, strip);
}

/// The IR infinite date's and timestamp's edge values, as `isoformat` writes them.
pub fn inf_date_iso(out: &mut Vec<u8>, sign: i8) {
    out.extend_from_slice(if sign > 0 { b"9999-12-31" } else { b"0001-01-01" });
}

pub fn inf_dt(sign: i8, aware: bool) -> Dt {
    let local_us = if sign > 0 { 253_402_300_799_999_999 } else { -62_135_596_800_000_000 };
    Dt { local_us, offset: aware.then_some(0), tz: if aware { Tz::Py(0) } else { Tz::Naive } }
}

// ---------------------------------------------------------------- durations

/// `str(timedelta)`: `[-]D day[s], H:MM:SS[.ffffff]` with floor days.
pub fn td_str(out: &mut Vec<u8>, us: i128) {
    let days = us.div_euclid(DAY_US as i128);
    let rem = us.rem_euclid(DAY_US as i128) as i64;
    if days != 0 {
        push_int(out, days);
        out.extend_from_slice(if days.abs() == 1 { b" day, " } else { b" days, " });
    }
    let secs = rem / 1_000_000;
    let frac = rem % 1_000_000;
    push_int(out, (secs / 3600) as i128);
    out.push(b':');
    push2(out, ((secs / 60) % 60) as u32);
    out.push(b':');
    push2(out, (secs % 60) as u32);
    if frac != 0 {
        out.extend_from_slice(format!(".{:06}", frac).as_bytes());
    }
}

/// `interval_parts` of a timedelta of `us` microseconds: the magnitude's (days, micros), signed.
pub fn td_parts(us: i128) -> (i64, i64) {
    let a = us.abs();
    let days = a / DAY_US as i128;
    let rem = a % DAY_US as i128;
    if us < 0 {
        (-(days as i64), -(rem as i64))
    } else {
        (days as i64, rem as i64)
    }
}

/// The IR interval (`interval_value`) of (months, days, micros): a timedelta, a relativedelta, or
/// an `_ExactInterval` (a relativedelta that keeps PostgreSQL's fields).
pub enum IvKind {
    Td(i128),
    Rd,
    Exact,
}

pub fn iv_kind(months: i64, days: i64, micros: i64) -> IvKind {
    if days.abs() <= 999_999_999 && months == 0 {
        let us = days as i128 * DAY_US as i128 + micros as i128;
        let (d, u) = td_parts(us);
        if (d, u) == (days, micros) {
            return IvKind::Td(us);
        }
        return IvKind::Exact;
    }
    // relativedelta(months, days, microseconds) normalizes microseconds into hours and leaves
    // days apart: its parts are the same unless the microseconds carry into days.
    let rd = Rd::new(months, days, micros);
    let secs = (rd.hours * 60 + rd.minutes) * 60 + rd.seconds;
    let parts = (rd.years * 12 + rd.months, rd.days, secs * 1_000_000 + rd.micros);
    if parts == (months, days, micros) {
        IvKind::Rd
    } else {
        IvKind::Exact
    }
}

/// dateutil's relativedelta after `_fix`.
pub struct Rd {
    pub years: i64,
    pub months: i64,
    pub days: i64,
    pub hours: i64,
    pub minutes: i64,
    pub seconds: i64,
    pub micros: i64,
}

fn carry(v: &mut i64, up: &mut i64, limit: i64, unit: i64) {
    if v.abs() > limit {
        let s = v.signum();
        let (div, m) = ((*v * s).div_euclid(unit), (*v * s).rem_euclid(unit));
        *v = m * s;
        *up += div * s;
    }
}

impl Rd {
    pub fn new(months: i64, days: i64, micros: i64) -> Rd {
        let mut r = Rd { years: 0, months, days, hours: 0, minutes: 0, seconds: 0, micros };
        carry(&mut r.micros, &mut r.seconds, 999_999, 1_000_000);
        carry(&mut r.seconds, &mut r.minutes, 59, 60);
        carry(&mut r.minutes, &mut r.hours, 59, 60);
        carry(&mut r.hours, &mut r.days, 23, 24);
        carry(&mut r.months, &mut r.years, 11, 12);
        r
    }

    /// `repr`: `Name(years=+1, months=+2, ...)`, each value with `{:+g}`.
    pub fn repr(&self, out: &mut Vec<u8>, name: &str) {
        out.extend_from_slice(name.as_bytes());
        out.push(b'(');
        let mut first = true;
        for (k, v) in [
            ("years", self.years),
            ("months", self.months),
            ("days", self.days),
            ("hours", self.hours),
            ("minutes", self.minutes),
            ("seconds", self.seconds),
            ("microseconds", self.micros),
        ] {
            if v != 0 {
                if !first {
                    out.extend_from_slice(b", ");
                }
                first = false;
                out.extend_from_slice(k.as_bytes());
                out.push(b'=');
                plus_g(out, v);
            }
        }
        out.push(b')');
    }
}

/// `format(v, "+g")` of an int: six significant digits, an exponent from 1e6.
fn plus_g(out: &mut Vec<u8>, v: i64) {
    out.push(if v < 0 { b'-' } else { b'+' });
    let a = v.unsigned_abs();
    if a < 1_000_000 {
        push_int(out, a as i128);
        return;
    }
    let s = format!("{:.5e}", a as f64);
    // "1.23457e6" -> mantissa without trailing zeros, exponent two digits with sign.
    let (m, e) = s.split_once('e').unwrap();
    let m = if m.contains('.') { m.trim_end_matches('0').trim_end_matches('.') } else { m };
    out.extend_from_slice(m.as_bytes());
    out.extend_from_slice(format!("e+{:02}", e.parse::<i32>().unwrap()).as_bytes());
}

// ---------------------------------------------------------------- repr

/// `repr(str)` of WTF-8 text.
pub fn str_repr(out: &mut Vec<u8>, s: &[u8]) {
    let has_sq = s.contains(&b'\'');
    let has_dq = s.contains(&b'"');
    let q = if has_sq && !has_dq { b'"' } else { b'\'' };
    out.push(q);
    let mut i = 0;
    while i < s.len() {
        let (cp, n) = wtf8_char(&s[i..]);
        let raw = &s[i..i + n];
        i += n;
        match cp {
            0x5C => out.extend_from_slice(b"\\\\"),
            0x0A => out.extend_from_slice(b"\\n"),
            0x0D => out.extend_from_slice(b"\\r"),
            0x09 => out.extend_from_slice(b"\\t"),
            c if c == q as u32 => {
                out.push(b'\\');
                out.push(q);
            }
            c if c < 0x80 && is_printable(c) => out.push(c as u8),
            c if c >= 0x80 && is_printable(c) => out.extend_from_slice(raw),
            c if c < 0x100 => out.extend_from_slice(format!("\\x{:02x}", c).as_bytes()),
            c if c < 0x10000 => out.extend_from_slice(format!("\\u{:04x}", c).as_bytes()),
            c => out.extend_from_slice(format!("\\U{:08x}", c).as_bytes()),
        }
    }
    out.push(q);
}

/// `repr(bytes)`: `b'...'`.
pub fn bytes_repr(out: &mut Vec<u8>, s: &[u8]) {
    let q = if s.contains(&b'\'') && !s.contains(&b'"') { b'"' } else { b'\'' };
    out.push(b'b');
    out.push(q);
    for &c in s {
        match c {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            c if c == q => {
                out.push(b'\\');
                out.push(q);
            }
            0x20..=0x7e => out.push(c),
            c => out.extend_from_slice(format!("\\x{:02x}", c).as_bytes()),
        }
    }
    out.push(q);
}

/// The code point at the start of WTF-8 text and its length in bytes.
pub fn wtf8_char(s: &[u8]) -> (u32, usize) {
    let b = s[0];
    if b < 0x80 {
        (b as u32, 1)
    } else if b < 0xE0 {
        (((b as u32 & 0x1F) << 6) | (s[1] as u32 & 0x3F), 2)
    } else if b < 0xF0 {
        (((b as u32 & 0x0F) << 12) | ((s[1] as u32 & 0x3F) << 6) | (s[2] as u32 & 0x3F), 3)
    } else {
        (
            ((b as u32 & 0x07) << 18) | ((s[1] as u32 & 0x3F) << 12) | ((s[2] as u32 & 0x3F) << 6) | (s[3] as u32 & 0x3F),
            4,
        )
    }
}

pub fn uuid_text(out: &mut Vec<u8>, b: &[u8; 16]) {
    const H: &[u8; 16] = b"0123456789abcdef";
    for (i, &c) in b.iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            out.push(b'-');
        }
        out.push(H[(c >> 4) as usize]);
        out.push(H[(c & 15) as usize]);
    }
}

pub fn hex_text(out: &mut Vec<u8>, b: &[u8]) {
    const H: &[u8; 16] = b"0123456789abcdef";
    out.extend_from_slice(b"\\x");
    for &c in b {
        out.push(H[(c >> 4) as usize]);
        out.push(H[(c & 15) as usize]);
    }
}
