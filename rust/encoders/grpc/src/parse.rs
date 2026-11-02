// Text read as Python reads it into a field of another type: int(text), float(text) and
// datetime.fromisoformat(text) (CPython 3.12). A value one of them refuses is refused here with
// the ValueError it raises.

use crate::pyfmt::{DAY_US, MAX_US, MIN_US};
use crate::{EncErr, R};

fn invalid<T>(what: &str, text: &str) -> R<T> {
    Err(EncErr::Value(format!("{what}: {text:?}")))
}

/// The digits of `s` with Python's underscores (one, between two digits) taken out; None when an
/// underscore is anywhere else.
fn without_underscores(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    for (i, &c) in b.iter().enumerate() {
        if c == b'_' {
            let between = i > 0 && b[i - 1].is_ascii_digit() && b.get(i + 1).is_some_and(|n| n.is_ascii_digit());
            if !between {
                return None;
            }
        } else {
            out.push(c as char);
        }
    }
    Some(out)
}

/// int(text) in base 10: surrounding whitespace, a sign, digits with single underscores between
/// them. A value past i128 is out of every proto integer's range.
pub fn int(text: &str) -> R<i128> {
    let t = text.trim();
    let (negative, body) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, t),
    };
    if body.is_empty() || !body.as_bytes()[0].is_ascii_digit() || !body.bytes().all(|c| c.is_ascii_digit() || c == b'_') {
        return invalid("invalid literal for int() with base 10", text);
    }
    let Some(digits) = without_underscores(body) else {
        return invalid("invalid literal for int() with base 10", text);
    };
    let mut v: i128 = 0;
    for c in digits.bytes() {
        v = v
            .checked_mul(10)
            .and_then(|v| v.checked_add((c - b'0') as i128))
            .ok_or_else(|| EncErr::Value(format!("Value out of range: {}", t)))?;
    }
    Ok(if negative { -v } else { v })
}

/// float(text): surrounding whitespace, inf / infinity / nan in any case with a sign, or a
/// decimal number with single underscores between digits.
pub fn float(text: &str) -> R<f64> {
    let t = text.trim();
    let body = t.strip_prefix(['+', '-']).unwrap_or(t);
    let lower = body.to_ascii_lowercase();
    if matches!(lower.as_str(), "inf" | "infinity" | "nan") {
        return Ok(t.parse::<f64>().unwrap());
    }
    let ok = !body.is_empty()
        && body.bytes().all(|c| c.is_ascii_digit() || matches!(c, b'.' | b'e' | b'E' | b'+' | b'-' | b'_'))
        && body.bytes().any(|c| c.is_ascii_digit());
    let plain = if ok { without_underscores(t) } else { None };
    match plain.and_then(|p| p.parse::<f64>().ok()) {
        Some(x) => Ok(x),
        None => invalid("could not convert string to float", text),
    }
}

/// datetime.fromisoformat(text): (wall clock in microseconds from 1970-01-01T00:00, the UTC offset
/// in seconds when it names one).
pub fn isoformat(text: &str) -> R<(i64, Option<i32>)> {
    parse_iso(text).ok_or_else(|| EncErr::Value(format!("Invalid isoformat string: {text:?}")))?
}

fn num(b: &[u8]) -> Option<i64> {
    if b.is_empty() || !b.iter().all(u8::is_ascii_digit) {
        return None;
    }
    Some(b.iter().fold(0i64, |v, &c| v * 10 + (c - b'0') as i64))
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days from 1970-01-01 of a proleptic Gregorian date (days_from_civil).
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The date part: YYYY-MM-DD, YYYYMMDD, YYYY-Www-D or YYYYWwwD; its length and its day number.
fn date_part(b: &[u8]) -> Option<Result<(usize, i64), EncErr>> {
    let year = num(b.get(0..4)?)?;
    let range = |what: &str| Some(Err(EncErr::Value(format!("{what} is out of range"))));
    let extended = b.get(4) == Some(&b'-');
    let rest = if extended { &b[5..] } else { &b[4..] };
    if rest.first() == Some(&b'W') {
        let week = num(rest.get(1..3)?)?;
        let (day, used) = if extended {
            if rest.get(3) == Some(&b'-') {
                (num(rest.get(4..5)?)?, 5)
            } else {
                (1, 3)
            }
        } else {
            match rest.get(3..4).and_then(num) {
                Some(d) => (d, 4),
                None => (1, 3),
            }
        };
        if year == 0 {
            return range("year");
        }
        let weekday = |d: i64| (d + 3).rem_euclid(7); // 0 Monday (1970-01-01 a Thursday)
        let jan1 = weekday(days_from_civil(year, 1, 1));
        let leap = days_in_month(year, 2) == 29;
        let weeks_in_year = if jan1 == 3 || (jan1 == 2 && leap) { 53 } else { 52 };
        let jan4 = days_from_civil(year, 1, 4);
        let weekday = weekday(jan4);
        if week < 1 || week > weeks_in_year || !(1..=7).contains(&day) {
            return range("week or weekday");
        }
        let days = jan4 - weekday + (week - 1) * 7 + (day - 1);
        return Some(Ok(((if extended { 5 } else { 4 }) + used, days)));
    }
    let (month, day, used) = if extended {
        if rest.get(2) != Some(&b'-') {
            return None;
        }
        (num(rest.get(0..2)?)?, num(rest.get(3..5)?)?, 10)
    } else {
        (num(rest.get(0..2)?)?, num(rest.get(2..4)?)?, 8)
    };
    if year < 1 {
        return range("year");
    }
    if !(1..=12).contains(&month) {
        return range("month");
    }
    if day < 1 || day > days_in_month(year, month) {
        return range("day");
    }
    Some(Ok((used, days_from_civil(year, month, day))))
}

/// HH[:MM[:SS[.f+]]] or HH[MM[SS[.f+]]]: microseconds of the day (a fraction past six digits is
/// truncated); `None` when it is not one.
fn time_part(b: &[u8]) -> Option<Result<i64, EncErr>> {
    let hour = num(b.get(0..2)?)?;
    let mut i = 2;
    let extended = b.get(2) == Some(&b':');
    let mut fields = [hour, 0, 0];
    for f in fields.iter_mut().skip(1) {
        if i >= b.len() {
            break;
        }
        if extended {
            if b[i] != b':' {
                break;
            }
            i += 1;
        }
        *f = num(b.get(i..i + 2)?)?;
        i += 2;
    }
    let mut micros = 0i64;
    if i < b.len() && (b[i] == b'.' || b[i] == b',') {
        let start = i + 1;
        let mut end = start;
        while end < b.len() && b[end].is_ascii_digit() {
            end += 1;
        }
        let frac = &b[start..end];
        if frac.is_empty() {
            return None;
        }
        let six: Vec<u8> = frac.iter().copied().chain(std::iter::repeat(b'0')).take(6).collect();
        micros = num(&six)?;
        i = end;
    }
    if i != b.len() {
        return None;
    }
    let [h, m, s] = fields;
    if h > 23 {
        return Some(Err(EncErr::Value("hour must be in 0..23".into())));
    }
    if m > 59 {
        return Some(Err(EncErr::Value("minute must be in 0..59".into())));
    }
    if s > 59 {
        return Some(Err(EncErr::Value("second must be in 0..59".into())));
    }
    Some(Ok(((h * 60 + m) * 60 + s) * 1_000_000 + micros))
}

fn parse_iso(text: &str) -> Option<R<(i64, Option<i32>)>> {
    let b = text.as_bytes();
    let (used, days) = match date_part(b)? {
        Ok(d) => d,
        Err(e) => return Some(Err(e)),
    };
    if used == b.len() {
        return Some(Ok((days * DAY_US, None)));
    }
    // the separator: any one character
    let sep_len = text[used..].chars().next()?.len_utf8();
    let t = &b[used + sep_len..];
    let tz_at = t.iter().position(|&c| c == b'+' || c == b'-' || c == b'Z');
    let (clock, zone) = match tz_at {
        Some(at) => (&t[..at], Some(&t[at..])),
        None => (t, None),
    };
    let micros = match time_part(clock)? {
        Ok(m) => m,
        Err(e) => return Some(Err(e)),
    };
    let offset = match zone {
        None => None,
        Some(b"Z") => Some(0),
        Some(z) => {
            let sign = if z[0] == b'-' { -1 } else { 1 };
            let off = match time_part(&z[1..])? {
                Ok(m) => m,
                Err(e) => return Some(Err(e)),
            };
            if off % 1_000_000 != 0 {
                return Some(Err(EncErr::Unsupported("a UTC offset with a fraction of a second".into())));
            }
            Some((sign * off / 1_000_000) as i32)
        }
    };
    let local = days * DAY_US + micros;
    if !(MIN_US..=MAX_US).contains(&local) {
        return Some(Err(EncErr::Value("datetime out of range".into())));
    }
    Some(Ok((local, offset)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ints_as_python_reads_them() {
        assert_eq!(int(" 12 ").unwrap(), 12);
        assert_eq!(int("-1_000").unwrap(), -1000);
        assert_eq!(int("+7").unwrap(), 7);
        for bad in ["", "1_", "_1", "1__0", "1.0", "0x10", "--1"] {
            assert!(int(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn floats_as_python_reads_them() {
        assert_eq!(float(" 1.5 ").unwrap(), 1.5);
        assert_eq!(float("1_0.2_5").unwrap(), 10.25);
        assert_eq!(float("-Infinity").unwrap(), f64::NEG_INFINITY);
        assert!(float("nan").unwrap().is_nan());
        assert_eq!(float("1e400").unwrap(), f64::INFINITY);
        for bad in ["", "1_", "abc", "1e", "1.2.3"] {
            assert!(float(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn isoformat_as_python_reads_it() {
        let day = days_from_civil(2024, 1, 1) * DAY_US;
        let hour = 3_600_000_000;
        assert_eq!(isoformat("2024-01-01").unwrap(), (day, None));
        assert_eq!(isoformat("20240101").unwrap(), (day, None));
        assert_eq!(isoformat("2024-W01-1").unwrap(), (day, None));
        assert_eq!(isoformat("2024-01-01T10").unwrap(), (day + 10 * hour, None));
        assert_eq!(isoformat("2024-01-01 10:00:00,5").unwrap(), (day + 10 * hour + 500_000, None));
        assert_eq!(isoformat("2024-01-01T10:00:00.1234567").unwrap(), (day + 10 * hour + 123_456, None));
        assert_eq!(isoformat("2024-01-01T10:00Z").unwrap(), (day + 10 * hour, Some(0)));
        assert_eq!(isoformat("2024-01-01T10:00:00+0530").unwrap(), (day + 10 * hour, Some(19_800)));
        assert_eq!(isoformat("2024-01-01T10:00:00.12-05").unwrap(), (day + 10 * hour + 120_000, Some(-18_000)));
        for bad in ["2024-01-01T24:00", "2024-13-01", "2023-02-29", "2024-01-01T", "x"] {
            assert!(isoformat(bad).is_err(), "{bad:?}");
        }
    }
}
