// Python's str() and isoformat() of the values a result's Arrow columns hold, each a port of the
// CPython or product function named in its comment. float repr and Decimal.__str__ are carried
// over from the pgwire encoder (J483), where they were held to CPython byte for byte.

use std::fmt::Write;


/// A fixed stack buffer for core::fmt output (no allocation per value).
pub struct StackBuf {
    buf: [u8; 128],
    len: usize,
}

impl StackBuf {
    pub fn new() -> Self {
        StackBuf { buf: [0; 128], len: 0 }
    }
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.buf[..self.len]).unwrap()
    }
}

impl Write for StackBuf {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let b = s.as_bytes();
        if self.len + b.len() > self.buf.len() {
            return Err(std::fmt::Error);
        }
        self.buf[self.len..self.len + b.len()].copy_from_slice(b);
        self.len += b.len();
        Ok(())
    }
}

/// Splits a Rust `{:e}` form (`1.2345e-5`) into its significant digits (no trailing zeros, "0"
/// for zero) and its decimal exponent.
fn sci_parts(s: &str, digits: &mut StackBuf) -> i32 {
    let (mantissa, exp) = s.split_once('e').unwrap();
    for c in mantissa.bytes().filter(|&c| c != b'.') {
        digits.buf[digits.len] = c;
        digits.len += 1;
    }
    while digits.len > 1 && digits.buf[digits.len - 1] == b'0' {
        digits.len -= 1;
    }
    if digits.len == 1 && digits.buf[0] == b'0' || digits.len == 0 {
        digits.buf[0] = b'0';
        digits.len = 1;
    }
    exp.parse().unwrap()
}

fn push_zeros(out: &mut Vec<u8>, n: i32) {
    for _ in 0..n.max(0) {
        out.push(b'0');
    }
}

/// repr's digits of a finite |x|: the shortest that read back, the closest of those, a tie to the
/// even one (David Gay's mode 0). Rust's shortest form can take the odd side of an exact tie,
/// which only a value needing 15+ digits can be at; there the correctly rounded form of that
/// length is taken when it reads back.
fn repr_digits(a: f64, digits: &mut StackBuf) -> i32 {
    let mut s = StackBuf::new();
    write!(s, "{:e}", a).unwrap();
    let exp = sci_parts(s.as_str(), digits);
    if digits.len < 15 {
        return exp;
    }
    let mut exact = StackBuf::new();
    write!(exact, "{:.*e}", digits.len - 1, a).unwrap();
    if exact.as_str().parse::<f64>().unwrap() == a {
        digits.len = 0;
        return sci_parts(exact.as_str(), digits);
    }
    exp
}

/// float.__repr__ (what json.dumps writes for a float).
pub fn float_repr(x: f64, out: &mut Vec<u8>) {
    if x.is_nan() {
        out.extend_from_slice(b"NaN");
        return;
    }
    if x.is_infinite() {
        out.extend_from_slice(if x > 0.0 { b"Infinity" } else { b"-Infinity" });
        return;
    }
    let mut db = StackBuf::new();
    let exp = repr_digits(x.abs(), &mut db);
    let digits = &db.buf[..db.len];
    if x.is_sign_negative() {
        out.push(b'-');
    }
    let decpt = exp + 1;
    if decpt > -4 && decpt <= 16 {
        if decpt <= 0 {
            out.extend_from_slice(b"0.");
            push_zeros(out, -decpt);
            out.extend_from_slice(digits);
        } else if decpt as usize >= digits.len() {
            out.extend_from_slice(digits);
            push_zeros(out, decpt - digits.len() as i32);
            out.extend_from_slice(b".0");
        } else {
            out.extend_from_slice(&digits[..decpt as usize]);
            out.push(b'.');
            out.extend_from_slice(&digits[decpt as usize..]);
        }
    } else {
        out.push(digits[0]);
        if digits.len() > 1 {
            out.push(b'.');
            out.extend_from_slice(&digits[1..]);
        }
        out.push(b'e');
        out.push(if exp < 0 { b'-' } else { b'+' });
        let a = exp.unsigned_abs();
        if a < 10 {
            out.push(b'0');
        }
        let mut b = itoa::Buffer::new();
        out.extend_from_slice(b.format(a).as_bytes());
    }
}

/// Decimal.__str__ (non-engineering) of a finite decimal: sign, coefficient digits, exponent.
pub fn decimal_str(negative: bool, digits: &[u8], exp: i32, out: &mut Vec<u8>) {
    let n = digits.len() as i32;
    let leftdigits = exp + n;
    let dotplace = if exp <= 0 && leftdigits > -6 { leftdigits } else { 1 };
    if negative {
        out.push(b'-');
    }
    if dotplace <= 0 {
        out.extend_from_slice(b"0.");
        push_zeros(out, -dotplace);
        out.extend_from_slice(digits);
    } else if dotplace >= n {
        out.extend_from_slice(digits);
        push_zeros(out, dotplace - n);
    } else {
        out.extend_from_slice(&digits[..dotplace as usize]);
        out.push(b'.');
        out.extend_from_slice(&digits[dotplace as usize..]);
    }
    if leftdigits != dotplace {
        let e = leftdigits - dotplace;
        out.push(b'E');
        out.push(if e < 0 { b'-' } else { b'+' });
        let mut b = itoa::Buffer::new();
        out.extend_from_slice(b.format(e.unsigned_abs()).as_bytes());
    }
}

/// str(float): repr, with CPython's own spellings of the non-finite values.
pub fn float_str(x: f64, out: &mut Vec<u8>) {
    if x.is_nan() {
        out.extend_from_slice(b"nan");
    } else if x.is_infinite() {
        out.extend_from_slice(if x > 0.0 { b"inf" } else { b"-inf" });
    } else {
        float_repr(x, out);
    }
}

/// str(Decimal) of a decimal128 value as pyarrow hands it back: coefficient |unscaled|, exponent
/// -scale (trailing zeros kept).
pub fn decimal128_str(unscaled: i128, scale: i32, out: &mut Vec<u8>) {
    let mut b = itoa::Buffer::new();
    let digits = b.format(unscaled.unsigned_abs());
    decimal_str(unscaled < 0, digits.as_bytes(), -scale, out);
}

pub fn int_str(v: i128, out: &mut Vec<u8>) {
    let mut b = itoa::Buffer::new();
    out.extend_from_slice(b.format(v).as_bytes());
}

pub const DAY_US: i64 = 86_400_000_000;
/// date.min and date.max as days from 1970-01-01.
pub const MIN_DAYS: i64 = -719_162;
pub const MAX_DAYS: i64 = 2_932_896;
/// datetime.min and datetime.max as microseconds from 1970-01-01T00:00.
pub const MIN_US: i64 = MIN_DAYS * DAY_US;
pub const MAX_US: i64 = (MAX_DAYS + 1) * DAY_US - 1;
/// date.toordinal() of 1970-01-01.
pub const EPOCH_ORDINAL: i64 = 719_163;

/// (year, month, day) of days from 1970-01-01 (proleptic Gregorian).
pub fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + if m <= 2 { 1 } else { 0 }, m, d)
}

fn push_pad(out: &mut Vec<u8>, v: i64, width: usize) {
    let mut b = itoa::Buffer::new();
    let s = b.format(v);
    for _ in s.len()..width {
        out.push(b'0');
    }
    out.extend_from_slice(s.as_bytes());
}

/// str(date): date.isoformat(), a year in 1..=9999.
pub fn date_str(days: i64, out: &mut Vec<u8>) {
    let (y, m, d) = civil(days);
    push_pad(out, y, 4);
    out.push(b'-');
    push_pad(out, m as i64, 2);
    out.push(b'-');
    push_pad(out, d as i64, 2);
}

/// A time of day, a datetime's wall clock, or a date's: what str() and isoformat() of it write.
/// `sub_ns` is a nanosecond unit's nanoseconds past the microsecond (pandas' Timestamp, which
/// to_pylist makes of one, writes nine fraction digits when they are not zero).
#[derive(Clone, Copy)]
pub struct Clock {
    pub us: i64,
    pub sub_ns: u32,
}

/// The fraction of a clock: .ffffff when its microsecond is not zero (.fffffffff with
/// nanoseconds); `trim` drops trailing zeros (serialize.temporal_text's _FRACTION_ZEROS).
fn fraction(c: Clock, trim: bool, out: &mut Vec<u8>) {
    let micro = c.us.rem_euclid(1_000_000);
    let (mut f, mut width) = if c.sub_ns != 0 {
        (micro * 1000 + c.sub_ns as i64, 9)
    } else {
        (micro, 6)
    };
    if f == 0 {
        return;
    }
    if trim {
        while f % 10 == 0 {
            f /= 10;
            width -= 1;
        }
    }
    out.push(b'.');
    push_pad(out, f, width);
}

/// str(time) / time.isoformat(): HH:MM:SS, then the fraction.
pub fn time_str(c: Clock, trim: bool, out: &mut Vec<u8>) {
    let secs = c.us.div_euclid(1_000_000);
    push_pad(out, secs / 3600, 2);
    out.push(b':');
    push_pad(out, secs / 60 % 60, 2);
    out.push(b':');
    push_pad(out, secs % 60, 2);
    fraction(c, trim, out);
}

/// A UTC offset as datetime writes it: +HH:MM, with :SS when it has seconds.
pub fn offset_str(off: i32, out: &mut Vec<u8>) {
    out.push(if off < 0 { b'-' } else { b'+' });
    let a = off.unsigned_abs() as i64;
    push_pad(out, a / 3600, 2);
    out.push(b':');
    push_pad(out, a / 60 % 60, 2);
    if a % 60 != 0 {
        out.push(b':');
        push_pad(out, a % 60, 2);
    }
}

/// str(datetime) (`sep` b' ') or datetime.isoformat() (`sep` b'T', `trim` for temporal_text) of a
/// wall clock in microseconds from 1970-01-01T00:00, with the UTC offset of an aware one.
pub fn datetime_str(local: Clock, offset_s: Option<i32>, sep: u8, trim: bool, out: &mut Vec<u8>) {
    date_str(local.us.div_euclid(DAY_US), out);
    out.push(sep);
    time_str(Clock { us: local.us.rem_euclid(DAY_US), sub_ns: local.sub_ns }, trim, out);
    if let Some(off) = offset_s {
        offset_str(off, out);
    }
}

/// provisa.core.ir_types.iso8601_duration of an interval's (months, days, microseconds).
pub fn iso8601_duration(months: i64, days: i64, micros: i64, out: &mut Vec<u8>) {
    if (months, days, micros) != (0, 0, 0) && months.max(days).max(micros) <= 0 {
        out.push(b'-');
        iso8601(-months, -days, -micros, out);
    } else {
        iso8601(months, days, micros, out);
    }
}

/// ir_types._iso8601.
fn iso8601(months: i64, days: i64, micros: i64, out: &mut Vec<u8>) {
    let years = months / 12; // int(months / 12): exact for any i32 month count, truncated
    let months = months - years * 12;
    let sign: &[u8] = if micros < 0 { b"-" } else { b"" };
    let a = micros.unsigned_abs();
    let (hours, rem) = (a / 3_600_000_000, a % 3_600_000_000);
    let (minutes, rem) = (rem / 60_000_000, rem % 60_000_000);
    let (secs, frac) = (rem / 1_000_000, rem % 1_000_000);
    let mut b = itoa::Buffer::new();
    let start = out.len();
    out.push(b'P');
    for (v, unit) in [(years, b'Y'), (months, b'M'), (days, b'D')] {
        if v != 0 {
            out.extend_from_slice(b.format(v).as_bytes());
            out.push(unit);
        }
    }
    let date_end = out.len();
    out.push(b'T');
    for (v, unit) in [(hours, b'H'), (minutes, b'M')] {
        if v != 0 {
            out.extend_from_slice(sign);
            out.extend_from_slice(b.format(v).as_bytes());
            out.push(unit);
        }
    }
    if secs != 0 || frac != 0 {
        out.extend_from_slice(sign);
        out.extend_from_slice(b.format(secs).as_bytes());
        let mut f = frac;
        let mut width = 6;
        while width > 0 && f % 10 == 0 {
            f /= 10;
            width -= 1;
        }
        if width > 0 {
            out.push(b'.');
            push_pad(out, f as i64, width);
        }
        out.push(b'S');
    }
    if out.len() == date_end + 1 {
        out.pop(); // no time part
        if date_end == start + 1 {
            out.truncate(start);
            out.extend_from_slice(b"PT0S");
        }
    }
}
