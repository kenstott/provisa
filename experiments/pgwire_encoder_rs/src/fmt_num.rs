// Number text and binary forms, each a port of the Python function named in its comment.

use std::fmt::Write;

use crate::{EncErr, R};

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

/// _shortest_text: float8out / float4out for a finite value's shortest digits.
fn shortest_text(digits: &[u8], exp: i32, negative: bool, fixed_below: i32, out: &mut Vec<u8>) {
    if negative {
        out.push(b'-');
    }
    if digits == b"0" {
        out.push(b'0');
        return;
    }
    if exp < -4 || exp >= fixed_below {
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
        return;
    }
    if exp < 0 {
        out.extend_from_slice(b"0.");
        push_zeros(out, -exp - 1);
        out.extend_from_slice(digits);
        return;
    }
    let point = (exp + 1) as usize;
    if digits.len() <= point {
        out.extend_from_slice(digits);
        push_zeros(out, (point - digits.len()) as i32);
    } else {
        out.extend_from_slice(&digits[..point]);
        out.push(b'.');
        out.extend_from_slice(&digits[point..]);
    }
}

/// _non_finite_text
fn non_finite(x: f64, out: &mut Vec<u8>) -> bool {
    if x.is_nan() {
        out.extend_from_slice(b"NaN");
    } else if x.is_infinite() {
        out.extend_from_slice(if x > 0.0 { b"Infinity" } else { b"-Infinity" });
    } else {
        return false;
    }
    true
}

/// _float8_text: the shortest round-trip digits (repr), PostgreSQL's layout.
pub fn float8_text(x: f64, out: &mut Vec<u8>) {
    if non_finite(x, out) {
        return;
    }
    let mut digits = StackBuf::new();
    let exp = repr_digits(x.abs(), &mut digits);
    shortest_text(&digits.buf[..digits.len], exp, x.is_sign_negative(), 15, out);
}

const FLOAT4_MAX_AS_DOUBLE: f64 = 3.4028234663852886e38;

/// _float4_text: the fewest digits (1..9) whose correctly rounded form, read as a double then
/// packed as a float, is the value's float.
pub fn float4_text(x: f64, out: &mut Vec<u8>) -> R<()> {
    if non_finite(x, out) {
        return Ok(());
    }
    let a = x.abs();
    let wanted = a as f32;
    if wanted.is_infinite() {
        return Err(EncErr::Overflow("float too large to pack with f format".into()));
    }
    let wanted = wanted.to_bits();
    let negative = x.is_sign_negative();
    for d in 1..10usize {
        let mut s = StackBuf::new();
        write!(s, "{:.*e}", d - 1, a).unwrap();
        let back: f64 = s.as_str().parse().unwrap();
        let packed = back as f32;
        let reads_back = if packed.is_infinite() {
            d == 8 && wanted == (FLOAT4_MAX_AS_DOUBLE as f32).to_bits()
        } else {
            packed.to_bits() == wanted
        };
        if reads_back {
            let mut digits = StackBuf::new();
            let exp = sci_parts(s.as_str(), &mut digits);
            shortest_text(&digits.buf[..digits.len], exp, negative, 6, out);
            return Ok(());
        }
    }
    let mut s = StackBuf::new();
    write!(s, "{:.8e}", a).unwrap();
    let mut digits = StackBuf::new();
    let exp = sci_parts(s.as_str(), &mut digits);
    shortest_text(&digits.buf[..digits.len], exp, negative, 6, out);
    Ok(())
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

fn groups_of(s: &[u8]) -> impl Iterator<Item = u16> + '_ {
    s.chunks(4).map(|c| c.iter().fold(0u16, |acc, &d| acc * 10 + (d - b'0') as u16))
}

/// abs(d) under the default decimal context: a coefficient past 28 digits is rounded to 28, half
/// to even, its exponent raised by the digits dropped.
fn context_round(digits: &[u8], exp: i32) -> (Vec<u8>, i32) {
    const PREC: usize = 28;
    if digits.len() <= PREC {
        return (digits.to_vec(), exp);
    }
    let dropped = digits.len() - PREC;
    let mut kept = digits[..PREC].to_vec();
    let first = digits[PREC];
    let rest_zero = digits[PREC + 1..].iter().all(|&c| c == b'0');
    let up = first > b'5' || (first == b'5' && (!rest_zero || (kept[PREC - 1] - b'0') % 2 == 1));
    let mut exp = exp + dropped as i32;
    if up {
        let mut k = PREC;
        loop {
            if k == 0 {
                // 99..9 carried to 10..0: one digit more than precision, so one more is dropped.
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
    (kept, exp)
}

/// _numeric_to_pg_binary for a finite decimal.
pub fn numeric_binary(negative: bool, digits: &[u8], exp: i32, out: &mut Vec<u8>) -> R<()> {
    let (rounded, exp) = context_round(digits, exp);
    let digits = &rounded[..];
    let sign_flag: u16 = if negative && digits != b"0" { 0x4000 } else { 0 };
    let dscale: i64 = if exp < 0 { -(exp as i64) } else { 0 };
    let dscale = u16::try_from(dscale).map_err(|_| EncErr::Value("ushort format requires 0 <= number <= 65535".into()))?;
    // format(d_abs, "f") split at the point.
    let mut int_part: Vec<u8> = Vec::with_capacity(digits.len() + 4);
    let mut frac_part: Vec<u8> = Vec::new();
    if exp >= 0 {
        int_part.extend_from_slice(digits);
        if digits != b"0" {
            push_zeros(&mut int_part, exp);
        }
    } else {
        let f = (-exp) as usize;
        if digits.len() > f {
            int_part.extend_from_slice(&digits[..digits.len() - f]);
            frac_part.extend_from_slice(&digits[digits.len() - f..]);
        } else {
            int_part.push(b'0');
            push_zeros(&mut frac_part, (f - digits.len()) as i32);
            frac_part.extend_from_slice(digits);
        }
    }
    let first = int_part.iter().position(|&c| c != b'0').unwrap_or(int_part.len());
    let int_str = &int_part[first..];
    let mut groups: Vec<u16> = Vec::with_capacity(int_str.len() / 4 + frac_part.len() / 4 + 2);
    let mut weight: i32;
    if !int_str.is_empty() {
        let pad = (4 - int_str.len() % 4) % 4;
        let mut padded = vec![b'0'; pad];
        padded.extend_from_slice(int_str);
        groups.extend(groups_of(&padded));
        weight = groups.len() as i32 - 1;
    } else {
        weight = -1;
    }
    if !frac_part.is_empty() {
        let pad = (4 - frac_part.len() % 4) % 4;
        push_zeros(&mut frac_part, pad as i32);
        let mut frac: Vec<u16> = groups_of(&frac_part).collect();
        let mut lead = 0;
        if groups.is_empty() {
            while lead < frac.len() && frac[lead] == 0 {
                lead += 1;
                weight -= 1;
            }
        }
        groups.extend(frac.drain(lead..));
    }
    while groups.last() == Some(&0) {
        groups.pop();
    }
    let weight = i16::try_from(weight).map_err(|_| EncErr::Value("short format requires -32768 <= number <= 32767".into()))?;
    if groups.is_empty() {
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&0i16.to_be_bytes());
        out.extend_from_slice(&sign_flag.to_be_bytes());
        out.extend_from_slice(&dscale.to_be_bytes());
        return Ok(());
    }
    out.extend_from_slice(&(groups.len() as u16).to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign_flag.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for g in groups {
        out.extend_from_slice(&g.to_be_bytes());
    }
    Ok(())
}
