// Number text and binary forms, each a port of the Python function named in its comment.

use crate::{EncErr, R};

/// A fixed stack buffer for a value's digits (no allocation per value).
pub struct StackBuf {
    buf: [u8; 128],
    len: usize,
}

impl StackBuf {
    pub fn new() -> Self {
        StackBuf { buf: [0; 128], len: 0 }
    }
}

fn push_zeros(out: &mut Vec<u8>, n: i32) {
    for _ in 0..n.max(0) {
        out.push(b'0');
    }
}

/// The significant digits and the decimal exponent of the first digit of a Ryu formatting of a
/// finite |x| (`1.5e-7`, `0.0001`, `1e16`).
fn ryu_digits(formatted: &str, digits: &mut StackBuf) -> i32 {
    let (mantissa, exp) = match formatted.split_once('e') {
        Some((m, e)) => (m, e.parse::<i32>().unwrap()),
        None => (formatted, 0),
    };
    let point = mantissa.bytes().position(|c| c == b'.').unwrap_or(mantissa.len()) as i32;
    let mut first = None;
    for (k, c) in mantissa.bytes().filter(|&c| c != b'.').enumerate() {
        if first.is_none() && c != b'0' {
            first = Some(k as i32);
        }
        if first.is_some() {
            digits.buf[digits.len] = c;
            digits.len += 1;
        }
    }
    while digits.len > 1 && digits.buf[digits.len - 1] == b'0' {
        digits.len -= 1;
    }
    let Some(first) = first else {
        digits.buf[0] = b'0';
        digits.len = 1;
        return 0;
    };
    point - 1 - first + exp
}

/// repr's digits of a finite |x|: the shortest that read back, the closest of those, an exact tie
/// to the even one, a decimal on the boundary of the rounding interval taken (David Gay's mode 0;
/// Ryu with acceptBounds).
fn repr_digits(a: f64, digits: &mut StackBuf) -> i32 {
    let mut b = ryu::Buffer::new();
    ryu_digits(b.format_finite(a), digits)
}

/// PostgreSQL's digits of a finite |x| (float8out): as repr's, except that a decimal on the
/// boundary of the rounding interval is never taken, so a few values carry one digit more
/// (`1.2188652062118339e+17`, not `1.218865206211834e+17`).
fn pg_digits_f64(a: f64, digits: &mut StackBuf) -> i32 {
    let mut b = ryu_pg::Buffer::new();
    ryu_digits(b.format_finite(a), digits)
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

/// float8out (extra_float_digits >= 1, the default): NaN, Infinity, -Infinity, -0, else
/// PostgreSQL's round-trip digits, in fixed form when the decimal exponent is in [-4, 15).
pub fn float8_text(x: f64, out: &mut Vec<u8>) {
    if non_finite(x, out) {
        return;
    }
    let mut digits = StackBuf::new();
    let exp = pg_digits_f64(x.abs(), &mut digits);
    shortest_text(&digits.buf[..digits.len], exp, x.is_sign_negative(), 15, out);
}

/// float4out of the float `x` packs to: as float8out over a float's round-trip digits (f2s.c, also
/// with acceptBounds false), in fixed form when the decimal exponent is in [-4, 6). A finite double past float's range is the
/// OverflowError struct.pack("!f") raises.
pub fn float4_text(x: f64, out: &mut Vec<u8>) -> R<()> {
    if non_finite(x, out) {
        return Ok(());
    }
    let f = (x as f32).abs();
    if f.is_infinite() {
        return Err(EncErr::Overflow("float too large to pack with f format".into()));
    }
    let mut b = ryu_pg::Buffer::new();
    let mut digits = StackBuf::new();
    let exp = ryu_digits(b.format_finite(f), &mut digits);
    shortest_text(&digits.buf[..digits.len], exp, x.is_sign_negative(), 6, out);
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
    s.chunks(4)
        .map(|c| c.iter().fold(0u16, |acc, &d| acc * 10 + (d - b'0') as u16))
}

/// numeric_out of a finite decimal: plain notation with its scale's digits after the point (the
/// product writes Decimal.__str__, which turns to an exponent below 1E-6), no sign on zero.
pub fn numeric_text(negative: bool, digits: &[u8], exp: i32, out: &mut Vec<u8>) {
    let first = digits.iter().position(|&c| c != b'0').unwrap_or(digits.len());
    let digits = &digits[first..];
    if negative && !digits.is_empty() {
        out.push(b'-');
    }
    if exp >= 0 {
        if digits.is_empty() {
            out.push(b'0');
        } else {
            out.extend_from_slice(digits);
            push_zeros(out, exp);
        }
        return;
    }
    let f = (-exp) as usize;
    if digits.len() > f {
        out.extend_from_slice(&digits[..digits.len() - f]);
    } else {
        out.push(b'0');
    }
    out.push(b'.');
    if digits.len() < f {
        push_zeros(out, (f - digits.len()) as i32);
        out.extend_from_slice(digits);
    } else {
        out.extend_from_slice(&digits[digits.len() - f..]);
    }
}

/// numeric_send of a finite decimal, every digit kept (the product's _numeric_to_pg_binary takes
/// abs(d) under the default context, which rounds past 28 significant digits).
pub fn numeric_binary(negative: bool, digits: &[u8], exp: i32, out: &mut Vec<u8>) -> R<()> {
    let sign_flag: u16 = if negative && digits != b"0" { 0x4000 } else { 0 };
    let dscale: i64 = if exp < 0 { -(exp as i64) } else { 0 };
    let dscale =
        u16::try_from(dscale).map_err(|_| EncErr::Value("ushort format requires 0 <= number <= 65535".into()))?;
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
    let weight =
        i16::try_from(weight).map_err(|_| EncErr::Value("short format requires -32768 <= number <= 32767".into()))?;
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
