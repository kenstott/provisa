//! Scalar text as the product writes it: orjson's float and string spelling, `str(Decimal)`'s
//! decimal point, `temporal_text` for dates and times, and `iso8601_duration` for intervals.

/// A JSON string as orjson escapes it: `"` and `\`, the C0 controls (`\b \t \n \f \r` short,
/// the rest `\u00xx`); everything else, DEL and non-ASCII included, written raw.
pub fn write_str(out: &mut Vec<u8>, s: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push(b'"');
    let mut start = 0;
    for (i, &b) in s.iter().enumerate() {
        let esc: &[u8] = match b {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            0x08 => b"\\b",
            0x09 => b"\\t",
            0x0a => b"\\n",
            0x0c => b"\\f",
            0x0d => b"\\r",
            0x00..=0x1f => b"",
            _ => continue,
        };
        out.extend_from_slice(&s[start..i]);
        if esc.is_empty() {
            out.extend_from_slice(b"\\u00");
            out.push(HEX[(b >> 4) as usize]);
            out.push(HEX[(b & 15) as usize]);
        } else {
            out.extend_from_slice(esc);
        }
        start = i + 1;
    }
    out.extend_from_slice(&s[start..]);
    out.push(b'"');
}

/// A float as orjson writes it: shortest round-trip digits; fixed notation with at least one
/// fractional digit for a decimal exponent in [-5, 16), else `d[.ddd]e±x`; NaN and the
/// infinities as `null`.
pub fn write_f64(out: &mut Vec<u8>, f: f64) {
    if !f.is_finite() {
        out.extend_from_slice(b"null");
        return;
    }
    let mut buf = ryu::Buffer::new();
    let text = buf.format_finite(f).as_bytes();
    // ryu writes "1.5", "1e16", "1.2345e-5", "0.001": recover the digits and the exponent.
    let (neg, body) = if text[0] == b'-' { (true, &text[1..]) } else { (false, text) };
    let (mant, exp_part) = match body.iter().position(|&c| c == b'e' || c == b'E') {
        Some(p) => (&body[..p], Some(&body[p + 1..])),
        None => (body, None),
    };
    let mut digits: [u8; 40] = [0; 40];
    let mut nd = 0usize;
    let mut point: i32 = -1; // digits before the point
    for &c in mant {
        if c == b'.' {
            point = nd as i32;
        } else {
            digits[nd] = c;
            nd += 1;
        }
    }
    if point < 0 {
        point = nd as i32;
    }
    let mut e10: i32 = exp_part
        .map(|e| std::str::from_utf8(e).unwrap().parse::<i32>().unwrap())
        .unwrap_or(0);
    // Strip leading zeros (0.001 → digits "1", point moves) and trailing zeros.
    let mut lead = 0;
    while lead < nd - 1 && digits[lead] == b'0' {
        lead += 1;
    }
    let mut end = nd;
    while end - lead > 1 && digits[end - 1] == b'0' {
        end -= 1;
    }
    let d = &digits[lead..end];
    point -= lead as i32;
    if d == b"0" {
        if neg {
            out.push(b'-');
        }
        out.extend_from_slice(b"0.0");
        return;
    }
    // value = 0.d × 10^(point + e10); scientific exponent = point + e10 - 1
    e10 += point;
    let sci = e10 - 1;
    if neg {
        out.push(b'-');
    }
    if (-5..16).contains(&sci) {
        if e10 <= 0 {
            out.extend_from_slice(b"0.");
            out.extend(std::iter::repeat(b'0').take((-e10) as usize));
            out.extend_from_slice(d);
        } else if (e10 as usize) >= d.len() {
            out.extend_from_slice(d);
            out.extend(std::iter::repeat(b'0').take(e10 as usize - d.len()));
            out.extend_from_slice(b".0");
        } else {
            out.extend_from_slice(&d[..e10 as usize]);
            out.push(b'.');
            out.extend_from_slice(&d[e10 as usize..]);
        }
    } else {
        out.push(d[0]);
        if d.len() > 1 {
            out.push(b'.');
            out.extend_from_slice(&d[1..]);
        }
        out.push(b'e');
        out.push(if sci < 0 { b'-' } else { b'+' });
        let mut ib = itoa::Buffer::new();
        out.extend_from_slice(ib.format(sci.unsigned_abs()).as_bytes());
    }
}

/// What `_convert_value` makes of a `Decimal`: an int when `str(d)` has no point and the float is
/// whole, else the float.
pub enum DecVal {
    Int(i128),
    /// A whole number orjson refuses: outside [-2^63, 2^64).
    BigInt,
    Float(f64),
}

/// `_convert_value(Decimal)` for a coefficient (as decimal digits, without sign) and scale.
pub fn decimal_value(neg: bool, digits: &str, small: Option<i128>, scale: i32) -> (DecVal, f64) {
    let n = digits.len() as i32;
    let exp = -scale;
    let adjusted = exp + n - 1;
    // Decimal.__str__: fixed notation when exp <= 0 and adjusted >= -6, else scientific.
    let has_point = if exp <= 0 && adjusted >= -6 { exp < 0 } else { n > 1 };
    let f = match small {
        // Exact operands, one correctly rounded division: the correctly rounded quotient.
        Some(c) if c.unsigned_abs() < (1u128 << 53) && (0..=22).contains(&scale) => {
            (c as f64) / POW10[scale as usize]
        }
        _ => {
            let s = format!("{}{}e{}", if neg { "-" } else { "" }, digits, exp);
            s.parse::<f64>().unwrap()
        }
    };
    if has_point || f.fract() != 0.0 {
        return (DecVal::Float(f), f);
    }
    if f >= 18446744073709551616.0 || f < -9223372036854775808.0 {
        (DecVal::BigInt, f)
    } else {
        (DecVal::Int(f as i128), f)
    }
}

const POW10: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];

pub const BIG_INT: &str = "Integer exceeds 64-bit range";
pub const SURROGATE: &str = "str is not valid UTF-8: surrogates not allowed";

/// (year, month, day) of a day count from 1970-01-01 (proleptic Gregorian).
pub fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn push2(out: &mut Vec<u8>, v: u32) {
    out.push(b'0' + (v / 10) as u8);
    out.push(b'0' + (v % 10) as u8);
}

/// `date.isoformat()`; Err for a year Python's date cannot hold.
pub fn date_text(out: &mut Vec<u8>, days: i64) -> Result<(), String> {
    let (y, m, d) = civil(days);
    if !(1..=9999).contains(&y) {
        return Err(format!("year {} is out of range", y));
    }
    let y = y as u32;
    push2(out, y / 100);
    push2(out, y % 100);
    out.push(b'-');
    push2(out, m);
    out.push(b'-');
    push2(out, d);
    Ok(())
}

/// The clock part `HH:MM:SS[.ffffff]` of a time of day in microseconds, the fraction's trailing
/// zeros dropped as `temporal_text` drops them.
pub fn clock_text(out: &mut Vec<u8>, micros_of_day: i64) {
    let secs = micros_of_day / 1_000_000;
    let frac = (micros_of_day % 1_000_000) as u32;
    push2(out, (secs / 3600) as u32);
    out.push(b':');
    push2(out, ((secs / 60) % 60) as u32);
    out.push(b':');
    push2(out, (secs % 60) as u32);
    if frac != 0 {
        out.push(b'.');
        let mut f = frac;
        let mut ds = [0u8; 6];
        for i in (0..6).rev() {
            ds[i] = b'0' + (f % 10) as u8;
            f /= 10;
        }
        let mut end = 6;
        while ds[end - 1] == b'0' {
            end -= 1;
        }
        out.extend_from_slice(&ds[..end]);
    }
}

/// `temporal_text(datetime)` for microseconds from the epoch, already shifted to local time, and
/// the zone's offset in minutes (None for a naive timestamp).
pub fn timestamp_text(out: &mut Vec<u8>, local_micros: i64, offset_min: Option<i32>) -> Result<(), String> {
    let day = local_micros.div_euclid(86_400_000_000);
    let tod = local_micros.rem_euclid(86_400_000_000);
    date_text(out, day)?;
    out.push(b'T');
    clock_text(out, tod);
    if let Some(off) = offset_min {
        out.push(if off < 0 { b'-' } else { b'+' });
        let a = off.unsigned_abs();
        push2(out, a / 60);
        out.push(b':');
        push2(out, a % 60);
    }
    Ok(())
}

/// `iso8601_duration` of an interval's PostgreSQL parts.
pub fn iso8601_duration(out: &mut Vec<u8>, months: i64, days: i64, micros: i64) {
    let (m, d, u) = (months as i128, days as i128, micros as i128);
    if (m, d, u) != (0, 0, 0) && m.max(d).max(u) <= 0 {
        out.push(b'-');
        iso8601(out, -m, -d, -u);
    } else {
        iso8601(out, m, d, u);
    }
}

fn push_int(out: &mut Vec<u8>, v: i128) {
    let mut b = itoa::Buffer::new();
    out.extend_from_slice(b.format(v).as_bytes());
}

fn iso8601(out: &mut Vec<u8>, months: i128, days: i128, micros: i128) {
    // int(months / 12): a float quotient truncated; exact for any i64 month count below 2^53.
    let years = ((months as f64) / 12.0).trunc() as i128;
    let months = months - years * 12;
    let sign: &[u8] = if micros < 0 { b"-" } else { b"" };
    let a = micros.abs();
    let hours = a / 3_600_000_000;
    let minutes = (a % 3_600_000_000) / 60_000_000;
    let rem = a % 60_000_000;
    let secs = rem / 1_000_000;
    let frac = rem % 1_000_000;
    let start = out.len();
    out.push(b'P');
    if years != 0 {
        push_int(out, years);
        out.push(b'Y');
    }
    if months != 0 {
        push_int(out, months);
        out.push(b'M');
    }
    if days != 0 {
        push_int(out, days);
        out.push(b'D');
    }
    let has_date = out.len() > start + 1;
    let has_time = hours != 0 || minutes != 0 || secs != 0 || frac != 0;
    if !has_date && !has_time {
        out.extend_from_slice(b"T0S");
        return;
    }
    if has_time {
        out.push(b'T');
        if hours != 0 {
            out.extend_from_slice(sign);
            push_int(out, hours);
            out.push(b'H');
        }
        if minutes != 0 {
            out.extend_from_slice(sign);
            push_int(out, minutes);
            out.push(b'M');
        }
        if secs != 0 || frac != 0 {
            out.extend_from_slice(sign);
            push_int(out, secs);
            if frac != 0 {
                out.push(b'.');
                let s = format!("{:06}", frac);
                out.extend_from_slice(s.trim_end_matches('0').as_bytes());
            }
            out.push(b'S');
        }
    }
}
