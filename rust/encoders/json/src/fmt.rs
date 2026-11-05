//! orjson's float and string spelling, the civil calendar, and `iso8601_duration` for intervals.

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

/// The shortest round-trip digits of a finite float: (negative, digits without leading or
/// trailing zeros ("0" for zero), e10) with value = 0.digits × 10^e10.
pub fn shortest(f: f64) -> (bool, Vec<u8>, i32) {
    let mut buf = ryu::Buffer::new();
    let text = buf.format_finite(f).as_bytes();
    let (neg, body) = if text[0] == b'-' { (true, &text[1..]) } else { (false, text) };
    let (mant, exp_part) = match body.iter().position(|&c| c == b'e' || c == b'E') {
        Some(p) => (&body[..p], Some(&body[p + 1..])),
        None => (body, None),
    };
    let mut digits: Vec<u8> = Vec::with_capacity(24);
    let mut point: i32 = -1;
    for &c in mant {
        if c == b'.' {
            point = digits.len() as i32;
        } else {
            digits.push(c);
        }
    }
    if point < 0 {
        point = digits.len() as i32;
    }
    let e10: i32 = exp_part.map(|e| std::str::from_utf8(e).unwrap().parse::<i32>().unwrap()).unwrap_or(0);
    let lead = digits.iter().take(digits.len() - 1).take_while(|&&c| c == b'0').count();
    let mut end = digits.len();
    while end - lead > 1 && digits[end - 1] == b'0' {
        end -= 1;
    }
    let d = digits[lead..end].to_vec();
    if d == b"0" {
        return (neg, d, 0);
    }
    (neg, d, e10 + point - lead as i32)
}

/// Fixed notation of 0.d × 10^e10 with at least one fractional digit.
pub fn fixed(out: &mut Vec<u8>, d: &[u8], e10: i32) {
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
}

/// A float as orjson writes it: shortest round-trip digits; fixed notation with at least one
/// fractional digit for a decimal exponent in [-5, 16), else `d[.ddd]e±x`; NaN and the
/// infinities as `null`.
pub fn write_f64(out: &mut Vec<u8>, f: f64) {
    if !f.is_finite() {
        out.extend_from_slice(b"null");
        return;
    }
    let (neg, d, e10) = shortest(f);
    if neg {
        out.push(b'-');
    }
    if d == b"0" {
        out.extend_from_slice(b"0.0");
        return;
    }
    let sci = e10 - 1;
    if (-5..16).contains(&sci) {
        fixed(out, &d, e10);
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
