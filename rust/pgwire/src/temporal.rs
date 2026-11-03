// Dates, timestamps, times and intervals, as provisa's column_values and buenavista's converters
// write them.

use arrow::datatypes::TimeUnit;

use crate::tz::Zone;
use crate::{EncErr, R};

pub const DAY_US: i128 = 86_400_000_000;
/// datetime.date(1, 1, 1) and date(9999, 12, 31) as days since 1970-01-01.
const MIN_DAY: i64 = -719_162;
const MAX_DAY: i64 = 2_932_896;
const TIMEDELTA_MAX_DAYS: i128 = 999_999_999;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum TzSpec {
    Naive,
    /// A fixed UTC offset in seconds (UTC is 0).
    Fixed(i32),
    /// An IANA zone: its offset at each instant.
    Named(Zone),
}

impl TzSpec {
    /// UTC and +HH:MM / -HH:MM are fixed offsets (pyarrow gives them a fixed tzinfo); any other
    /// name is an IANA zone.
    pub fn parse(name: &str) -> R<TzSpec> {
        if name == "UTC" {
            return Ok(TzSpec::Fixed(0));
        }
        let b = name.as_bytes();
        if b.len() == 6 && (b[0] == b'+' || b[0] == b'-') && b[3] == b':' {
            if let (Ok(h), Ok(m)) = (name[1..3].parse::<i32>(), name[4..6].parse::<i32>()) {
                let secs = h * 3600 + m * 60;
                return Ok(TzSpec::Fixed(if b[0] == b'-' { -secs } else { secs }));
            }
        }
        Zone::named(name)
            .map(TzSpec::Named)
            .ok_or_else(|| EncErr::Value(format!("unknown time zone {name:?}")))
    }

    /// The offset in seconds east of UTC at `micros` after the epoch (UTC), None for a naive value.
    pub fn offset_at(&self, micros: i128) -> Option<i32> {
        match self {
            TzSpec::Naive => None,
            TzSpec::Fixed(off) => Some(*off),
            TzSpec::Named(zone) => Some(zone.offset_at(micros.div_euclid(1_000_000) as i64)),
        }
    }
}

pub fn date_infinity(days: i32) -> i32 {
    // ir_arrow._DATE_INFINITY: the int32 range's top, and its negation, mark +/-infinity.
    if days == i32::MAX {
        1
    } else if days == -i32::MAX {
        -1
    } else {
        0
    }
}

pub fn timestamp_infinity(raw: i64) -> i32 {
    if raw == i64::MAX {
        1
    } else if raw == -i64::MAX {
        -1
    } else {
        0
    }
}

pub fn check_date(days: i64) -> R<()> {
    if !(MIN_DAY..=MAX_DAY).contains(&days) {
        return Err(EncErr::Value("year is out of range".into()));
    }
    Ok(())
}

/// Civil (year, month, day) of days since 1970-01-01 (Howard Hinnant's days_from_civil inverse).
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

fn push_ymd(days: i64, out: &mut Vec<u8>) {
    let (y, m, d) = civil(days);
    let y = y as u32;
    out.push(b'0' + (y / 1000) as u8);
    out.push(b'0' + (y / 100 % 10) as u8);
    push2(out, y % 100);
    out.push(b'-');
    push2(out, m);
    out.push(b'-');
    push2(out, d);
}

/// _date_text of days since 1970-01-01, or the ±infinity `inf` marks.
pub fn date_text(days: i64, inf: i32, out: &mut Vec<u8>) -> R<()> {
    match inf {
        1 => out.extend_from_slice(b"infinity"),
        -1 => out.extend_from_slice(b"-infinity"),
        _ => {
            check_date(days)?;
            push_ymd(days, out);
        }
    }
    Ok(())
}

/// Microseconds of an Arrow time value in `unit`. A nanosecond value is a pandas Timestamp or
/// Timedelta in to_pylist, its microseconds floored; the int64 minimum is NaT, which no converter
/// takes.
pub fn micros_of(raw: i64, unit: TimeUnit) -> R<i128> {
    Ok(match unit {
        TimeUnit::Second => raw as i128 * 1_000_000,
        TimeUnit::Millisecond => raw as i128 * 1_000,
        TimeUnit::Microsecond => raw as i128,
        TimeUnit::Nanosecond if raw == i64::MIN => return Err(EncErr::Value("NaT is not a value".into())),
        TimeUnit::Nanosecond => raw.div_euclid(1000) as i128,
    })
}

/// _pg_clock's HH:MM:SS[.fraction] of microseconds since midnight.
pub fn clock_text(us: i64, out: &mut Vec<u8>) {
    let secs = us / 1_000_000;
    let frac = (us % 1_000_000) as u32;
    push2(out, (secs / 3600) as u32);
    out.push(b':');
    push2(out, (secs / 60 % 60) as u32);
    out.push(b':');
    push2(out, (secs % 60) as u32);
    if frac != 0 {
        let mut f = frac;
        let mut digits = [b'0'; 6];
        for k in (0..6).rev() {
            digits[k] = b'0' + (f % 10) as u8;
            f /= 10;
        }
        let mut n = 6;
        while digits[n - 1] == b'0' {
            n -= 1;
        }
        out.push(b'.');
        out.extend_from_slice(&digits[..n]);
    }
}

/// _pg_offset
pub fn offset_text(seconds: i32, out: &mut Vec<u8>) {
    out.push(if seconds < 0 { b'-' } else { b'+' });
    let a = seconds.unsigned_abs();
    let (h, rest) = (a / 3600, a % 3600);
    let (m, s) = (rest / 60, rest % 60);
    if h >= 100 {
        let mut b = itoa::Buffer::new();
        out.extend_from_slice(b.format(h).as_bytes());
    } else {
        push2(out, h);
    }
    if m != 0 || s != 0 {
        out.push(b':');
        push2(out, m);
    }
    if s != 0 {
        out.push(b':');
        push2(out, s);
    }
}

/// A timestamp to_pylist can hold: its UTC and its local time both within datetime's years. Its
/// local time and offset when it is.
fn local_of(micros: i128, tz: &TzSpec) -> R<(i128, Option<i32>)> {
    let lo = MIN_DAY as i128 * DAY_US;
    let hi = (MAX_DAY as i128 + 1) * DAY_US;
    let out_of_range = |v: i128| v < lo || v >= hi;
    if out_of_range(micros) {
        return Err(EncErr::Value("year is out of range".into()));
    }
    let offset = tz.offset_at(micros);
    let local = micros + offset.unwrap_or(0) as i128 * 1_000_000;
    if out_of_range(local) {
        return Err(EncErr::Value("year is out of range".into()));
    }
    Ok((local, offset))
}

pub fn check_timestamp(micros: i128, tz: &TzSpec) -> R<()> {
    local_of(micros, tz).map(|_| ())
}

/// _timestamp_text for a finite value: the local date and clock, and the offset of an aware one.
pub fn timestamp_text(micros: i128, tz: &TzSpec, out: &mut Vec<u8>) -> R<()> {
    let (local, offset) = local_of(micros, tz)?;
    let days = local.div_euclid(DAY_US) as i64;
    let tod = local.rem_euclid(DAY_US) as i64;
    push_ymd(days, out);
    out.push(b' ');
    clock_text(tod, out);
    if let Some(off) = offset {
        offset_text(off, out);
    }
    Ok(())
}

/// buenavista _interval_parts of a timedelta of `total` microseconds: its days, truncated toward
/// zero, and the rest. The product divides in floating point (int(micros / 86400e6)), which
/// rounds a day up within about ten milliseconds of the next near the billion-day limit.
fn timedelta_parts(total: i128) -> (i64, i64, i64) {
    let days = total / DAY_US;
    (0, days as i64, (total - days * DAY_US) as i64)
}

fn timedelta_in_range(total: i128) -> R<()> {
    if total.div_euclid(DAY_US).abs() > TIMEDELTA_MAX_DAYS {
        return Err(EncErr::Overflow("days is out of range".into()));
    }
    Ok(())
}

/// A duration column's value (a timedelta from to_pylist) as buenavista splits it.
pub fn interval_parts_of_duration(total: i128) -> R<(i64, i64, i64)> {
    timedelta_in_range(total)?;
    Ok(timedelta_parts(total))
}

/// A month-day-nano interval as column_values (ir_types.interval_value) hands it on and buenavista
/// splits it: a timedelta when it has no months and says the same fields back, else the fields.
pub fn interval_parts_of_mdn(months: i32, days: i32, nanos: i64) -> R<(i64, i64, i64)> {
    let micros = nanos.div_euclid(1000) as i128;
    let (months, days) = (months as i128, days as i128);
    if months == 0 && days.abs() <= TIMEDELTA_MAX_DAYS {
        let total = days * DAY_US + micros;
        timedelta_in_range(total)?;
        // ir_types.interval_parts of that timedelta
        let said = if total < 0 {
            (-((-total) / DAY_US), -((-total) % DAY_US))
        } else {
            (total / DAY_US, total % DAY_US)
        };
        if said == (days, micros) {
            return Ok(timedelta_parts(total));
        }
    }
    Ok((months as i64, days as i64, micros as i64))
}

/// _interval_text (IntervalStyle postgres)
pub fn interval_text(months: i64, days: i64, micros: i64, out: &mut Vec<u8>) {
    let years = months / 12;
    let months = months - years * 12;
    let mut any = false;
    let mut after_negative = false;
    for (n, unit) in [(years, "year"), (months, "mon"), (days, "day")] {
        if n != 0 {
            if any {
                out.push(b' ');
            }
            if after_negative && n > 0 {
                out.push(b'+');
            }
            let mut b = itoa::Buffer::new();
            out.extend_from_slice(b.format(n).as_bytes());
            out.push(b' ');
            out.extend_from_slice(unit.as_bytes());
            if n != 1 {
                out.push(b's');
            }
            any = true;
            after_negative = n < 0;
        }
    }
    if micros != 0 || !any {
        if any {
            out.push(b' ');
        }
        let a = micros.unsigned_abs();
        let hours = a / 3_600_000_000;
        let rem = a % 3_600_000_000;
        let minutes = rem / 60_000_000;
        let rem = rem % 60_000_000;
        let (secs, frac) = (rem / 1_000_000, rem % 1_000_000);
        if micros < 0 {
            out.push(b'-');
        } else if after_negative {
            out.push(b'+');
        }
        if hours < 10 {
            out.push(b'0');
        }
        let mut b = itoa::Buffer::new();
        out.extend_from_slice(b.format(hours).as_bytes());
        out.push(b':');
        push2(out, minutes as u32);
        out.push(b':');
        push2(out, secs as u32);
        if frac != 0 {
            let mut digits = [b'0'; 6];
            let mut f = frac;
            for k in (0..6).rev() {
                digits[k] = b'0' + (f % 10) as u8;
                f /= 10;
            }
            let mut n = 6;
            while digits[n - 1] == b'0' {
                n -= 1;
            }
            out.push(b'.');
            out.extend_from_slice(&digits[..n]);
        }
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date, any year (Howard Hinnant's
/// days_from_civil).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The days in a month of a proleptic Gregorian year.
pub fn month_days(y: i64, m: u32) -> i64 {
    match m {
        4 | 6 | 9 | 11 => 30,
        2 if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) => 29,
        2 => 28,
        _ => 31,
    }
}
