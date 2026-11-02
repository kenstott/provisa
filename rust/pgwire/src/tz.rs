// A named time zone's UTC offset at an instant, as zoneinfo and PostgreSQL find it: chrono-tz's
// tables (the zone's transitions, its rules expanded for 1800..2099), then from 2100 the zone's
// POSIX TZ rule, which zoneinfo and PostgreSQL apply after a zone's last explicit transition.

use chrono::{DateTime, Offset, TimeZone};
use chrono_tz::Tz;

use crate::tz_rules::RULES;

/// 2100-01-01T00:00:00Z: chrono-tz holds no rule transition from here on.
const RULE_FROM: i64 = 4_102_444_800;

/// M<month>.<week>.<weekday>/<time>: the weekday (0 Sunday) of the week (5 the last) of the month,
/// at a time of day in seconds that may pass either end of the day.
#[derive(Debug, Clone, Copy)]
struct RuleDay {
    month: u32,
    week: u32,
    weekday: i64,
    time: i64,
}

#[derive(Debug, Clone, Copy)]
struct Posix {
    /// Standard and daylight offsets, seconds east of UTC.
    std: i64,
    dst: Option<(i64, RuleDay, RuleDay)>,
}

#[derive(Debug, Clone, Copy)]
pub struct Zone {
    tz: Tz,
    rule: Posix,
}

impl PartialEq for Zone {
    fn eq(&self, other: &Zone) -> bool {
        self.tz == other.tz
    }
}

impl Zone {
    pub fn named(name: &str) -> Option<Zone> {
        let tz: Tz = name.parse().ok()?;
        let i = RULES.binary_search_by(|(n, _)| (*n).cmp(name)).ok()?;
        let rule = parse_posix(RULES[i].1).unwrap_or_else(|| panic!("tz_rules.rs: bad rule for {name}"));
        Some(Zone { tz, rule })
    }

    /// The offset in seconds east of UTC at `secs` after 1970-01-01T00:00:00Z, which lies within
    /// years 1..=9999.
    pub fn offset_at(&self, secs: i64) -> i32 {
        if secs >= RULE_FROM {
            return self.rule.offset_at(secs) as i32;
        }
        let utc = DateTime::from_timestamp(secs, 0)
            .expect("instant within years 1..=9999")
            .naive_utc();
        self.tz.offset_from_utc_datetime(&utc).fix().local_minus_utc()
    }
}

/// Days since 1970-01-01 of a civil date (Howard Hinnant's days_from_civil).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn month_days(y: i64, m: u32) -> i64 {
    let next = if m == 12 {
        days_from_civil(y + 1, 1, 1)
    } else {
        days_from_civil(y, m + 1, 1)
    };
    next - days_from_civil(y, m, 1)
}

impl RuleDay {
    /// The transition's local time, in seconds after 1970-01-01T00:00:00 local, in year `y`.
    fn local(self, y: i64) -> i64 {
        let first = days_from_civil(y, self.month, 1);
        // 1970-01-01 was a Thursday (4).
        let mut day = first + (self.weekday - (first + 4).rem_euclid(7)).rem_euclid(7) + 7 * (self.week as i64 - 1);
        while day >= first + month_days(y, self.month) {
            day -= 7;
        }
        day * 86_400 + self.time
    }
}

impl Posix {
    fn offset_at(&self, secs: i64) -> i64 {
        let Some((dst, start, end)) = self.dst else {
            return self.std;
        };
        let year = crate::temporal::civil(secs.div_euclid(86_400)).0;
        // DST begins at a local standard time and ends at a local daylight time; a southern zone's
        // DST runs from one year's start into the next year's end.
        for y in year - 1..=year + 1 {
            let on = start.local(y) - self.std;
            let off_same = end.local(y) - dst;
            let off = if on < off_same {
                off_same
            } else {
                end.local(y + 1) - dst
            };
            if on <= secs && secs < off {
                return dst;
            }
        }
        self.std
    }
}

/// A zone abbreviation: alphabetic, or anything in angle brackets.
fn take_name(s: &str) -> Option<&str> {
    if let Some(rest) = s.strip_prefix('<') {
        return Some(&rest[rest.find('>')? + 1..]);
    }
    let n = s.find(|c: char| !c.is_ascii_alphabetic()).unwrap_or(s.len());
    (n >= 3).then(|| &s[n..])
}

/// [+-]hh[:mm[:ss]] in seconds, and the rest.
fn take_hms(s: &str) -> Option<(i64, &str)> {
    let (sign, s) = match s.as_bytes().first()? {
        b'-' => (-1, &s[1..]),
        b'+' => (1, &s[1..]),
        _ => (1, s),
    };
    let end = s.find(|c: char| !(c.is_ascii_digit() || c == ':')).unwrap_or(s.len());
    let mut secs = 0;
    let mut scale = 3600;
    for part in s[..end].split(':') {
        secs += part.parse::<i64>().ok()? * scale;
        scale /= 60;
    }
    Some((sign * secs, &s[end..]))
}

fn take_rule_day(s: &str) -> Option<(RuleDay, &str)> {
    let s = s.strip_prefix(",M")?;
    let end = s.find(['/', ',']).unwrap_or(s.len());
    let mut f = s[..end].split('.');
    let month = f.next()?.parse().ok()?;
    let week = f.next()?.parse().ok()?;
    let weekday = f.next()?.parse().ok()?;
    let (time, rest) = match s[end..].strip_prefix('/') {
        Some(t) => take_hms(t)?,
        None => (7200, &s[end..]),
    };
    Some((
        RuleDay {
            month,
            week,
            weekday,
            time,
        },
        rest,
    ))
}

/// A TZif footer: std offset [dst [offset] ,start[/time],end[/time]], offsets west of UTC.
fn parse_posix(s: &str) -> Option<Posix> {
    let s = take_name(s)?;
    let (west, s) = take_hms(s)?;
    let std = -west;
    if s.is_empty() {
        return Some(Posix { std, dst: None });
    }
    let s = take_name(s)?;
    let (dst, s) = if s.starts_with(',') {
        (std + 3600, s)
    } else {
        let (west, rest) = take_hms(s)?;
        (-west, rest)
    };
    let (start, s) = take_rule_day(s)?;
    let (end, s) = take_rule_day(s)?;
    s.is_empty().then_some(Posix {
        std,
        dst: Some((dst, start, end)),
    })
}
