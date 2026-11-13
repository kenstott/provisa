// The coercers ProvisaQueryResult applies to a column's values before its encoder runs
// (provisa/pgwire/server.py _SHAPE_COERCERS, _ENGINE_VALUES; provisa/pgwire/cast_types.py's
// CastType.coerce), each over a cell's value in Rust. The wiring names each column's coercers
// (python_ref.coercer_names); a name here is that coercer.

use crate::pyval::{
    self, dec_of_str, str_of, to_decimal, to_float, to_int, truthy, type_error, DateTime, Dec, DecSpecial, Inet, PyVal,
    TzRepr,
};
use crate::temporal::DAY_US;
use crate::{EncErr, R};

#[derive(Clone, Debug, PartialEq)]
pub enum Coercer {
    /// int (server's) and cast_types._to_int
    Int,
    /// float and cast_types._to_float
    Float,
    /// server._to_decimal and cast_types._to_decimal
    Decimal,
    /// server._to_date
    Date,
    /// server._to_timestamp
    Timestamp,
    /// server._to_list
    List,
    /// duckdb_pg_xml.xml_out
    XmlOut,
    /// cast_types._padded(n)
    Pad(usize),
    /// cast_types._to_bool
    Bool,
    /// pg_forms_json.jsonb_out
    JsonbOut,
    /// cast_types.record_text
    Record,
    /// cast_types._clock_of
    Clock,
    /// cast_types._of_no_zone
    NoZone,
    /// server._null_alone
    NullAlone,
    /// server._duckdb_inet
    DuckdbInet,
    /// pg_enum.array_text: an enum array as array_out writes it
    EnumArray,
    /// cast_types.interval_of_text: interval_out's text, which an engine holds an interval as
    IntervalText,
    /// cast_types.bytes_of_text: a string an engine holds bytes as, as those bytes
    TextBytes,
    /// cast_types.clock_of_text: time_out's text, which an engine holds a time as
    ClockText,
}

pub fn coercer_of(name: &str) -> R<Coercer> {
    Ok(match name {
        "int" => Coercer::Int,
        "float" => Coercer::Float,
        "decimal" => Coercer::Decimal,
        "date" => Coercer::Date,
        "timestamp" => Coercer::Timestamp,
        "list" => Coercer::List,
        "xml_out" => Coercer::XmlOut,
        "bool" => Coercer::Bool,
        "jsonb_out" => Coercer::JsonbOut,
        "record" => Coercer::Record,
        "clock" => Coercer::Clock,
        "no_zone" => Coercer::NoZone,
        "null_alone" => Coercer::NullAlone,
        "duckdb_inet" => Coercer::DuckdbInet,
        "enum_array" => Coercer::EnumArray,
        "interval_text" => Coercer::IntervalText,
        "text_bytes" => Coercer::TextBytes,
        "clock_text" => Coercer::ClockText,
        _ => match name.strip_prefix("pad:").and_then(|n| n.parse().ok()) {
            Some(n) => Coercer::Pad(n),
            None => return Err(EncErr::Value(format!("no coercer named {name:?}"))),
        },
    })
}

/// The coercer applied to a value (not None).
pub fn apply(c: &Coercer, v: PyVal) -> R<PyVal> {
    Ok(match c {
        Coercer::Int => pyval::to_int_value(&v)?,
        Coercer::Float => PyVal::Float(to_float(&v)?),
        Coercer::Decimal => PyVal::Dec(to_decimal(&v)?),
        // A datetime's date; an infinite timestamp is the infinite date (the product's .date()
        // loses the infinity).
        Coercer::Date => match v {
            PyVal::DateTime(d) if d.inf != 0 => PyVal::Date {
                days: if d.inf > 0 { 2_932_896 } else { -719_162 },
                inf: d.inf,
            },
            PyVal::DateTime(d) => PyVal::Date {
                days: d.local.div_euclid(DAY_US) as i64,
                inf: 0,
            },
            other => other,
        },
        // A date's midnight; an infinite date is the infinite timestamp (the product's
        // datetime(y, m, d) loses the infinity).
        Coercer::Timestamp => match v {
            PyVal::Date { days, inf } => PyVal::DateTime(DateTime {
                local: if inf > 0 {
                    2_932_897 * DAY_US - 1
                } else {
                    days as i128 * DAY_US
                },
                offset: None,
                tz: TzRepr::Naive,
                inf,
                ns: None,
            }),
            other => other,
        },
        // server._to_list: PostgreSQL's text of a bounded array is sent as it is (REQ-2167).
        Coercer::List => match v {
            PyVal::Str(s) if !crate::pgtypes::is_bounded_array_text(&s) => json_loads(&s, false)?,
            other => other,
        },
        Coercer::XmlOut => match v {
            PyVal::Str(s) => PyVal::Str(xml_out(&s)),
            other => return Err(type_error("object has no attribute 'startswith'", &other)),
        },
        Coercer::Pad(n) => match v {
            PyVal::Str(s) => {
                let mut t: String = s.chars().take(*n).collect();
                let len = t.chars().count();
                t.extend(std::iter::repeat_n(' ', n - len));
                PyVal::Str(t)
            }
            other => other,
        },
        Coercer::Bool => PyVal::Bool(truthy(&v)),
        Coercer::JsonbOut => match v {
            PyVal::Str(s) if !s.is_empty() => {
                let parsed = json_loads(&s, true)?;
                let mut out = String::new();
                jsonb_written(&parsed, &mut out)?;
                PyVal::Str(out)
            }
            other => other,
        },
        Coercer::Record => match v {
            PyVal::Tuple(items) => PyVal::Str(record(items.iter())),
            PyVal::Dict(items) => PyVal::Str(record(items.iter().map(|(_, v)| v))),
            other => other,
        },
        Coercer::Clock => match v {
            PyVal::Bytes(b) => {
                // int.from_bytes(little, signed) microseconds after datetime.min, its time of day
                if b.len() > 16 {
                    return Err(EncErr::Overflow("date value out of range".into()));
                }
                let mut buf = [if b.last().is_some_and(|&x| x & 0x80 != 0) {
                    0xff
                } else {
                    0
                }; 16];
                buf[..b.len()].copy_from_slice(&b);
                let count = i128::from_le_bytes(buf);
                if !(0..3_652_059 * DAY_US).contains(&count) {
                    return Err(EncErr::Overflow("date value out of range".into()));
                }
                PyVal::Time((count % DAY_US) as i64)
            }
            other => other,
        },
        // An aware value's wall clock; an infinite one stays infinite (the product's replace()
        // loses the infinity).
        Coercer::NoZone => match v {
            PyVal::DateTime(d) if d.offset.is_some() => PyVal::DateTime(DateTime {
                offset: None,
                tz: TzRepr::Naive,
                ..d
            }),
            other => other,
        },
        Coercer::NullAlone => {
            return Err(EncErr::Type(format!(
                "a value of no type the statement says: {}",
                repr(&v)
            )))
        }
        Coercer::DuckdbInet => match v {
            PyVal::Dict(_) => duckdb_inet(&v)?,
            other => other,
        },
        Coercer::EnumArray => match v {
            PyVal::List(items) | PyVal::Tuple(items) => {
                let mut out = String::from("{");
                for (k, item) in items.iter().enumerate() {
                    if k > 0 {
                        out.push(',');
                    }
                    match item {
                        PyVal::None => out.push_str("NULL"),
                        other => enum_element(&str_of(other), &mut out),
                    }
                }
                out.push('}');
                PyVal::Str(out)
            }
            other => other,
        },
        Coercer::IntervalText => match v {
            PyVal::Str(s) => {
                let (months, days, micros) = interval_fields(&s)
                    .ok_or_else(|| EncErr::Value(format!("invalid input syntax for type interval: \"{s}\"")))?;
                pyval::interval_value(months, days, micros)
            }
            other => other,
        },
        Coercer::TextBytes => match v {
            PyVal::Str(s) => PyVal::Bytes(s.into_bytes()),
            other => other,
        },
        Coercer::ClockText => match v {
            PyVal::Str(s) => PyVal::Time(
                clock_of_text(&s)
                    .ok_or_else(|| EncErr::Value(format!("invalid input syntax for type time: \"{s}\"")))?,
            ),
            other => other,
        },
    })
}

/// An interval's (months, days, microseconds) of the text interval_out writes (IntervalStyle
/// postgres): "1 year 2 mons -3 days +04:05:06.5"; None for any other text. As
/// cast_types.interval_of_text reads it.
pub fn interval_fields(text: &str) -> Option<(i128, i128, i128)> {
    let words: Vec<&str> = text.split(' ').collect();
    let (mut months, mut days, mut micros) = (0i128, 0i128, 0i128);
    let mut at = 0;
    while at < words.len() {
        let word = words[at];
        if let Some(clock) = clock_micros(word) {
            micros += clock;
            at += 1;
            continue;
        }
        let amount: i128 = word.strip_prefix('+').unwrap_or(word).parse().ok()?;
        match *words.get(at + 1)? {
            "year" | "years" => months += amount * 12,
            "mon" | "mons" => months += amount,
            "day" | "days" => days += amount,
            _ => return None,
        }
        at += 2;
    }
    (!text.is_empty()).then_some((months, days, micros))
}

/// time_out's text, "HH:MM:SS[.ffffff]", as its microseconds since midnight; None for any other
/// text, and for 24:00:00, which is no time of day. As cast_types.clock_of_text reads it.
pub fn clock_of_text(text: &str) -> Option<i64> {
    let b = text.as_bytes();
    let two = |at: usize| -> Option<i64> {
        let (h, l) = (*b.get(at)?, *b.get(at + 1)?);
        (h.is_ascii_digit() && l.is_ascii_digit()).then(|| i64::from(h - b'0') * 10 + i64::from(l - b'0'))
    };
    if b.len() < 8 || b[2] != b':' || b[5] != b':' {
        return None;
    }
    let (hours, minutes, seconds) = (two(0)?, two(3)?, two(6)?);
    if hours > 23 || minutes > 59 || seconds > 59 {
        return None;
    }
    let fraction = match &b[8..] {
        [] => 0,
        [b'.', digits @ ..] if (1..=6).contains(&digits.len()) && digits.iter().all(u8::is_ascii_digit) => {
            let padded = format!("{:0<6}", std::str::from_utf8(digits).ok()?);
            padded.parse::<i64>().ok()?
        }
        _ => return None,
    };
    Some(((hours * 60 + minutes) * 60 + seconds) * 1_000_000 + fraction)
}

/// "[+-]H:MM:SS[.ffffff]" as its signed microseconds.
fn clock_micros(word: &str) -> Option<i128> {
    let (sign, rest) = match word.as_bytes().first()? {
        b'-' => (-1, &word[1..]),
        b'+' => (1, &word[1..]),
        _ => (1, word),
    };
    let mut parts = rest.splitn(3, ':');
    let (hours, minutes, seconds) = (parts.next()?, parts.next()?, parts.next()?);
    let (whole, fraction) = seconds.split_once('.').unwrap_or((seconds, ""));
    let digits = |t: &str| !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit());
    let fine = digits(hours) && minutes.len() == 2 && digits(minutes) && whole.len() == 2 && digits(whole);
    if !fine || fraction.len() > 6 || !(fraction.is_empty() || digits(fraction)) {
        return None;
    }
    let number = |t: &str| t.parse::<i128>().ok();
    let total = (number(hours)? * 3600 + number(minutes)? * 60 + number(whole)?) * 1_000_000
        + if fraction.is_empty() {
            0
        } else {
            number(&format!("{fraction:0<6}"))?
        };
    Some(sign * total)
}

fn repr(v: &PyVal) -> String {
    let mut s = String::new();
    pyval::py_repr(v, &mut s);
    s
}

/// DuckDB's INET (ip_type, address, mask) as the address it is. Its address reaches Arrow as a
/// decimal128(38, 0) (a HUGEINT), an IPv6 one with its top bit flipped; the product's
/// _duckdb_inet takes only an int there, and raises on the Arrow value.
fn duckdb_inet(v: &PyVal) -> R<PyVal> {
    let PyVal::Dict(items) = v else { unreachable!() };
    let field = |name: &str| {
        items
            .iter()
            .find(|(k, _)| matches!(k, PyVal::Str(s) if s == name))
            .map(|(_, v)| v)
            .ok_or_else(|| EncErr::Value(format!("KeyError: {name:?}")))
    };
    let ip_type = to_int(field("ip_type")?)?;
    let address = to_int(field("address")?)?;
    let mask = to_int(field("mask")?)?;
    let (v6, addr, max) = if ip_type == 1 {
        (false, address, 32)
    } else {
        (true, address.wrapping_add(i128::MIN), 128)
    };
    if !(0..=max).contains(&mask) || (!v6 && !(0..=u32::MAX as i128).contains(&addr)) {
        return Err(EncErr::Value(format!("not an address: {}", repr(v))));
    }
    Ok(PyVal::Inet(Inet {
        v6,
        addr: addr as u128,
        bits: mask as u8,
    }))
}

/// _record_field: nothing for NULL, quoted where it must be.
fn record_field(v: &PyVal, out: &mut String) {
    let text = match v {
        PyVal::None => return,
        PyVal::List(items) => {
            let inner: Vec<String> = items
                .iter()
                .map(|x| {
                    if *x == PyVal::None {
                        "NULL".to_string()
                    } else {
                        str_of(x)
                    }
                })
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        PyVal::Bool(b) => (if *b { "t" } else { "f" }).to_string(),
        _ => str_of(v),
    };
    if text.is_empty() || text.chars().any(|c| "\",\\() \t\n".contains(c)) {
        out.push('"');
        out.push_str(&text.replace('\\', "\\\\").replace('"', "\"\""));
        out.push('"');
    } else {
        out.push_str(&text);
    }
}

fn record<'a>(fields: impl Iterator<Item = &'a PyVal>) -> String {
    let mut out = String::from("(");
    for (k, f) in fields.enumerate() {
        if k > 0 {
            out.push(',');
        }
        record_field(f, &mut out);
    }
    out.push(')');
    out
}

// --- xml_out ---

const XML_SPACE: &[u8] = b" \t\r\n";

fn skip_space(t: &[u8], mut p: usize) -> usize {
    while p < t.len() && XML_SPACE.contains(&t[p]) {
        p += 1;
    }
    p
}

fn quoted(t: &[u8], p: usize) -> Option<(&[u8], usize)> {
    let q = *t.get(p)?;
    if q != b'\'' && q != b'"' {
        return None;
    }
    let end = t[p + 1..].iter().position(|&c| c == q)? + p + 1;
    Some((&t[p + 1..end], end + 1))
}

fn named_value(t: &[u8], p: usize, name: &str) -> Option<usize> {
    let after = skip_space(t, p + name.len());
    if t.get(after) != Some(&b'=') {
        return None;
    }
    Some(skip_space(t, after + 1))
}

/// parse_declaration: the declaration's length, version and standalone (1, 0, -1) when it is
/// well formed.
fn parse_declaration(text: &str) -> Option<(usize, Option<String>, i32)> {
    let t = text.as_bytes();
    let next = text.get(5..).and_then(|r| r.chars().next());
    let name_char = |c: char| c.is_alphanumeric() || c == '_' || ":.-·".contains(c);
    if !text.starts_with("<?xml") || next.is_some_and(name_char) {
        return Some((0, None, -1));
    }
    let mut p = 5;
    if !t.get(p).is_some_and(|c| XML_SPACE.contains(c)) {
        return None;
    }
    p = skip_space(t, p);
    if !t[p..].starts_with(b"version") {
        return None;
    }
    let (v, end) = quoted(t, named_value(t, p, "version")?)?;
    let version = String::from_utf8_lossy(v).to_string();
    p = end;
    let saved = p;
    p = skip_space(t, p);
    if t[p..].starts_with(b"encoding") {
        if !t.get(saved).is_some_and(|c| XML_SPACE.contains(c)) {
            return None;
        }
        let (_, end) = quoted(t, named_value(t, p, "encoding")?)?;
        p = end;
    } else {
        p = saved;
    }
    let mut standalone = -1;
    let saved = p;
    p = skip_space(t, p);
    if t[p..].starts_with(b"standalone") {
        if !t.get(saved).is_some_and(|c| XML_SPACE.contains(c)) {
            return None;
        }
        let at = named_value(t, p, "standalone")?;
        let mut found = None;
        for (spelled, value) in [("'yes'", 1), ("\"yes\"", 1), ("'no'", 0), ("\"no\"", 0)] {
            if t[at..].starts_with(spelled.as_bytes()) {
                found = Some((value, at + spelled.len()));
                break;
            }
        }
        let (value, end) = found?;
        standalone = value;
        p = end;
    } else {
        p = saved;
    }
    p = skip_space(t, p);
    if !t[p..].starts_with(b"?>") {
        return None;
    }
    p += 2;
    if !t[..p].is_ascii() {
        return None;
    }
    Some((p, Some(version), standalone))
}

/// xml_out: the value with its declaration written as print_xml_decl writes it.
pub fn xml_out(value: &str) -> String {
    let Some((length, version, standalone)) = parse_declaration(value) else {
        return value.to_string();
    };
    let declaration = match (&version, standalone) {
        (Some(v), _) if v != "1.0" => Some(v.clone()),
        (_, s) if s != -1 => Some(version.clone().unwrap_or_else(|| "1.0".into())),
        _ => None,
    };
    let mut rest = &value[length..];
    match declaration {
        None => {
            if let Some(r) = rest.strip_prefix('\n') {
                rest = r;
            }
            rest.to_string()
        }
        Some(v) => {
            let said = match standalone {
                1 => " standalone=\"yes\"",
                0 => " standalone=\"no\"",
                _ => "",
            };
            format!("<?xml version=\"{v}\"{said}?>{rest}")
        }
    }
}

// --- json.loads and jsonb_out ---

struct JsonParser<'a> {
    c: Vec<char>,
    p: usize,
    text: &'a str,
    decimals: bool,
}

impl JsonParser<'_> {
    fn err(&self) -> EncErr {
        EncErr::Value(format!("JSONDecodeError at {} in {:?}", self.p, self.text))
    }
    fn ws(&mut self) {
        while self.p < self.c.len() && matches!(self.c[self.p], ' ' | '\t' | '\n' | '\r') {
            self.p += 1;
        }
    }
    fn lit(&mut self, w: &str) -> bool {
        let wc: Vec<char> = w.chars().collect();
        if self.c.len() >= self.p + wc.len() && self.c[self.p..self.p + wc.len()] == wc[..] {
            self.p += wc.len();
            true
        } else {
            false
        }
    }
    fn value(&mut self) -> R<PyVal> {
        self.ws();
        let ch = *self.c.get(self.p).ok_or_else(|| self.err())?;
        match ch {
            '{' => {
                self.p += 1;
                let mut items: Vec<(PyVal, PyVal)> = Vec::new();
                self.ws();
                if self.c.get(self.p) == Some(&'}') {
                    self.p += 1;
                    return Ok(PyVal::Dict(items));
                }
                loop {
                    self.ws();
                    if self.c.get(self.p) != Some(&'"') {
                        return Err(self.err());
                    }
                    let key = PyVal::Str(self.string()?);
                    self.ws();
                    if self.c.get(self.p) != Some(&':') {
                        return Err(self.err());
                    }
                    self.p += 1;
                    let v = self.value()?;
                    match items.iter_mut().find(|(k, _)| *k == key) {
                        Some(slot) => slot.1 = v,
                        None => items.push((key, v)),
                    }
                    self.ws();
                    match self.c.get(self.p) {
                        Some(',') => self.p += 1,
                        Some('}') => {
                            self.p += 1;
                            return Ok(PyVal::Dict(items));
                        }
                        _ => return Err(self.err()),
                    }
                }
            }
            '[' => {
                self.p += 1;
                let mut items = Vec::new();
                self.ws();
                if self.c.get(self.p) == Some(&']') {
                    self.p += 1;
                    return Ok(PyVal::List(items));
                }
                loop {
                    items.push(self.value()?);
                    self.ws();
                    match self.c.get(self.p) {
                        Some(',') => self.p += 1,
                        Some(']') => {
                            self.p += 1;
                            return Ok(PyVal::List(items));
                        }
                        _ => return Err(self.err()),
                    }
                }
            }
            '"' => Ok(PyVal::Str(self.string()?)),
            _ if self.lit("null") => Ok(PyVal::None),
            _ if self.lit("true") => Ok(PyVal::Bool(true)),
            _ if self.lit("false") => Ok(PyVal::Bool(false)),
            _ if self.lit("NaN") => Ok(PyVal::Float(f64::NAN)),
            _ if self.lit("Infinity") => Ok(PyVal::Float(f64::INFINITY)),
            _ if self.lit("-Infinity") => Ok(PyVal::Float(f64::NEG_INFINITY)),
            _ => self.number(),
        }
    }
    fn number(&mut self) -> R<PyVal> {
        // -?(0|[1-9]\d*)(\.\d+)?([eE][-+]?\d+)?
        let start = self.p;
        let digit = |p: usize, c: &Vec<char>| c.get(p).is_some_and(|x| x.is_ascii_digit());
        if self.c.get(self.p) == Some(&'-') {
            self.p += 1;
        }
        if self.c.get(self.p) == Some(&'0') {
            self.p += 1;
        } else if digit(self.p, &self.c) {
            while digit(self.p, &self.c) {
                self.p += 1;
            }
        } else {
            self.p = start;
            return Err(self.err());
        }
        let mut is_float = false;
        if self.c.get(self.p) == Some(&'.') && digit(self.p + 1, &self.c) {
            self.p += 1;
            while digit(self.p, &self.c) {
                self.p += 1;
            }
            is_float = true;
        }
        if matches!(self.c.get(self.p), Some('e' | 'E')) {
            let mut q = self.p + 1;
            if matches!(self.c.get(q), Some('-' | '+')) {
                q += 1;
            }
            if digit(q, &self.c) {
                self.p = q;
                while digit(self.p, &self.c) {
                    self.p += 1;
                }
                is_float = true;
            }
        }
        let tok: String = self.c[start..self.p].iter().collect();
        if self.decimals {
            return Ok(PyVal::Dec(dec_of_str(&tok)?));
        }
        if is_float {
            Ok(PyVal::Float(tok.parse().map_err(|_| self.err())?))
        } else {
            Ok(match tok.parse::<i128>() {
                Ok(n) => PyVal::Int(n),
                Err(_) => PyVal::BigInt(tok),
            })
        }
    }
    fn string(&mut self) -> R<String> {
        self.p += 1;
        let mut s = String::new();
        loop {
            let ch = *self.c.get(self.p).ok_or_else(|| self.err())?;
            self.p += 1;
            match ch {
                '"' => return Ok(s),
                '\\' => {
                    let e = *self.c.get(self.p).ok_or_else(|| self.err())?;
                    self.p += 1;
                    match e {
                        '"' => s.push('"'),
                        '\\' => s.push('\\'),
                        '/' => s.push('/'),
                        'b' => s.push('\u{8}'),
                        'f' => s.push('\u{c}'),
                        'n' => s.push('\n'),
                        'r' => s.push('\r'),
                        't' => s.push('\t'),
                        'u' => {
                            let unit = self.hex4()?;
                            if (0xd800..0xdc00).contains(&unit)
                                && self.c.get(self.p) == Some(&'\\')
                                && self.c.get(self.p + 1) == Some(&'u')
                            {
                                let save = self.p;
                                self.p += 2;
                                let low = self.hex4()?;
                                if (0xdc00..0xe000).contains(&low) {
                                    let cp = 0x10000 + ((unit - 0xd800) << 10) + (low - 0xdc00);
                                    s.push(char::from_u32(cp).expect("a surrogate pair"));
                                    continue;
                                }
                                self.p = save;
                            }
                            // a lone surrogate: a str UTF-8 cannot encode
                            s.push(
                                char::from_u32(unit)
                                    .ok_or_else(|| EncErr::Value("'utf-8' codec can't encode a surrogate".into()))?,
                            );
                        }
                        _ => return Err(self.err()),
                    }
                }
                c if (c as u32) < 0x20 => return Err(self.err()),
                c => s.push(c),
            }
        }
    }
    fn hex4(&mut self) -> R<u32> {
        let h: String = self
            .c
            .get(self.p..self.p + 4)
            .ok_or_else(|| self.err())?
            .iter()
            .collect();
        if !h.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(self.err());
        }
        self.p += 4;
        Ok(u32::from_str_radix(&h, 16).expect("hex"))
    }
}

/// json.loads(text) (with `decimals`: parse_float=Decimal, parse_int=Decimal).
pub fn json_loads(text: &str, decimals: bool) -> R<PyVal> {
    let mut p = JsonParser {
        c: text.chars().collect(),
        p: 0,
        text,
        decimals,
    };
    let v = p.value()?;
    p.ws();
    if p.p != p.c.len() {
        return Err(p.err());
    }
    Ok(v)
}

/// json.dumps(ensure_ascii=False) of a str.
fn json_str_unicode(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// format(Decimal, "f")
fn dec_f(d: &Dec, out: &mut String) {
    match d.special {
        DecSpecial::NaN => out.push_str("NaN"),
        DecSpecial::Infinity => out.push_str(if d.neg { "-Infinity" } else { "Infinity" }),
        DecSpecial::Finite => {
            if d.neg {
                out.push('-');
            }
            let digits = std::str::from_utf8(&d.digits).expect("ascii");
            if d.exp >= 0 {
                out.push_str(digits);
                if digits != "0" {
                    out.extend(std::iter::repeat_n('0', d.exp as usize));
                }
            } else {
                let f = (-d.exp) as usize;
                if digits.len() > f {
                    out.push_str(&digits[..digits.len() - f]);
                    out.push('.');
                    out.push_str(&digits[digits.len() - f..]);
                } else {
                    out.push_str("0.");
                    out.extend(std::iter::repeat_n('0', f - digits.len()));
                    out.push_str(digits);
                }
            }
        }
    }
}

/// jsonb_written: keys shortest first then by their bytes, ", " and ": " between.
fn jsonb_written(v: &PyVal, out: &mut String) -> R<()> {
    match v {
        PyVal::Dict(items) => {
            let mut keyed: Vec<(&String, &PyVal)> = items
                .iter()
                .map(|(k, v)| match k {
                    PyVal::Str(s) => (s, v),
                    _ => unreachable!("json object keys are str"),
                })
                .collect();
            keyed.sort_by(|a, b| (a.0.len(), a.0.as_bytes()).cmp(&(b.0.len(), b.0.as_bytes())));
            out.push('{');
            for (k, (key, value)) in keyed.iter().enumerate() {
                if k > 0 {
                    out.push_str(", ");
                }
                json_str_unicode(key, out);
                out.push_str(": ");
                jsonb_written(value, out)?;
            }
            out.push('}');
        }
        PyVal::List(items) => {
            out.push('[');
            for (k, item) in items.iter().enumerate() {
                if k > 0 {
                    out.push_str(", ");
                }
                jsonb_written(item, out)?;
            }
            out.push(']');
        }
        PyVal::Dec(d) => {
            let mut plain = String::new();
            pyval::dec_str(d, &mut plain);
            if d.is_zero() && !plain.contains('.') {
                out.push('0');
            } else {
                dec_f(d, out);
            }
        }
        PyVal::Str(s) => json_str_unicode(s, out),
        PyVal::None => out.push_str("null"),
        PyVal::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        PyVal::Float(x) => {
            let mut b = Vec::new();
            crate::fmt_num::float_repr(*x, &mut b);
            out.push_str(std::str::from_utf8(&b).expect("ascii"));
        }
        other => return Err(type_error("Object of this type is not JSON serializable", other)),
    }
    Ok(())
}

/// pg_enum._element: a label as an array element, quoted where array_out quotes it.
fn enum_element(label: &str, out: &mut String) {
    let plain = !label.is_empty()
        && !label.eq_ignore_ascii_case("NULL")
        && !label
            .chars()
            .any(|c| matches!(c, '{' | '}' | ',' | '"' | '\\') || c.is_whitespace() || crate::is_py_space(c));
    if plain {
        out.push_str(label);
        return;
    }
    out.push('"');
    for c in label.chars() {
        if c == '\\' || c == '"' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
}
