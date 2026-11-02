//! `json.loads` (Python's strict C scanner) into the Python values it gives: an int any size (the
//! 4,300-digit limit raises), a float literal as `serialize.JsonNumber` (the product's JSON
//! columns) or as a `Decimal` (OData's relationship values), the NaN and infinity constants
//! refused by name (JSON columns) or read as floats, a repeated key keeping its first position
//! and its last value, a lone `\uD800` escape kept as its surrogate.

use std::borrow::Cow;

use crate::pyval::{Error, Res, P};
use crate::text;

#[derive(Clone, Copy, PartialEq)]
pub enum Floats {
    /// `parse_float=JsonNumber, parse_constant=refuse` (`serialize._parse_json_text`).
    JsonNumber,
    /// `parse_float=Decimal`, constants as floats (`odata.expand._decoded`).
    Decimal,
}

/// Python 3.12's int-from-string limit (`sys.get_int_max_str_digits`).
const INT_MAX_STR_DIGITS: usize = 4300;
/// Nesting past this raises RecursionError in `json.loads` or the walk after it. The product's
/// limit depends on the caller's stack (about 1,000 frames less those in use); a value nested this
/// deep is refused by the response writer anyway (255 levels).
pub const MAX_PARSE_DEPTH: usize = 900;

pub enum Fail {
    /// JSONDecodeError: the message's kind (`Expecting value`, `Extra data`, ...).
    Decode(&'static str),
    /// A refused constant (`NaN`, `Infinity`, `-Infinity`).
    Constant(&'static str),
    Recursion,
    /// The int limit: a ValueError from `int()`.
    IntLimit,
}

pub fn parse(s: &[u8], floats: Floats) -> Result<P<'_>, Fail> {
    let mut p = Parser { s, i: 0, depth: 0, floats };
    p.ws();
    let v = p.value()?;
    p.ws();
    if p.i != s.len() {
        return Err(Fail::Decode("Extra data"));
    }
    Ok(v)
}

/// `serialize._parse_json_text`: JSON text a JSON-typed column holds, as its value, refused by
/// name where JSON cannot carry it.
pub fn parse_column_text<'a>(s: &'a [u8], field: &str) -> Res<P<'a>> {
    let refused = |reason: String| Error::ValueRefused(field.to_string(), reason);
    match parse(s, Floats::JsonNumber) {
        Ok(v) => {
            if has_lone_surrogate(&v) {
                return Err(refused("holds a lone UTF-16 surrogate escape, which is no character".into()));
            }
            Ok(v)
        }
        Err(Fail::Constant(t)) => Err(refused(format!("holds {t}, a number JSON cannot carry"))),
        Err(Fail::Recursion) => Err(refused("nests deeper than a JSON response can carry".into())),
        Err(Fail::Decode(kind)) => Err(refused(format!("is typed JSON but its text is not JSON ({kind})"))),
        Err(Fail::IntLimit) => Err(Error::Value("Exceeds the limit (4300 digits) for integer string conversion".into())),
    }
}

/// `json.loads(text, parse_float=Decimal)`: OData's reading of a relationship's JSON.
pub fn parse_decimal_text(s: &[u8]) -> Res<P<'_>> {
    match parse(s, Floats::Decimal) {
        Ok(v) => Ok(v),
        Err(Fail::Decode(kind)) => Err(Error::Value(format!("JSONDecodeError: {kind}"))),
        Err(Fail::Recursion) => Err(Error::Value("RecursionError".into())),
        Err(Fail::IntLimit) => Err(Error::Value("Exceeds the limit (4300 digits) for integer string conversion".into())),
        Err(Fail::Constant(_)) => unreachable!(),
    }
}

pub fn has_surrogate(s: &[u8]) -> bool {
    s.windows(2).any(|w| w[0] == 0xED && w[1] >= 0xA0)
}

fn has_lone_surrogate(v: &P) -> bool {
    match v {
        P::Str(s) => has_surrogate(s),
        P::List(items) => items.iter().any(has_lone_surrogate),
        P::Dict(e) => e.iter().any(|(k, x)| has_surrogate(k) || has_lone_surrogate(x)),
        _ => false,
    }
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
    depth: usize,
    floats: Floats,
}

type PR<T> = Result<T, Fail>;

impl<'a> Parser<'a> {
    #[allow(clippy::needless_lifetimes)]
    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }

    fn lit(&mut self, w: &[u8]) -> bool {
        if self.s[self.i..].starts_with(w) {
            self.i += w.len();
            true
        } else {
            false
        }
    }

    fn constant(&mut self, name: &'static str, v: f64) -> PR<P<'a>> {
        match self.floats {
            Floats::JsonNumber => Err(Fail::Constant(name)),
            Floats::Decimal => Ok(P::Float(v)),
        }
    }

    fn value(&mut self) -> PR<P<'a>> {
        match self.peek().ok_or(Fail::Decode("Expecting value"))? {
            b'{' => self.object(),
            b'[' => self.array(),
            b'"' => Ok(P::Str(self.string()?)),
            b'n' if self.lit(b"null") => Ok(P::None),
            b't' if self.lit(b"true") => Ok(P::Bool(true)),
            b'f' if self.lit(b"false") => Ok(P::Bool(false)),
            b'N' if self.lit(b"NaN") => self.constant("NaN", f64::NAN),
            b'I' if self.lit(b"Infinity") => self.constant("Infinity", f64::INFINITY),
            b'-' if self.s[self.i..].starts_with(b"-Infinity") => {
                self.i += 9;
                self.constant("-Infinity", f64::NEG_INFINITY)
            }
            _ => self.number(),
        }
    }

    fn number(&mut self) -> PR<P<'a>> {
        let s = self.s;
        let start = self.i;
        let mut i = self.i;
        if s.get(i) == Some(&b'-') {
            i += 1;
        }
        match s.get(i) {
            Some(b'0') => i += 1,
            Some(b'1'..=b'9') => {
                while matches!(s.get(i), Some(b'0'..=b'9')) {
                    i += 1;
                }
            }
            _ => return Err(Fail::Decode("Expecting value")),
        }
        let int_end = i;
        let mut is_float = false;
        if s.get(i) == Some(&b'.') && matches!(s.get(i + 1), Some(b'0'..=b'9')) {
            i += 1;
            while matches!(s.get(i), Some(b'0'..=b'9')) {
                i += 1;
            }
            is_float = true;
        }
        if matches!(s.get(i), Some(b'e' | b'E')) {
            let mut j = i + 1;
            if matches!(s.get(j), Some(b'+' | b'-')) {
                j += 1;
            }
            if matches!(s.get(j), Some(b'0'..=b'9')) {
                while matches!(s.get(j), Some(b'0'..=b'9')) {
                    j += 1;
                }
                i = j;
                is_float = true;
            }
        }
        self.i = i;
        let text = std::str::from_utf8(&s[start..i]).unwrap();
        if is_float {
            return Ok(match self.floats {
                Floats::JsonNumber => P::JNum(Cow::Borrowed(text)),
                Floats::Decimal => P::Dec(text::decimal_of_number(text)),
            });
        }
        let digits = &s[start..int_end];
        let neg = digits[0] == b'-';
        let body = if neg { &digits[1..] } else { digits };
        if body.len() > INT_MAX_STR_DIGITS {
            return Err(Fail::IntLimit);
        }
        Ok(match text.parse::<i128>() {
            Ok(v) => P::Int(v),
            Err(_) => P::BigInt(Cow::Borrowed(text)),
        })
    }

    fn enter(&mut self) -> PR<()> {
        self.depth += 1;
        if self.depth > MAX_PARSE_DEPTH {
            return Err(Fail::Recursion);
        }
        Ok(())
    }

    fn array(&mut self) -> PR<P<'a>> {
        self.enter()?;
        self.i += 1;
        let mut v = Vec::new();
        self.ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            self.depth -= 1;
            return Ok(P::List(v));
        }
        loop {
            self.ws();
            v.push(self.value()?);
            self.ws();
            match self.peek() {
                Some(b',') => self.i += 1,
                Some(b']') => {
                    self.i += 1;
                    break;
                }
                _ => return Err(Fail::Decode("Expecting ',' delimiter")),
            }
        }
        self.depth -= 1;
        Ok(P::List(v))
    }

    fn object(&mut self) -> PR<P<'a>> {
        self.enter()?;
        self.i += 1;
        let mut v: Vec<(Cow<'a, [u8]>, P<'a>)> = Vec::new();
        self.ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            self.depth -= 1;
            return Ok(P::Dict(v));
        }
        loop {
            self.ws();
            if self.peek() != Some(b'"') {
                return Err(Fail::Decode("Expecting property name enclosed in double quotes"));
            }
            let k = self.string()?;
            self.ws();
            if self.peek() != Some(b':') {
                return Err(Fail::Decode("Expecting ':' delimiter"));
            }
            self.i += 1;
            self.ws();
            let val = self.value()?;
            P::dict_set(&mut v, k, val);
            self.ws();
            match self.peek() {
                Some(b',') => self.i += 1,
                Some(b'}') => {
                    self.i += 1;
                    break;
                }
                _ => return Err(Fail::Decode("Expecting ',' delimiter")),
            }
        }
        self.depth -= 1;
        Ok(P::Dict(v))
    }

    fn hex4(&self, at: usize) -> PR<u32> {
        let h = self.s.get(at..at + 4).ok_or(Fail::Decode("Invalid \\uXXXX escape"))?;
        let mut v = 0u32;
        for &c in h {
            v = v * 16
                + match c {
                    b'0'..=b'9' => c - b'0',
                    b'a'..=b'f' => c - b'a' + 10,
                    b'A'..=b'F' => c - b'A' + 10,
                    _ => return Err(Fail::Decode("Invalid \\uXXXX escape")),
                } as u32;
        }
        Ok(v)
    }

    /// A string token as WTF-8, borrowed from the text when it has no escape.
    fn string(&mut self) -> PR<Cow<'a, [u8]>> {
        self.i += 1;
        let start = self.i;
        let mut j = start;
        while j < self.s.len() && self.s[j] != b'"' && self.s[j] != b'\\' && self.s[j] >= 0x20 {
            j += 1;
        }
        if self.s.get(j) == Some(&b'"') {
            self.i = j + 1;
            return Ok(Cow::Borrowed(&self.s[start..j]));
        }
        let mut out = self.s[start..j].to_vec();
        self.i = j;
        loop {
            let c = *self.s.get(self.i).ok_or(Fail::Decode("Unterminated string starting at"))?;
            match c {
                b'"' => {
                    self.i += 1;
                    return Ok(Cow::Owned(out));
                }
                0x00..=0x1f => return Err(Fail::Decode("Invalid control character at")),
                b'\\' => {
                    let e = *self.s.get(self.i + 1).ok_or(Fail::Decode("Unterminated string starting at"))?;
                    self.i += 2;
                    match e {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let mut cp = self.hex4(self.i)?;
                            self.i += 4;
                            if (0xD800..0xDC00).contains(&cp) && self.s.get(self.i) == Some(&b'\\') && self.s.get(self.i + 1) == Some(&b'u') {
                                if let Ok(lo) = self.hex4(self.i + 2) {
                                    if (0xDC00..0xE000).contains(&lo) {
                                        cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                        self.i += 6;
                                    }
                                }
                            }
                            if (0xD800..0xE000).contains(&cp) {
                                out.push(0xED);
                                out.push(0x80 | ((cp >> 6) & 0x3F) as u8);
                                out.push(0x80 | (cp & 0x3F) as u8);
                            } else {
                                let ch = char::from_u32(cp).unwrap();
                                let mut b = [0u8; 4];
                                out.extend_from_slice(ch.encode_utf8(&mut b).as_bytes());
                            }
                        }
                        _ => return Err(Fail::Decode("Invalid \\escape")),
                    }
                }
                _ => {
                    out.push(c);
                    self.i += 1;
                }
            }
        }
    }
}
