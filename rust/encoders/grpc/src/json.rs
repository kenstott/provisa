// JSON as Python's json module reads and writes it: json.loads into a value tree (an object keeps
// its keys' first places and their last values, as a dict does; a number with a fraction or an
// exponent is a float, any other an int of any size) and json.dumps out of one, either as it
// writes by default (ensure_ascii, ", " and ": ") or compact with ensure_ascii off, as
// grpc/server.py's _text wrote an array, struct or map (REQ-1965).

use crate::pyfmt;
use crate::{EncErr, R};

pub enum J {
    Null,
    Bool(bool),
    /// An int's digits as Python writes it back: no leading zeros, no "-0".
    Int(String),
    Float(f64),
    Str(String),
    Arr(Vec<J>),
    Obj(Vec<(String, J)>),
}

fn bad(what: &str, at: usize) -> EncErr {
    EncErr::Value(format!("JSON: {what} at char {at}"))
}

pub fn loads(text: &str) -> R<J> {
    let mut p = Parser { s: text.as_bytes(), i: 0 };
    p.ws();
    let v = p.value()?;
    p.ws();
    if p.i != p.s.len() {
        return Err(bad("Extra data", p.i));
    }
    Ok(v)
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn lit(&mut self, word: &[u8]) -> bool {
        if self.s[self.i..].starts_with(word) {
            self.i += word.len();
            true
        } else {
            false
        }
    }

    fn value(&mut self) -> R<J> {
        let Some(&c) = self.s.get(self.i) else {
            return Err(bad("Expecting value", self.i));
        };
        match c {
            b'{' => self.object(),
            b'[' => self.array(),
            b'"' => Ok(J::Str(self.string()?)),
            _ if self.lit(b"null") => Ok(J::Null),
            _ if self.lit(b"true") => Ok(J::Bool(true)),
            _ if self.lit(b"false") => Ok(J::Bool(false)),
            _ if self.lit(b"NaN") => Ok(J::Float(f64::NAN)),
            _ if self.lit(b"Infinity") => Ok(J::Float(f64::INFINITY)),
            _ if self.lit(b"-Infinity") => Ok(J::Float(f64::NEG_INFINITY)),
            b'-' | b'0'..=b'9' => self.number(),
            _ => Err(bad("Expecting value", self.i)),
        }
    }

    /// -?(0|[1-9]\d*)(\.\d+)?([eE][-+]?\d+)? (json.scanner.NUMBER_RE)
    fn number(&mut self) -> R<J> {
        let start = self.i;
        let digits = |p: &mut Self| {
            let from = p.i;
            while p.i < p.s.len() && p.s[p.i].is_ascii_digit() {
                p.i += 1;
            }
            p.i - from
        };
        if self.s[self.i] == b'-' {
            self.i += 1;
        }
        let int_start = self.i;
        if self.s.get(self.i) == Some(&b'0') {
            self.i += 1;
        } else if digits(self) == 0 {
            return Err(bad("Expecting value", start));
        }
        let int_end = self.i;
        let mut float = false;
        if self.s.get(self.i) == Some(&b'.') {
            let back = self.i;
            self.i += 1;
            if digits(self) == 0 {
                self.i = back;
            } else {
                float = true;
            }
        }
        if matches!(self.s.get(self.i), Some(b'e' | b'E')) {
            let back = self.i;
            self.i += 1;
            if matches!(self.s.get(self.i), Some(b'+' | b'-')) {
                self.i += 1;
            }
            if digits(self) == 0 {
                self.i = back;
            } else {
                float = true;
            }
        }
        let text = std::str::from_utf8(&self.s[start..self.i]).unwrap();
        if float {
            return Ok(J::Float(text.parse::<f64>().unwrap()));
        }
        let magnitude = std::str::from_utf8(&self.s[int_start..int_end]).unwrap();
        Ok(J::Int(if magnitude == "0" { "0".into() } else { text.into() }))
    }

    fn hex4(&mut self) -> R<u32> {
        let h = self.s.get(self.i..self.i + 4).ok_or_else(|| bad("Invalid \\uXXXX escape", self.i))?;
        let v = std::str::from_utf8(h)
            .ok()
            .and_then(|h| u32::from_str_radix(h, 16).ok())
            .ok_or_else(|| bad("Invalid \\uXXXX escape", self.i))?;
        self.i += 4;
        Ok(v)
    }

    fn string(&mut self) -> R<String> {
        self.i += 1;
        let mut out = String::new();
        loop {
            let start = self.i;
            while self.i < self.s.len() && !matches!(self.s[self.i], b'"' | b'\\') && self.s[self.i] >= 0x20 {
                self.i += 1;
            }
            out.push_str(std::str::from_utf8(&self.s[start..self.i]).unwrap());
            match self.s.get(self.i) {
                None => return Err(bad("Unterminated string starting", start)),
                Some(b'"') => {
                    self.i += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.i += 1;
                    let e = *self.s.get(self.i).ok_or_else(|| bad("Unterminated string starting", start))?;
                    self.i += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let mut c = self.hex4()?;
                            if (0xd800..0xdc00).contains(&c) && self.s[self.i..].starts_with(b"\\u") {
                                let back = self.i;
                                self.i += 2;
                                let low = self.hex4()?;
                                if (0xdc00..0xe000).contains(&low) {
                                    c = 0x10000 + ((c - 0xd800) << 10) + (low - 0xdc00);
                                } else {
                                    self.i = back;
                                }
                            }
                            // A lone surrogate is text Python holds and Rust cannot.
                            out.push(char::from_u32(c).ok_or_else(|| {
                                EncErr::Value(format!("JSON: a lone surrogate \\u{c:04x} is not text"))
                            })?);
                        }
                        _ => return Err(bad("Invalid \\escape", self.i - 2)),
                    }
                }
                Some(_) => return Err(bad("Invalid control character", self.i)),
            }
        }
    }

    fn array(&mut self) -> R<J> {
        self.i += 1;
        let mut items = Vec::new();
        self.ws();
        if self.s.get(self.i) == Some(&b']') {
            self.i += 1;
            return Ok(J::Arr(items));
        }
        loop {
            self.ws();
            items.push(self.value()?);
            self.ws();
            match self.s.get(self.i) {
                Some(b',') => self.i += 1,
                Some(b']') => {
                    self.i += 1;
                    return Ok(J::Arr(items));
                }
                _ => return Err(bad("Expecting ',' delimiter", self.i)),
            }
        }
    }

    fn object(&mut self) -> R<J> {
        self.i += 1;
        let mut entries: Vec<(String, J)> = Vec::new();
        self.ws();
        if self.s.get(self.i) == Some(&b'}') {
            self.i += 1;
            return Ok(J::Obj(entries));
        }
        loop {
            self.ws();
            if self.s.get(self.i) != Some(&b'"') {
                return Err(bad("Expecting property name enclosed in double quotes", self.i));
            }
            let key = self.string()?;
            self.ws();
            if self.s.get(self.i) != Some(&b':') {
                return Err(bad("Expecting ':' delimiter", self.i));
            }
            self.i += 1;
            self.ws();
            let value = self.value()?;
            put(&mut entries, key, value);
            self.ws();
            match self.s.get(self.i) {
                Some(b',') => self.i += 1,
                Some(b'}') => {
                    self.i += 1;
                    return Ok(J::Obj(entries));
                }
                _ => return Err(bad("Expecting ',' delimiter", self.i)),
            }
        }
    }
}

/// dict[key] = value: a key already present keeps its place and takes the new value.
pub fn put<T>(entries: &mut Vec<(String, T)>, key: String, value: T) {
    match entries.iter_mut().find(|(k, _)| *k == key) {
        Some(slot) => slot.1 = value,
        None => entries.push((key, value)),
    }
}

/// json.dumps' string escapes: `ascii` (ensure_ascii, the default) writes everything outside
/// ' '..='~' as \uXXXX (a surrogate pair past the BMP); off, only '"', '\\' and the controls.
pub fn write_str(s: &str, ascii: bool, out: &mut Vec<u8>) {
    out.push(b'"');
    for c in s.chars() {
        match c {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\t' => out.extend_from_slice(b"\\t"),
            '\u{8}' => out.extend_from_slice(b"\\b"),
            '\u{c}' => out.extend_from_slice(b"\\f"),
            c if (c as u32) < 0x20 || (ascii && !(' '..='~').contains(&c)) => {
                let mut units = [0u16; 2];
                for u in c.encode_utf16(&mut units) {
                    out.extend_from_slice(format!("\\u{:04x}", u).as_bytes());
                }
            }
            c => {
                let mut b = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut b).as_bytes());
            }
        }
    }
    out.push(b'"');
}

/// json.dumps(value) with its defaults: ensure_ascii, ", " and ": ", NaN and the infinities as
/// NaN / Infinity / -Infinity.
pub fn dumps(v: &J, out: &mut Vec<u8>) {
    match v {
        J::Null => out.extend_from_slice(b"null"),
        J::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        J::Int(digits) => out.extend_from_slice(digits.as_bytes()),
        J::Float(x) => pyfmt::float_repr(*x, out),
        J::Str(s) => write_str(s, true, out),
        J::Arr(items) => {
            out.push(b'[');
            for (n, item) in items.iter().enumerate() {
                if n > 0 {
                    out.extend_from_slice(b", ");
                }
                dumps(item, out);
            }
            out.push(b']');
        }
        J::Obj(entries) => {
            out.push(b'{');
            for (n, (k, item)) in entries.iter().enumerate() {
                if n > 0 {
                    out.extend_from_slice(b", ");
                }
                write_str(k, true, out);
                out.extend_from_slice(b": ");
                dumps(item, out);
            }
            out.push(b'}');
        }
    }
}

/// A read mask's selection inside a JSON value (grpc/query_ir.MaskTree): each selected key, and
/// below it either the whole value (None) or a further selection.
pub struct Mask(pub Vec<(String, Option<Mask>)>);

impl Mask {
    pub fn get(&self, key: &str) -> Option<&Option<Mask>> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, m)| m)
    }
}

/// query_ir._restrict_decoded: an object keeps only the selected keys, a list is restricted item
/// by item, anything else is as it is.
pub fn restrict(v: J, mask: &Mask) -> J {
    match v {
        J::Obj(entries) => J::Obj(
            entries
                .into_iter()
                .filter_map(|(k, item)| match mask.get(&k) {
                    None => None,
                    Some(None) => Some((k, item)),
                    Some(Some(below)) => Some((k, restrict(item, below))),
                })
                .collect(),
        ),
        J::Arr(items) => J::Arr(items.into_iter().map(|item| restrict(item, mask)).collect()),
        other => other,
    }
}
