//! A converted JSON value: what `_convert_value` makes of a JSON text column (`json.loads`, then
//! each nested string that looks like JSON parsed again) or of an Arrow list or struct value
//! (`to_pylist`, each element converted). It is written as orjson writes the Python value, and
//! keyed as `_to_hashable` keys it (`json.dumps(sort_keys=True)`).

use crate::fmt;

pub enum J {
    Null,
    Bool(bool),
    /// A Python int, as decimal digits (any size).
    Int(String),
    Float(f64),
    /// A Python str as WTF-8: a lone surrogate from a `\uXXXX` escape kept as its 3 bytes.
    Str(Vec<u8>, bool),
    Arr(Vec<J>),
    Obj(Vec<(Vec<u8>, J)>),
}

impl J {
    pub fn str(s: Vec<u8>) -> J {
        J::Str(s, false)
    }

    /// Insert as a Python dict does: a repeated key keeps its first position, takes the new value.
    pub fn obj_set(entries: &mut Vec<(Vec<u8>, J)>, k: Vec<u8>, v: J) {
        if let Some(e) = entries.iter_mut().find(|e| e.0 == k) {
            e.1 = v;
        } else {
            entries.push((k, v));
        }
    }

    /// The depth of nested containers (a scalar is 0).
    pub fn depth(&self) -> usize {
        match self {
            J::Arr(v) => 1 + v.iter().map(J::depth).max().unwrap_or(0),
            J::Obj(v) => 1 + v.iter().map(|e| e.1.depth()).max().unwrap_or(0),
            _ => 0,
        }
    }

    /// Why orjson refuses this value, if it does (an int outside 64 bits, a lone surrogate).
    pub fn poison(&self) -> Option<&'static str> {
        match self {
            J::Int(d) => {
                if int_fits(d) {
                    None
                } else {
                    Some(fmt::BIG_INT)
                }
            }
            J::Str(_, s) => s.then_some(fmt::SURROGATE),
            J::Arr(v) => v.iter().find_map(J::poison),
            J::Obj(v) => v.iter().find_map(|(k, e)| {
                if has_surrogate(k) {
                    Some(fmt::SURROGATE)
                } else {
                    e.poison()
                }
            }),
            _ => None,
        }
    }

    /// orjson's bytes for this value (call only when `poison` is None).
    pub fn write(&self, out: &mut Vec<u8>) {
        match self {
            J::Null => out.extend_from_slice(b"null"),
            J::Bool(true) => out.extend_from_slice(b"true"),
            J::Bool(false) => out.extend_from_slice(b"false"),
            J::Int(d) => out.extend_from_slice(d.as_bytes()),
            J::Float(f) => fmt::write_f64(out, *f),
            J::Str(s, _) => fmt::write_str(out, s),
            J::Arr(v) => {
                out.push(b'[');
                for (i, e) in v.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    e.write(out);
                }
                out.push(b']');
            }
            J::Obj(v) => {
                out.push(b'{');
                for (i, (k, e)) in v.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    fmt::write_str(out, k);
                    out.push(b':');
                    e.write(out);
                }
                out.push(b'}');
            }
        }
    }

    /// An equality key: equal exactly when `json.dumps(v, sort_keys=True)` is (float by repr, so
    /// -0.0 is not 0.0 and every NaN is NaN; 1 is not 1.0 nor true).
    pub fn canon(&self, out: &mut Vec<u8>) {
        match self {
            J::Null => out.push(b'n'),
            J::Bool(b) => out.push(if *b { b't' } else { b'f' }),
            J::Int(d) => {
                out.push(b'i');
                out.extend_from_slice(d.as_bytes());
                out.push(b';');
            }
            J::Float(f) => {
                out.push(b'd');
                let bits = if f.is_nan() { f64::NAN.to_bits() } else { f.to_bits() };
                out.extend_from_slice(&bits.to_le_bytes());
            }
            J::Str(s, _) => {
                out.push(b's');
                out.extend_from_slice(&(s.len() as u64).to_le_bytes());
                out.extend_from_slice(s);
            }
            J::Arr(v) => {
                out.push(b'a');
                out.extend_from_slice(&(v.len() as u64).to_le_bytes());
                v.iter().for_each(|e| e.canon(out));
            }
            J::Obj(v) => {
                out.push(b'o');
                out.extend_from_slice(&(v.len() as u64).to_le_bytes());
                let mut idx: Vec<usize> = (0..v.len()).collect();
                idx.sort_by(|&a, &b| v[a].0.cmp(&v[b].0));
                for i in idx {
                    out.extend_from_slice(&(v[i].0.len() as u64).to_le_bytes());
                    out.extend_from_slice(&v[i].0);
                    v[i].1.canon(out);
                }
            }
        }
    }

    /// `_recursive_json_convert`: each string below, at any depth, that is longer than one
    /// character and opens with `{` or `[` is replaced by its own parse when it parses.
    pub fn convert_nested(&mut self) {
        match self {
            J::Arr(v) => v.iter_mut().for_each(J::convert_value),
            J::Obj(v) => v.iter_mut().for_each(|e| e.1.convert_value()),
            _ => {}
        }
    }

    /// `_convert_value` of an element already in this tree.
    pub fn convert_value(&mut self) {
        if let J::Str(s, false) = self {
            if let Some(parsed) = parse_json_text(s) {
                *self = parsed;
                return;
            }
        }
        self.convert_nested();
    }
}

fn has_surrogate(s: &[u8]) -> bool {
    s.windows(2).any(|w| w[0] == 0xED && w[1] >= 0xA0)
}

pub fn int_fits(d: &str) -> bool {
    match d.parse::<i128>() {
        Ok(v) => v >= i64::MIN as i128 && v <= u64::MAX as i128,
        Err(_) => false,
    }
}

/// `_convert_value(str)` for a text value: `json.loads` and the nested conversion when it is
/// longer than one character, opens with `{` or `[`, and parses; None otherwise (the string stays).
pub fn parse_json_text(s: &[u8]) -> Option<J> {
    if s.len() < 2 || (s[0] != b'{' && s[0] != b'[') {
        return None;
    }
    let mut p = Parser { s, i: 0, depth: 0 };
    let mut v = p.value().ok()?;
    p.ws();
    if p.i != s.len() {
        return None; // "Extra data"
    }
    v.convert_nested();
    Some(v)
}

/// Python's `json` scanner (the C one, strict): the grammar json.loads accepts, NaN and the
/// infinities included.
struct Parser<'a> {
    s: &'a [u8],
    i: usize,
    depth: usize,
}

type PR<T> = Result<T, ()>;

/// Python 3.12's int-from-string limit (`sys.get_int_max_str_digits`): a longer int literal is a
/// ValueError, which `_convert_value` catches.
const INT_MAX_STR_DIGITS: usize = 4300;
/// Deeper than this, json.loads or the conversion after it recurses past Python's limit. The
/// product raises RecursionError there, which is not caught; so does this port.
pub const MAX_PARSE_DEPTH: usize = 400;

impl<'a> Parser<'a> {
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

    fn value(&mut self) -> PR<J> {
        match self.peek().ok_or(())? {
            b'{' => self.object(),
            b'[' => self.array(),
            b'"' => {
                let (s, sur) = self.string()?;
                Ok(J::Str(s, sur))
            }
            _ => self.scalar(),
        }
    }

    fn scalar(&mut self) -> PR<J> {
        if self.lit(b"null") {
            return Ok(J::Null);
        }
        if self.lit(b"true") {
            return Ok(J::Bool(true));
        }
        if self.lit(b"false") {
            return Ok(J::Bool(false));
        }
        if self.lit(b"NaN") {
            return Ok(J::Float(f64::NAN));
        }
        if self.lit(b"Infinity") {
            return Ok(J::Float(f64::INFINITY));
        }
        if self.lit(b"-Infinity") {
            return Ok(J::Float(f64::NEG_INFINITY));
        }
        self.number()
    }

    fn number(&mut self) -> PR<J> {
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
            _ => return Err(()),
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
            return Ok(J::Float(text.parse::<f64>().map_err(|_| ())?));
        }
        let digits = &s[start..int_end];
        let neg = digits[0] == b'-';
        let body = if neg { &digits[1..] } else { digits };
        if body.len() > INT_MAX_STR_DIGITS {
            return Err(());
        }
        // Python's int: no "-0".
        if body == b"0" {
            return Ok(J::Int("0".into()));
        }
        Ok(J::Int(text.to_string()))
    }

    fn enter(&mut self) -> PR<()> {
        self.depth += 1;
        if self.depth > MAX_PARSE_DEPTH {
            return Err(());
        }
        Ok(())
    }

    fn array(&mut self) -> PR<J> {
        self.enter()?;
        self.i += 1;
        let mut v = Vec::new();
        self.ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            self.depth -= 1;
            return Ok(J::Arr(v));
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
                _ => return Err(()),
            }
        }
        self.depth -= 1;
        Ok(J::Arr(v))
    }

    fn object(&mut self) -> PR<J> {
        self.enter()?;
        self.i += 1;
        let mut v: Vec<(Vec<u8>, J)> = Vec::new();
        self.ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            self.depth -= 1;
            return Ok(J::Obj(v));
        }
        loop {
            self.ws();
            if self.peek() != Some(b'"') {
                return Err(());
            }
            let (k, _) = self.string()?;
            self.ws();
            if self.peek() != Some(b':') {
                return Err(());
            }
            self.i += 1;
            self.ws();
            let val = self.value()?;
            J::obj_set(&mut v, k, val);
            self.ws();
            match self.peek() {
                Some(b',') => self.i += 1,
                Some(b'}') => {
                    self.i += 1;
                    break;
                }
                _ => return Err(()),
            }
        }
        self.depth -= 1;
        Ok(J::Obj(v))
    }

    fn hex4(&self, at: usize) -> PR<u32> {
        let h = self.s.get(at..at + 4).ok_or(())?;
        let mut v = 0u32;
        for &c in h {
            v = v * 16
                + match c {
                    b'0'..=b'9' => c - b'0',
                    b'a'..=b'f' => c - b'a' + 10,
                    b'A'..=b'F' => c - b'A' + 10,
                    _ => return Err(()),
                } as u32;
        }
        Ok(v)
    }

    /// A string token; the bool says it holds a lone surrogate.
    fn string(&mut self) -> PR<(Vec<u8>, bool)> {
        self.i += 1;
        let mut out = Vec::new();
        let mut sur = false;
        loop {
            let c = *self.s.get(self.i).ok_or(())?;
            match c {
                b'"' => {
                    self.i += 1;
                    return Ok((out, sur));
                }
                0x00..=0x1f => return Err(()), // strict: no raw control character
                b'\\' => {
                    let e = *self.s.get(self.i + 1).ok_or(())?;
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
                            if (0xD800..0xDC00).contains(&cp)
                                && self.s.get(self.i) == Some(&b'\\')
                                && self.s.get(self.i + 1) == Some(&b'u')
                            {
                                let lo = self.hex4(self.i + 2)?;
                                if (0xDC00..0xE000).contains(&lo) {
                                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                    self.i += 6;
                                }
                            }
                            if (0xD800..0xE000).contains(&cp) {
                                sur = true;
                                // WTF-8: the surrogate's own three bytes.
                                out.push(0xED);
                                out.push(0x80 | ((cp >> 6) & 0x3F) as u8);
                                out.push(0x80 | (cp & 0x3F) as u8);
                            } else {
                                let ch = char::from_u32(cp).ok_or(())?;
                                let mut b = [0u8; 4];
                                out.extend_from_slice(ch.encode_utf8(&mut b).as_bytes());
                            }
                        }
                        _ => return Err(()),
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
