//! `serialize.column_value`: a raw value as JSON writes it. A JSON-typed column's text parsed
//! (`json_value`, the strings at its `json_text` key paths parsed again), an integer-typed
//! column's Decimal as its int (`integer_value`), any other value through `_convert_value`.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use crate::json;
use crate::pyval::{err, Res, P};
use crate::pywrite;
use crate::text;

#[derive(Clone)]
pub enum Mode {
    Convert,
    Integer,
    /// `col.json`, with `col.json_text`.
    Json(Arc<HashSet<Vec<u8>>>),
}

/// `_convert_value(p)`.
pub fn convert<'a>(p: P<'a>) -> P<'a> {
    match p {
        P::Dec(ref d) => {
            let mut t = Vec::new();
            text::decimal_f(&mut t, d);
            P::Str(Cow::Owned(t))
        }
        P::DecSpecial(s) => P::Str(Cow::Owned(s.into_owned().into_bytes())),
        P::Td(..) | P::Iv(..) => {
            let mut t = Vec::new();
            pywrite::write(&mut t, &p, 0).unwrap();
            P::Str(Cow::Owned(t[1..t.len() - 1].to_vec()))
        }
        P::Bytes(ref b) => {
            let mut t = Vec::new();
            text::hex_text(&mut t, b);
            P::Str(Cow::Owned(t))
        }
        P::Date(_) | P::Dt(_) | P::Time(_) | P::InfDate(_) | P::InfDt(..) | P::NaT => {
            let mut t = Vec::new();
            pywrite::temporal_text(&mut t, &p);
            P::Str(Cow::Owned(t))
        }
        P::Dict(e) => P::Dict(e.into_iter().map(|(k, v)| (k, convert(v))).collect()),
        P::List(v) => P::List(v.into_iter().map(convert).collect()),
        other => other,
    }
}

/// `integer_value(p, field)`.
pub fn integer<'a>(p: P<'a>, field: &str) -> Res<P<'a>> {
    match p {
        P::Dec(ref d) => {
            let (neg, digits, exp) = text::decimal_canon(d);
            if exp < 0 {
                let mut t = Vec::new();
                text::decimal_str(&mut t, d);
                return err(format!("field '{}' is typed integer but holds {}", field, String::from_utf8_lossy(&t)));
            }
            let mut s = String::new();
            if neg {
                s.push('-');
            }
            s.push_str(&digits);
            if digits != "0" {
                s.extend(std::iter::repeat('0').take(exp as usize));
            }
            Ok(match s.parse::<i128>() {
                Ok(i) => P::Int(i),
                Err(_) => P::BigInt(Cow::Owned(s)),
            })
        }
        P::DecSpecial(s) => err(format!("field '{field}' is typed integer but holds {s}")),
        other => Ok(other),
    }
}

/// `json_value(p, field, json_text)`.
pub fn json_value<'a>(p: P<'a>, field: &str, json_text: &HashSet<Vec<u8>>) -> Res<P<'a>> {
    let v = match p {
        P::Str(Cow::Borrowed(s)) => json::parse_column_text(s, field)?,
        P::Str(s) => json::parse_column_text(&s, field)?.into_owned(),
        other => other,
    };
    walk(v, field, json_text, &mut Vec::new())
}

fn walk<'a>(v: P<'a>, field: &str, json_text: &HashSet<Vec<u8>>, path: &mut Vec<u8>) -> Res<P<'a>> {
    Ok(match v {
        P::Dict(e) => {
            let mut out = Vec::with_capacity(e.len());
            for (k, x) in e {
                let len = path.len();
                path.extend_from_slice(&k);
                let x = match x {
                    P::Str(Cow::Borrowed(s)) if !json_text.is_empty() && json_text.contains(path.as_slice()) => json::parse_column_text(s, field)?,
                    P::Str(s) if !json_text.is_empty() && json_text.contains(path.as_slice()) => json::parse_column_text(&s, field)?.into_owned(),
                    x => x,
                };
                path.push(b'.');
                let x = walk(x, field, json_text, path)?;
                path.truncate(len);
                out.push((k, x));
            }
            P::Dict(out)
        }
        P::List(items) => {
            let mut out = Vec::with_capacity(items.len());
            for x in items {
                out.push(walk(x, field, json_text, path)?);
            }
            P::List(out)
        }
        other => convert(other),
    })
}

/// `column_value(p, col)`.
pub fn column_value<'a>(p: P<'a>, mode: &Mode, field: &str) -> Res<P<'a>> {
    match mode {
        Mode::Convert => Ok(convert(p)),
        Mode::Integer => integer(p, field),
        Mode::Json(paths) => json_value(p, field, paths),
    }
}
