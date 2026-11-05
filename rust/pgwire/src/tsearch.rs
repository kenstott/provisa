// tsvector and tsquery binary forms, from their text.

use crate::{int_ascii, EncErr, R};

fn bad(what: &str, t: &str) -> EncErr {
    EncErr::Value(format!("not a {what}: {t:?}"))
}

/// buenavista _tsvector_binary: tsvectorsend of a tsvector's text. Each lexeme NUL-terminated,
/// its position count, then each position with its weight in the top two bits.
pub fn tsvector_send(t: &str, out: &mut Vec<u8>) -> R<()> {
    let text: Vec<char> = t.chars().collect();
    let at = |k: usize| -> R<char> {
        text.get(k)
            .copied()
            .ok_or_else(|| EncErr::Value("IndexError: string index out of range".into()))
    };
    let mut lexemes: Vec<u8> = Vec::new();
    let mut count: i64 = 0;
    let mut i = 0;
    while i < text.len() {
        if text[i] == ' ' {
            i += 1;
            continue;
        }
        if text[i] != '\'' {
            return Err(bad("tsvector", t));
        }
        i += 1;
        let mut lexeme = String::new();
        loop {
            let c = at(i)?;
            if c == '\\' {
                lexeme.push(at(i + 1)?);
                i += 2;
            } else if c == '\'' && text.get(i + 1) == Some(&'\'') {
                lexeme.push('\'');
                i += 2;
            } else if c == '\'' {
                i += 1;
                break;
            } else {
                lexeme.push(c);
                i += 1;
            }
        }
        let mut positions: Vec<i64> = Vec::new();
        if text.get(i) == Some(&':') {
            // re.match(r":((?:\d+[A-D]?,?)+)")
            let is_digit = |c: char| int_ascii(c).is_ascii_digit() && (c.is_ascii_digit() || !c.is_ascii());
            let mut k = i + 1;
            let mut items: Vec<String> = Vec::new();
            let mut cur = String::new();
            loop {
                let start = k;
                while k < text.len() && is_digit(text[k]) {
                    k += 1;
                }
                if k == start {
                    break;
                }
                cur.extend(text[start..k].iter().map(|&c| int_ascii(c) as char));
                if k < text.len() && matches!(text[k], 'A'..='D') {
                    cur.push(text[k]);
                    k += 1;
                }
                items.push(std::mem::take(&mut cur));
                if k < text.len() && text[k] == ',' {
                    k += 1;
                }
            }
            if items.is_empty() {
                return Err(bad("tsvector", t));
            }
            for item in items {
                let weight = match item.chars().last() {
                    Some('A') => 3,
                    Some('B') => 2,
                    Some('C') => 1,
                    _ => 0,
                };
                let pos: i64 = item
                    .trim_end_matches(['A', 'B', 'C', 'D'])
                    .parse()
                    .map_err(|_| bad("tsvector", t))?;
                positions.push((weight << 14) | pos);
            }
            i = k;
        }
        lexemes.extend_from_slice(lexeme.as_bytes());
        lexemes.push(0);
        let n = positions.len() as i64;
        for v in std::iter::once(n).chain(positions) {
            let v = u16::try_from(v)
                .map_err(|_| EncErr::Value("struct.error: ushort format requires 0 <= number <= 65535".into()))?;
            lexemes.extend_from_slice(&v.to_be_bytes());
        }
        count += 1;
    }
    out.extend_from_slice(&(count as i32).to_be_bytes());
    out.extend_from_slice(&lexemes);
    Ok(())
}

#[derive(Debug)]
enum Node {
    Val {
        lexeme: Vec<u8>,
        weight: u8,
        prefix: bool,
    },
    Not(Box<Node>),
    Bin {
        op: u8,
        distance: i16,
        left: Box<Node>,
        right: Box<Node>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Tok {
    Not,
    /// OP_AND 2, OP_OR 3, OP_PHRASE 4 with its distance
    Op(u8, i16),
    Open,
    Close,
    Val(usize),
}

const OP_NOT: u8 = 1;
const OP_AND: u8 = 2;
const OP_OR: u8 = 3;
const OP_PHRASE: u8 = 4;

fn priority(op: u8) -> u8 {
    // tsquery's operator priorities: | lowest, then &, then <->, ! highest
    match op {
        OP_OR => 1,
        OP_AND => 2,
        OP_PHRASE => 3,
        _ => 4,
    }
}

/// An operand: its lexeme, weight bits and prefix flag.
type Operand = (Vec<u8>, u8, bool);

fn tokens(t: &str) -> R<(Vec<Tok>, Vec<Operand>)> {
    let c: Vec<char> = t.chars().collect();
    let mut toks = Vec::new();
    let mut vals = Vec::new();
    let mut i = 0;
    let err = || bad("tsquery", t);
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }
        match ch {
            '!' => {
                toks.push(Tok::Not);
                i += 1;
            }
            '&' => {
                toks.push(Tok::Op(OP_AND, 0));
                i += 1;
            }
            '|' => {
                toks.push(Tok::Op(OP_OR, 0));
                i += 1;
            }
            '(' => {
                toks.push(Tok::Open);
                i += 1;
            }
            ')' => {
                toks.push(Tok::Close);
                i += 1;
            }
            '<' => {
                let end = c[i..].iter().position(|&x| x == '>').ok_or_else(err)? + i;
                let inner: String = c[i + 1..end].iter().collect();
                let distance: i16 = if inner == "-" {
                    1
                } else {
                    let d: i32 = inner.trim().parse().map_err(|_| err())?;
                    i16::try_from(d)
                        .ok()
                        .filter(|d| (0..=16384).contains(d))
                        .ok_or_else(err)?
                };
                toks.push(Tok::Op(OP_PHRASE, distance));
                i = end + 1;
            }
            _ => {
                let mut lexeme = String::new();
                if ch == '\'' {
                    i += 1;
                    loop {
                        let x = *c.get(i).ok_or_else(err)?;
                        if x == '\\' {
                            lexeme.push(*c.get(i + 1).ok_or_else(err)?);
                            i += 2;
                        } else if x == '\'' && c.get(i + 1) == Some(&'\'') {
                            lexeme.push('\'');
                            i += 2;
                        } else if x == '\'' {
                            i += 1;
                            break;
                        } else {
                            lexeme.push(x);
                            i += 1;
                        }
                    }
                } else {
                    while i < c.len() && !c[i].is_whitespace() && !"!&|()<:'".contains(c[i]) {
                        if c[i] == '\\' && i + 1 < c.len() {
                            i += 1;
                        }
                        lexeme.push(c[i]);
                        i += 1;
                    }
                }
                if lexeme.is_empty() {
                    return Err(err());
                }
                let (mut weight, mut prefix) = (0u8, false);
                if c.get(i) == Some(&':') {
                    i += 1;
                    while i < c.len() {
                        match c[i] {
                            'a' | 'A' => weight |= 1 << 3,
                            'b' | 'B' => weight |= 1 << 2,
                            'c' | 'C' => weight |= 1 << 1,
                            'd' | 'D' => weight |= 1,
                            '*' => prefix = true,
                            _ => break,
                        }
                        i += 1;
                    }
                }
                vals.push((lexeme.into_bytes(), weight, prefix));
                toks.push(Tok::Val(vals.len() - 1));
            }
        }
    }
    Ok((toks, vals))
}

/// The tree tsquery_in builds of a tsquery's text: binary operators left-associative, | below &
/// below <-> below !.
fn parse(t: &str) -> R<Option<Node>> {
    let (toks, vals) = tokens(t)?;
    if toks.is_empty() {
        return Ok(None);
    }
    let mut pos = 0;
    let node = parse_expr(&toks, &vals, &mut pos, 0, t)?;
    if pos != toks.len() {
        return Err(bad("tsquery", t));
    }
    Ok(Some(node))
}

fn parse_expr(toks: &[Tok], vals: &[Operand], pos: &mut usize, min: u8, t: &str) -> R<Node> {
    let mut left = parse_unary(toks, vals, pos, t)?;
    while let Some(Tok::Op(op, distance)) = toks.get(*pos).copied() {
        let p = priority(op);
        if p <= min {
            break;
        }
        *pos += 1;
        let right = parse_expr(toks, vals, pos, p, t)?;
        left = Node::Bin {
            op,
            distance,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_unary(toks: &[Tok], vals: &[Operand], pos: &mut usize, t: &str) -> R<Node> {
    match toks.get(*pos).copied() {
        Some(Tok::Not) => {
            *pos += 1;
            Ok(Node::Not(Box::new(parse_unary(toks, vals, pos, t)?)))
        }
        Some(Tok::Open) => {
            *pos += 1;
            let inner = parse_expr(toks, vals, pos, 0, t)?;
            if toks.get(*pos) != Some(&Tok::Close) {
                return Err(bad("tsquery", t));
            }
            *pos += 1;
            Ok(inner)
        }
        Some(Tok::Val(k)) => {
            *pos += 1;
            let (lexeme, weight, prefix) = vals[k].clone();
            Ok(Node::Val { lexeme, weight, prefix })
        }
        _ => Err(bad("tsquery", t)),
    }
}

fn count(n: &Node) -> i32 {
    match n {
        Node::Val { .. } => 1,
        Node::Not(x) => 1 + count(x),
        Node::Bin { left, right, .. } => 1 + count(left) + count(right),
    }
}

/// An item and its operands in tsquery's storage order: an operator, then its right operand,
/// then its left.
fn send_node(n: &Node, out: &mut Vec<u8>) {
    match n {
        Node::Val { lexeme, weight, prefix } => {
            out.extend_from_slice(&[1, *weight, *prefix as u8]);
            out.extend_from_slice(lexeme);
            out.push(0);
        }
        Node::Not(x) => {
            out.extend_from_slice(&[2, OP_NOT]);
            send_node(x, out);
        }
        Node::Bin {
            op,
            distance,
            left,
            right,
        } => {
            out.extend_from_slice(&[2, *op]);
            if *op == OP_PHRASE {
                out.extend_from_slice(&distance.to_be_bytes());
            }
            send_node(right, out);
            send_node(left, out);
        }
    }
}

/// tsquerysend of tsquery_in(text): the item count, then each item (1 an operand: its weight
/// bits, prefix flag and NUL-terminated lexeme; 2 an operator: its code, and a phrase's
/// distance). The product sends a tsquery in text only.
pub fn tsquery_send(t: &str, out: &mut Vec<u8>) -> R<()> {
    match parse(t)? {
        None => out.extend_from_slice(&0i32.to_be_bytes()),
        Some(node) => {
            out.extend_from_slice(&count(&node).to_be_bytes());
            send_node(&node, out);
        }
    }
    Ok(())
}
