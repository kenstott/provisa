// The message descriptors of a serialized FileDescriptorSet (what protoc writes and what the
// product compiles each organisation's proto into), read with a hand-written wire decoder: the
// service is generated at run time, so there is nothing to generate code from at build time.

use std::collections::HashMap;

use crate::{EncErr, R};

pub const T_DOUBLE: i32 = 1;
pub const T_FLOAT: i32 = 2;
pub const T_INT64: i32 = 3;
pub const T_UINT64: i32 = 4;
pub const T_INT32: i32 = 5;
pub const T_BOOL: i32 = 8;
pub const T_STRING: i32 = 9;
pub const T_MESSAGE: i32 = 11;
pub const T_BYTES: i32 = 12;
pub const T_UINT32: i32 = 13;

const LABEL_REPEATED: i32 = 3;

#[derive(Clone, Debug)]
pub struct FieldDesc {
    pub name: String,
    pub number: u32,
    pub ftype: i32,
    pub repeated: bool,
    /// The message type's full name, without the leading dot ("" for a scalar).
    pub type_name: String,
    /// Whether a value equal to its type's default is still written (explicit presence).
    pub presence: bool,
}

#[derive(Clone, Debug)]
pub struct MsgDesc {
    pub full_name: String,
    pub fields: Vec<FieldDesc>,
}

impl MsgDesc {
    pub fn field(&self, name: &str) -> Option<&FieldDesc> {
        self.fields.iter().find(|f| f.name == name)
    }
}

pub struct Pool {
    msgs: HashMap<String, MsgDesc>,
}

impl Pool {
    pub fn message(&self, full_name: &str) -> R<&MsgDesc> {
        self.msgs
            .get(full_name.trim_start_matches('.'))
            .ok_or_else(|| EncErr::Value(format!("the descriptor set has no message {full_name:?}")))
    }

    pub fn parse(set: &[u8]) -> R<Pool> {
        let mut pool = Pool { msgs: HashMap::new() };
        let mut r = Reader::new(set);
        while let Some((num, wt)) = r.tag()? {
            match (num, wt) {
                (1, 2) => pool.file(r.bytes()?)?,
                _ => r.skip(wt)?,
            }
        }
        Ok(pool)
    }

    fn file(&mut self, buf: &[u8]) -> R<()> {
        let mut package = String::new();
        let mut syntax = String::new();
        let mut messages: Vec<&[u8]> = Vec::new();
        let mut r = Reader::new(buf);
        while let Some((num, wt)) = r.tag()? {
            match (num, wt) {
                (2, 2) => package = r.string()?,
                (4, 2) => messages.push(r.bytes()?),
                (12, 2) => syntax = r.string()?,
                _ => r.skip(wt)?,
            }
        }
        let proto3 = match syntax.as_str() {
            "proto3" => true,
            "" | "proto2" => false,
            other => return Err(EncErr::Unsupported(format!("descriptor syntax {other:?}"))),
        };
        for m in messages {
            self.message_proto(&package, m, proto3)?;
        }
        Ok(())
    }

    fn message_proto(&mut self, scope: &str, buf: &[u8], proto3: bool) -> R<()> {
        let mut name = String::new();
        let mut fields = Vec::new();
        let mut nested: Vec<&[u8]> = Vec::new();
        let mut r = Reader::new(buf);
        while let Some((num, wt)) = r.tag()? {
            match (num, wt) {
                (1, 2) => name = r.string()?,
                (2, 2) => fields.push(field_proto(r.bytes()?, proto3)?),
                (3, 2) => nested.push(r.bytes()?),
                _ => r.skip(wt)?,
            }
        }
        let full_name = if scope.is_empty() { name } else { format!("{scope}.{name}") };
        for n in nested {
            self.message_proto(&full_name, n, proto3)?;
        }
        fields.sort_by_key(|f: &FieldDesc| f.number);
        self.msgs.insert(full_name.clone(), MsgDesc { full_name, fields });
        Ok(())
    }
}

fn field_proto(buf: &[u8], proto3: bool) -> R<FieldDesc> {
    let mut f = FieldDesc {
        name: String::new(),
        number: 0,
        ftype: 0,
        repeated: false,
        type_name: String::new(),
        presence: false,
    };
    let mut proto3_optional = false;
    let mut r = Reader::new(buf);
    while let Some((num, wt)) = r.tag()? {
        match (num, wt) {
            (1, 2) => f.name = r.string()?,
            (3, 0) => f.number = r.varint()? as u32,
            (4, 0) => f.repeated = r.varint()? as i32 == LABEL_REPEATED,
            (5, 0) => f.ftype = r.varint()? as i32,
            (6, 2) => f.type_name = r.string()?.trim_start_matches('.').to_string(),
            (17, 0) => proto3_optional = r.varint()? != 0,
            _ => r.skip(wt)?,
        }
    }
    f.presence = !f.repeated && (f.ftype == T_MESSAGE || proto3_optional || !proto3);
    Ok(f)
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn malformed() -> EncErr {
        EncErr::Value("malformed FileDescriptorSet".into())
    }

    fn varint(&mut self) -> R<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = *self.buf.get(self.pos).ok_or_else(Self::malformed)?;
            self.pos += 1;
            v |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(Self::malformed())
    }

    fn tag(&mut self) -> R<Option<(u32, u8)>> {
        if self.pos >= self.buf.len() {
            return Ok(None);
        }
        let t = self.varint()?;
        Ok(Some(((t >> 3) as u32, (t & 7) as u8)))
    }

    fn bytes(&mut self) -> R<&'a [u8]> {
        let n = self.varint()? as usize;
        let end = self.pos.checked_add(n).filter(|&e| e <= self.buf.len()).ok_or_else(Self::malformed)?;
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn string(&mut self) -> R<String> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| Self::malformed())
    }

    fn skip(&mut self, wt: u8) -> R<()> {
        match wt {
            0 => {
                self.varint()?;
            }
            1 => self.pos += 8,
            2 => {
                self.bytes()?;
            }
            5 => self.pos += 4,
            _ => return Err(Self::malformed()),
        }
        if self.pos > self.buf.len() {
            return Err(Self::malformed());
        }
        Ok(())
    }
}
