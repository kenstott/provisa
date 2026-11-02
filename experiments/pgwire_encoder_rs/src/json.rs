// json.dumps (default separators, ensure_ascii, allow_nan) of an Arrow value as to_pylist hands it
// on: a struct as a dict in field order, a map as a list of (key, value) pairs, a list as a list.

use arrow::array::*;
use arrow::datatypes::DataType;

use crate::fmt_num::float_repr;
use crate::{EncErr, R};

/// Ok when every value of `dt` is one json.dumps takes (and to_pylist gives a plain value for).
pub fn check_dumpable(dt: &DataType) -> Result<(), ()> {
    match dt {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => Ok(()),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => check_dumpable(f.data_type()),
        DataType::Struct(fields) => {
            let mut names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
            names.sort_unstable();
            if names.windows(2).any(|w| w[0] == w[1]) {
                return Err(()); // to_pylist folds duplicate names into one key
            }
            fields.iter().try_for_each(|f| check_dumpable(f.data_type()))
        }
        DataType::Map(f, _) => check_dumpable(f.data_type()),
        _ => Err(()),
    }
}

/// py_encode_basestring_ascii
pub fn string(s: &str, out: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
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
            ' '..='~' => out.push(c as u8),
            _ => {
                let mut units = [0u16; 2];
                for u in c.encode_utf16(&mut units) {
                    out.extend_from_slice(b"\\u");
                    for shift in [12, 8, 4, 0] {
                        out.push(HEX[((*u >> shift) & 15) as usize]);
                    }
                }
            }
        }
    }
    out.push(b'"');
}

fn list_items(arr: &dyn Array, i: usize, out: &mut Vec<u8>) -> R<()> {
    let (values, s, e): (ArrayRef, usize, usize) = match arr.data_type() {
        DataType::List(_) => {
            let a = arr.as_any().downcast_ref::<ListArray>().unwrap();
            let o = a.value_offsets();
            (a.values().clone(), o[i] as usize, o[i + 1] as usize)
        }
        DataType::LargeList(_) => {
            let a = arr.as_any().downcast_ref::<LargeListArray>().unwrap();
            let o = a.value_offsets();
            (a.values().clone(), o[i] as usize, o[i + 1] as usize)
        }
        DataType::FixedSizeList(_, n) => {
            let a = arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let n = *n as usize;
            (a.values().clone(), i * n, i * n + n)
        }
        _ => unreachable!(),
    };
    out.push(b'[');
    for j in s..e {
        if j > s {
            out.extend_from_slice(b", ");
        }
        dumps(values.as_ref(), j, out)?;
    }
    out.push(b']');
    Ok(())
}

pub fn dumps(arr: &dyn Array, i: usize, out: &mut Vec<u8>) -> R<()> {
    if arr.is_null(i) {
        out.extend_from_slice(b"null");
        return Ok(());
    }
    macro_rules! int {
        ($t:ty) => {{
            let v = arr.as_any().downcast_ref::<$t>().unwrap().value(i);
            let mut b = itoa::Buffer::new();
            out.extend_from_slice(b.format(v).as_bytes());
        }};
    }
    match arr.data_type() {
        DataType::Boolean => {
            let v = arr.as_any().downcast_ref::<BooleanArray>().unwrap().value(i);
            out.extend_from_slice(if v { b"true" } else { b"false" });
        }
        DataType::Int8 => int!(Int8Array),
        DataType::Int16 => int!(Int16Array),
        DataType::Int32 => int!(Int32Array),
        DataType::Int64 => int!(Int64Array),
        DataType::UInt8 => int!(UInt8Array),
        DataType::UInt16 => int!(UInt16Array),
        DataType::UInt32 => int!(UInt32Array),
        DataType::UInt64 => int!(UInt64Array),
        DataType::Float32 => float_repr(arr.as_any().downcast_ref::<Float32Array>().unwrap().value(i) as f64, out),
        DataType::Float64 => float_repr(arr.as_any().downcast_ref::<Float64Array>().unwrap().value(i), out),
        DataType::Utf8 => string(arr.as_any().downcast_ref::<StringArray>().unwrap().value(i), out),
        DataType::LargeUtf8 => string(arr.as_any().downcast_ref::<LargeStringArray>().unwrap().value(i), out),
        DataType::Utf8View => string(arr.as_any().downcast_ref::<StringViewArray>().unwrap().value(i), out),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => list_items(arr, i, out)?,
        DataType::Struct(fields) => {
            let a = arr.as_any().downcast_ref::<StructArray>().unwrap();
            out.push(b'{');
            for (k, f) in fields.iter().enumerate() {
                if k > 0 {
                    out.extend_from_slice(b", ");
                }
                string(f.name(), out);
                out.extend_from_slice(b": ");
                dumps(a.column(k).as_ref(), i, out)?;
            }
            out.push(b'}');
        }
        DataType::Map(..) => {
            let a = arr.as_any().downcast_ref::<MapArray>().unwrap();
            let o = a.value_offsets();
            let (keys, values) = (a.keys(), a.values());
            out.push(b'[');
            for j in o[i] as usize..o[i + 1] as usize {
                if j > o[i] as usize {
                    out.extend_from_slice(b", ");
                }
                out.push(b'[');
                dumps(keys.as_ref(), j, out)?;
                out.extend_from_slice(b", ");
                dumps(values.as_ref(), j, out)?;
                out.push(b']');
            }
            out.push(b']');
        }
        DataType::Null => out.extend_from_slice(b"null"),
        other => return Err(EncErr::Type(format!("Object of type {other} is not JSON serializable"))),
    }
    Ok(())
}
