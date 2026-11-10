// Copyright (c) 2026 Kenneth Stott
// Canary: 6eeb2c9b-df06-407e-9b60-57a0c9014592
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

//! Rows as Arrow IPC batches against the schema a call gives (REQ-2098,
//! provisa/codesource/arrow_rows.py), built with Apache Arrow for Rust. Each row is its serde
//! form: an object whose fields are its columns. A `numeric` or `json` column is text with its IR
//! type in the field's metadata; a timestamp or a date is the text serde writes one as (RFC 3339,
//! as chrono's types and any other write it).

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, ListArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use serde_json::Value;

use crate::wire::to_wire;

const IR_KEY: &str = "provisa.ir";

/// The schema a call carries: an Arrow IPC stream of it with no batches.
pub fn read_schema(raw: Vec<u8>) -> Result<SchemaRef, String> {
    StreamReader::try_new(Cursor::new(raw), None).map(|r| r.schema()).map_err(|e| e.to_string())
}

/// `rows` as one Arrow IPC stream of one batch.
pub fn ipc(schema: &SchemaRef, rows: &[Value]) -> Result<Vec<u8>, String> {
    for row in rows {
        if !row.is_object() {
            return Err(format!("a {} is not a row", kind_of(row)));
        }
    }
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let values: Vec<Option<&Value>> = rows.iter().map(|r| r.get(field.name())).collect();
        columns.push(array(field, &values)?);
    }
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut out, schema).map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
        writer.finish().map_err(|e| e.to_string())?;
    }
    Ok(out)
}

fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "list",
        Value::Object(_) => "object",
    }
}

fn refuse(field: &Field, value: &Value) -> String {
    format!("field {}: a {} is not a {}", field.name(), kind_of(value), field.data_type())
}

fn present<'a>(value: &Option<&'a Value>) -> Option<&'a Value> {
    value.filter(|v| !v.is_null())
}

/// Each value of `values` through `each`, a null where it is absent or null.
fn each<T>(
    values: &[Option<&Value>],
    mut convert: impl FnMut(&Value) -> Result<T, String>,
) -> Result<Vec<Option<T>>, String> {
    values.iter().map(|v| present(v).map(&mut convert).transpose()).collect()
}

/// A column of `field`'s type holding `values`.
fn array(field: &Field, values: &[Option<&Value>]) -> Result<ArrayRef, String> {
    match field.metadata().get(IR_KEY).map(String::as_str) {
        Some("json") => {
            let texts = each(values, |v| Ok(to_wire(v.clone()).to_string()))?;
            return Ok(Arc::new(StringArray::from(texts)));
        }
        Some("numeric") => {
            let texts = each(values, |v| match v {
                Value::String(s) => Ok(s.clone()),
                Value::Number(n) => Ok(n.to_string()),
                other => Err(refuse(field, other)),
            })?;
            return Ok(Arc::new(StringArray::from(texts)));
        }
        _ => {}
    }
    Ok(match field.data_type() {
        DataType::Utf8 => Arc::new(StringArray::from(each(values, |v| match v {
            Value::String(s) => Ok(s.clone()),
            other => Err(refuse(field, other)),
        })?)),
        DataType::Boolean => Arc::new(BooleanArray::from(each(values, |v| {
            v.as_bool().ok_or_else(|| refuse(field, v))
        })?)),
        DataType::Int64 => Arc::new(Int64Array::from(each(values, |v| match (v.as_i64(), v.as_u64()) {
            (Some(n), _) => Ok(n),
            (None, Some(n)) => Err(format!("field {}: {n} is beyond a bigint", field.name())),
            _ => Err(refuse(field, v)),
        })?)),
        DataType::Float64 => Arc::new(Float64Array::from(each(values, |v| {
            v.as_f64().ok_or_else(|| refuse(field, v))
        })?)),
        DataType::Timestamp(unit, zone) => {
            let nanos = each(values, |v| match v {
                Value::String(s) => instant(s).ok_or_else(|| {
                    format!("field {}: {s:?} is not a timestamp (RFC 3339)", field.name())
                }),
                other => Err(refuse(field, other)),
            })?;
            let scaled = |per: i128| -> Vec<Option<i64>> {
                nanos.iter().map(|n| n.map(|n| n.div_euclid(per) as i64)).collect()
            };
            let zone = zone.clone();
            match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(scaled(1_000_000_000)).with_timezone_opt(zone)),
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(scaled(1_000_000)).with_timezone_opt(zone)),
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(scaled(1_000)).with_timezone_opt(zone)),
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(scaled(1)).with_timezone_opt(zone)),
            }
        }
        DataType::Date32 => Arc::new(Date32Array::from(each(values, |v| match v {
            Value::String(s) => day(s).ok_or_else(|| format!("field {}: {s:?} is not a date", field.name())),
            other => Err(refuse(field, other)),
        })?)),
        DataType::List(element) => {
            let mut offsets = vec![0i32];
            let mut valid = Vec::with_capacity(values.len());
            let mut items: Vec<Option<&Value>> = Vec::new();
            for value in values {
                match present(value) {
                    None => valid.push(false),
                    Some(Value::Array(list)) => {
                        valid.push(true);
                        items.extend(list.iter().map(Some));
                    }
                    Some(other) => return Err(refuse(field, other)),
                }
                offsets.push(i32::try_from(items.len()).map_err(|_| format!("field {}: too many items", field.name()))?);
            }
            let child = array(element, &items)?;
            let list = ListArray::try_new(
                element.clone(),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                child,
                Some(NullBuffer::from(valid)),
            )
            .map_err(|e| format!("field {}: {e}", field.name()))?;
            Arc::new(list)
        }
        DataType::Struct(fields) => {
            let mut valid = Vec::with_capacity(values.len());
            for value in values {
                match present(value) {
                    None => valid.push(false),
                    Some(Value::Object(_)) => valid.push(true),
                    Some(other) => return Err(refuse(field, other)),
                }
            }
            let mut children = Vec::with_capacity(fields.len());
            for inner in fields {
                let column: Vec<Option<&Value>> =
                    values.iter().map(|v| present(v).and_then(|o| o.get(inner.name()))).collect();
                children.push(array(inner, &column)?);
            }
            let made = StructArray::try_new(fields.clone(), children, Some(NullBuffer::from(valid)))
                .map_err(|e| format!("field {}: {e}", field.name()))?;
            Arc::new(made)
        }
        other => return Err(format!("field {}: the Arrow type {other} has no form here", field.name())),
    })
}

fn digits(text: &str, at: usize, count: usize) -> Option<i64> {
    let part = text.get(at..at + count)?;
    if !part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    part.parse().ok()
}

/// Days from 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's days_from_civil).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn date_part(text: &str) -> Option<i64> {
    let (year, month, day) = (digits(text, 0, 4)?, digits(text, 5, 2)?, digits(text, 8, 2)?);
    if text.as_bytes().get(4) != Some(&b'-') || text.as_bytes().get(7) != Some(&b'-') {
        return None;
    }
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    Some(days_from_civil(year, month, day))
}

/// A calendar date: `YYYY-MM-DD`, or the date of a timestamp.
pub fn day(text: &str) -> Option<i32> {
    if text.len() == 10 {
        return date_part(text).and_then(|d| i32::try_from(d).ok());
    }
    instant(text).and_then(|n| i32::try_from(n.div_euclid(86_400_000_000_000)).ok())
}

/// An instant as nanoseconds since the epoch: RFC 3339 (`T` or a space between date and time,
/// a fraction of up to nine digits, `Z` or an offset); one with no offset is UTC.
pub fn instant(text: &str) -> Option<i128> {
    let days = date_part(text)?;
    let bytes = text.as_bytes();
    if !matches!(bytes.get(10), Some(b'T' | b't' | b' ')) || bytes.get(13) != Some(&b':') || bytes.get(16) != Some(&b':') {
        return None;
    }
    let (hour, minute, second) = (digits(text, 11, 2)?, digits(text, 14, 2)?, digits(text, 17, 2)?);
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let mut at = 19;
    let mut fraction: i128 = 0;
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        let start = at;
        while bytes.get(at).is_some_and(u8::is_ascii_digit) {
            at += 1;
        }
        let written = &text[start..at];
        if written.is_empty() {
            return None;
        }
        let nine: String = written.chars().chain(std::iter::repeat('0')).take(9).collect();
        fraction = nine.parse().ok()?;
    }
    let offset_seconds: i64 = match bytes.get(at) {
        None => 0,
        Some(b'Z' | b'z') if at + 1 == bytes.len() => 0,
        Some(sign @ (b'+' | b'-')) => {
            let rest = &text[at + 1..];
            let (h, m) = match rest.len() {
                5 if rest.as_bytes()[2] == b':' => (digits(rest, 0, 2)?, digits(rest, 3, 2)?),
                4 => (digits(rest, 0, 2)?, digits(rest, 2, 2)?),
                2 => (digits(rest, 0, 2)?, 0),
                _ => return None,
            };
            let span = h * 3600 + m * 60;
            if *sign == b'+' { span } else { -span }
        }
        _ => return None,
    };
    let seconds = days * 86_400 + hour * 3600 + minute * 60 + second - offset_seconds;
    Some(i128::from(seconds) * 1_000_000_000 + fraction)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instants_and_days() {
        assert_eq!(instant("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(instant("1970-01-01T01:00:00+01:00"), Some(0));
        assert_eq!(instant("2024-02-29T12:30:15.5Z"), Some(1_709_209_815_500_000_000));
        assert_eq!(instant("1969-12-31T23:59:59.999999999"), Some(-1));
        assert_eq!(day("2024-02-29"), Some(19_782));
        assert_eq!(day("1969-12-31T23:00:00Z"), Some(-1));
        assert_eq!(instant("2024-13-01T00:00:00Z"), None);
    }
}
