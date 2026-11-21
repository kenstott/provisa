// Copyright (c) 2026 Kenneth Stott
// Canary: eb137868-39ad-4a72-9784-e7615637ee2e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

//! Values on the code-source wire (provisa/codesource/worker.py): a timestamp travels as
//! `{"$date": iso}`, a calendar date as `{"$day": iso}`, an integer outside JSON's exact range as
//! `{"$bigint": digits}`, a decimal as `{"$decimal": digits}`, and an argument the read does not
//! give as `{"$absent": true}`. A Rust value's serde form has a date as its text already, so a
//! date arrives as the text serde reads one from.

use serde_json::{Map, Number, Value};

/// The largest integer JSON carries exactly.
const EXACT: u64 = (1 << 53) - 1;

pub fn absent(value: &Value) -> bool {
    matches!(value, Value::Object(m) if m.len() == 1 && m.get("$absent") == Some(&Value::Bool(true)))
}

fn number(digits: &str) -> Value {
    if let Ok(n) = digits.parse::<i64>() {
        return Value::Number(n.into());
    }
    if let Ok(n) = digits.parse::<u64>() {
        return Value::Number(n.into());
    }
    match digits.parse::<f64>().ok().and_then(Number::from_f64) {
        Some(n) => Value::Number(n),
        None => Value::String(digits.to_string()),
    }
}

/// A wire value as serde reads a Rust value from it.
pub fn from_wire(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(from_wire).collect()),
        Value::Object(fields) => {
            if fields.len() == 1 {
                let (key, inner) = fields.iter().next().expect("one field");
                match (key.as_str(), inner) {
                    ("$date" | "$day", Value::String(_)) => return inner.clone(),
                    ("$bigint" | "$decimal", Value::String(digits)) => return number(digits),
                    _ => {}
                }
            }
            Value::Object(fields.iter().map(|(k, v)| (k.clone(), from_wire(v))).collect())
        }
        _ => value.clone(),
    }
}

/// A value as the wire carries it.
pub fn to_wire(value: Value) -> Value {
    match value {
        Value::Number(n) => {
            let beyond = match (n.as_i64(), n.as_u64()) {
                (Some(i), _) => i.unsigned_abs() > EXACT,
                (None, Some(u)) => u > EXACT,
                _ => false,
            };
            if beyond {
                let mut out = Map::new();
                out.insert("$bigint".to_string(), Value::String(n.to_string()));
                Value::Object(out)
            } else {
                Value::Number(n)
            }
        }
        Value::Array(items) => Value::Array(items.into_iter().map(to_wire).collect()),
        Value::Object(fields) => Value::Object(fields.into_iter().map(|(k, v)| (k, to_wire(v))).collect()),
        other => other,
    }
}
