// Copyright (c) 2026 Kenneth Stott
// Canary: cc609495-d219-4089-9c0b-4edc981552b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

//! What the generated exports.rs (provisa/rustlang/build.py) calls the code's functions with. A
//! Rust program has no reflection, so each function is called by generated code, its arguments
//! decoded with serde into the types its parameters name, and what it returns made an `Answer`.

// A build calls the helpers its functions need, and leaves the others unused.
#![allow(dead_code)]

use std::fmt::Display;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::wire::{absent, from_wire};

/// One set of a call's arguments: their values as the wire gives them, and their parameters'
/// names.
pub struct Args<'a> {
    pub export: &'a str,
    pub args: &'a [Value],
    pub names: &'a [String],
}

/// Rows, one at a time: each a row's serde form, or the code's failure.
pub type Rows = Box<dyn Iterator<Item = Result<Value, String>>>;

/// What a function gave.
pub enum Answer {
    Nothing,
    Value(Value),
    Rows(Rows),
}

/// A code function, as exports.rs registers it.
pub type Export = fn(&Args) -> Result<Answer, String>;

/// A command's arguments are its parameters, in order, as many as it has.
pub fn expect(a: &Args, count: usize) -> Result<(), String> {
    if a.args.len() != count {
        return Err(format!(
            "{} takes {count} arguments, and the call gives {}",
            a.export,
            a.args.len()
        ));
    }
    Ok(())
}

/// Argument `index`, decoded into its parameter's type; one the read does not give is null, an
/// Option's None.
pub fn arg<T: DeserializeOwned>(a: &Args, index: usize) -> Result<T, String> {
    let given = a.args.get(index).filter(|v| !absent(v)).map(from_wire).unwrap_or(Value::Null);
    serde_json::from_value(given).map_err(|e| format!("{}: argument {}: {e}", a.export, index + 1))
}

/// A table's or stream's inputs: a struct made of the arguments by name, an argument left out
/// leaving its field to serde (None for an Option, its default where it has one).
pub fn inputs<T: DeserializeOwned>(a: &Args) -> Result<T, String> {
    let mut given = Map::new();
    for (name, value) in a.names.iter().zip(a.args) {
        if !absent(value) {
            given.insert(name.clone(), from_wire(value));
        }
    }
    serde_json::from_value(Value::Object(given)).map_err(|e| format!("{}: its inputs: {e}", a.export))
}

/// The code's own failure, as its error says it.
pub fn failed<E: Display>(error: E) -> String {
    error.to_string()
}

fn serialized<T: Serialize>(value: T) -> Result<Value, String> {
    serde_json::to_value(value).map_err(|e| format!("a value has no serde form: {e}"))
}

/// A single value.
pub fn value<T: Serialize>(value: T) -> Result<Answer, String> {
    Ok(Answer::Value(serialized(value)?))
}

/// Rows from a collection or an iterator.
pub fn rows<I>(collection: I) -> Result<Answer, String>
where
    I: IntoIterator + 'static,
    I::Item: Serialize,
{
    Ok(Answer::Rows(Box::new(collection.into_iter().map(serialized))))
}

/// Rows from an iterator whose items are each a Result: an error ends the call with it.
pub fn fallible_rows<I, T, E>(collection: I) -> Result<Answer, String>
where
    I: IntoIterator<Item = Result<T, E>> + 'static,
    T: Serialize,
    E: Display,
{
    Ok(Answer::Rows(Box::new(
        collection.into_iter().map(|item| item.map_err(failed).and_then(serialized)),
    )))
}
