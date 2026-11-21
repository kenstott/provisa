// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

use std::collections::HashMap;

use provisa_sdk::{health, on_start, on_stop, table};

pub struct Row {
    pub id: i64,
    pub name: String,
}

/// Open the client.
pub fn start(values: HashMap<String, String>) -> Result<(), String> {
    let _ = values;
    Ok(())
}

/// Close the client.
pub fn stop() {}

pub fn status() -> String {
    "ok".to_string()
}

pub fn rows() -> Vec<Row> {
    Vec::new()
}

on_start!(start);
on_stop!(stop);
health!(status);
table!(rows);
