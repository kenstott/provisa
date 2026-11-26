// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

use std::collections::HashMap;

pub struct Row {
    pub id: i64,
    pub name: String,
}

/// The read context, as much of it as this code reads.
pub struct ReadContext {
    pub limit: Option<i64>,
}

/// Rows of the keys, 100 at a time.
pub fn rows(context: ReadContext) -> HashMap<String, Vec<Row>> {
    let _ = context;
    HashMap::new()
}

provisa_sdk::table!(rows, loader = 100);
