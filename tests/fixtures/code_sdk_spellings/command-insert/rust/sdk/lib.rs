// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

pub struct Row {
    pub id: i64,
    pub name: String,
}

/// The insert of rows.
pub fn insert_rows(rows: Vec<Row>) -> i64 {
    let _ = rows;
    0
}

provisa_sdk::command!(insert_rows, insert = "rows");
