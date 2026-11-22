// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

use provisa_sdk as provisa;

pub struct Row {
    pub id: i64,
    pub name: String,
}

/// The delete of rows.
pub fn delete_rows(keys: Vec<i64>) -> i64 {
    let _ = keys;
    0
}

provisa::command!(delete_rows, delete = "rows");
