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

/// Rows from the API.
///
/// @table
/// @requires API_KEY:secret
/// @requires REGION
pub fn rows() -> Vec<Row> {
    Vec::new()
}
