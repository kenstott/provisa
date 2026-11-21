// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

use std::marker::PhantomData;

pub struct Row {
    pub id: i64,
    pub name: String,
}

pub struct TableRef<T> {
    pub name: String,
    pub row: PhantomData<T>,
}

pub struct Match {
    pub left_id: i64,
    pub right_id: i64,
}

/// Rows of two tables that belong together.
///
/// @query
pub fn pairs(left: TableRef<Row>, right: TableRef<Row>) -> Vec<Match> {
    let _ = (left, right);
    Vec::new()
}
