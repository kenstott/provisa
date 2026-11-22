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

pub struct Changed {
    pub left_id: i64,
    pub right_id: i64,
    pub removed: bool,
    pub seq: i64,
}

/// Matches changed since `since`.
///
/// @change table=pairs
/// @change key=left_id,right_id
/// @change removed=removed
/// @change cursor=seq
pub fn pairs_changes(left: TableRef<Row>, right: TableRef<Row>, since: Option<i64>) -> Vec<Changed> {
    let _ = (left, right, since);
    Vec::new()
}
