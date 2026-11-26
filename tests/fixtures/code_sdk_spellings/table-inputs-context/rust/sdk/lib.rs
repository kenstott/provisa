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

pub struct Inputs {
    pub region: Option<String>,
}

/// The read context, as much of it as this code reads.
pub struct ReadContext {
    pub limit: Option<i64>,
}

/// Rows of a region, as the read asks.
pub fn rows(inputs: Inputs, context: ReadContext) -> impl Iterator<Item = Row> {
    let _ = (inputs, context);
    std::iter::empty()
}

provisa_sdk::table!(rows);
