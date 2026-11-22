// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

use provisa_sdk::*;

pub struct Row {
    pub id: i64,
    pub name: String,
}

pub struct Inputs {
    pub region: Option<String>,
}

/// Each row as it arrives.
pub fn ticks(inputs: Inputs) -> Stream<Result<Row, String>> {
    let _ = inputs;
    Box::new(std::iter::once(Ok(Row { id: 1, name: "a".into() })))
}

stream!(ticks);
