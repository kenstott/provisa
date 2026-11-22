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

/// Cancel one order.
///
/// @command
pub fn cancel(order_id: i64) -> Result<i64, String> {
    let _ = order_id;
    Ok(1)
}
