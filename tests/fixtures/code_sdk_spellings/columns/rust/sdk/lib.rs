// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

use provisa_sdk::{columns, Batch, Results};

pub struct Row {
    pub id: i64,
    pub name: String,
}

pub struct Upper {
    pub upper: String,
}

/// Each name in capitals.
pub fn shout(names: Batch<String>) -> Results<Upper> {
    names.iter().map(|n| Upper { upper: n.to_uppercase() }).collect()
}

columns!(shout);
