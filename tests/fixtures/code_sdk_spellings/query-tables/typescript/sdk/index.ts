// Copyright (c) 2026 Kenneth Stott
// Canary: 775782d7-4ae1-4ea8-81fa-6001a8e320a4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { query, type TableRef } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

export interface Match {
  left_id: number;
  right_id: number;
}

/** Rows of two tables that belong together. */
export const pairs = query(
  async function pairs(left: TableRef<Row>, right: TableRef<Row>): Promise<Match[]> {
    return [];
  },
);
