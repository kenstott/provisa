// Copyright (c) 2026 Kenneth Stott
// Canary: bf9acb73-6c39-4326-9b84-895dcef7789b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { command } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** The insert of rows. */
export const insertRows = command(
  async function insertRows(rows: Row[]): Promise<number> {
    return 0;
  },
  { insert: "rows" },
);
