// Copyright (c) 2026 Kenneth Stott
// Canary: 9144a443-7d20-4f64-87fe-1b7f8207968b
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

export interface Changes {
  name?: string;
}

/** The update of rows. */
export const updateRows = command(
  async function updateRows(keys: number[], changes: Changes): Promise<number> {
    return 0;
  },
  { update: "rows" },
);
