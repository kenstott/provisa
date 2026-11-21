// Copyright (c) 2026 Kenneth Stott
// Canary: d91ef280-e520-4cc5-92ad-64fc27e297fc
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

/** The delete of rows. */
export const deleteRows = command(
  async function deleteRows(keys: number[]): Promise<number> {
    return 0;
  },
  { delete: "rows" },
);
