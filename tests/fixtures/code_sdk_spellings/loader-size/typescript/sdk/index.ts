// Copyright (c) 2026 Kenneth Stott
// Canary: 09e98e35-ffb9-4f70-a529-97e34183f026
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { table, type ReadContext } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** Rows of the keys, 100 at a time. */
export const rows = table(
  async function rows(context: ReadContext): Promise<Row[]> {
    return [];
  },
  { loader: 100 },
);
