// Copyright (c) 2026 Kenneth Stott
// Canary: a95faa71-2324-42ef-847f-273140cd9c2c
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
export interface Inputs {
  region?: string;
}

/** Rows of every key in one batch. */
export const rows = table(
  async function rows(inputs: Inputs, context: ReadContext): Promise<Row[]> {
    return [];
  },
  { loader: true },
);
