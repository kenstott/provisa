// Copyright (c) 2026 Kenneth Stott
// Canary: 9c21b4ec-e04c-4682-b1e7-6b5723c1d217
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { table, type AsyncRows, type ReadContext } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Rows of a region, as the read asks. */
export const rows = table(async function* rows(inputs: Inputs, context: ReadContext): AsyncRows<Row> {});
