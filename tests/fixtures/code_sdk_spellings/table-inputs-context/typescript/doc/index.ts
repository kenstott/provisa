// Copyright (c) 2026 Kenneth Stott
// Canary: 39097389-7c10-452d-9e39-132d28b033c0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ReadContext } from "@provisa/services";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Rows of a region, as the read asks.
 * @table
 */
export async function* rows(inputs: Inputs, context: ReadContext): AsyncIterable<Row> {}
