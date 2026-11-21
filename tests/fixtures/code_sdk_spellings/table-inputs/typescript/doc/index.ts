// Copyright (c) 2026 Kenneth Stott
// Canary: 52cbac61-8339-4eb9-aa31-d862eefcad68
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Rows of a region.
 * @table
 */
export function rows(inputs: Inputs): Iterable<Row> {
  return [];
}
