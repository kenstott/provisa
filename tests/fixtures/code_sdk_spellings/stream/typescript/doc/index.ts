// Copyright (c) 2026 Kenneth Stott
// Canary: bf4c20b2-2e20-4071-b9d7-f66fd8c780b6
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

/** Each row as it arrives.
 * @stream
 */
export async function* ticks(inputs: Inputs): AsyncGenerator<Row> {}
