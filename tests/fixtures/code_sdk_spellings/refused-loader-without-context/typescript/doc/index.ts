// Copyright (c) 2026 Kenneth Stott
// Canary: d5cdfc66-3ec3-4af3-9706-125509454010
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

/** @table
 * @loader
 */
export function rows(inputs: Inputs): Row[] {
  return [];
}
