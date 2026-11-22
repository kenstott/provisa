// Copyright (c) 2026 Kenneth Stott
// Canary: 28c94e73-68f3-4e06-ab45-8be6ca1df07d
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

/** Every row.
 * @table
 */
export function rows(): Row[] {
  return [];
}
