// Copyright (c) 2026 Kenneth Stott
// Canary: 1106c317-ee4e-4b35-9091-e9c4fa0b2181
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

/** The delete of rows.
 * @command
 * @delete rows
 */
export async function deleteRows(keys: number[]): Promise<number> {
  return 0;
}
