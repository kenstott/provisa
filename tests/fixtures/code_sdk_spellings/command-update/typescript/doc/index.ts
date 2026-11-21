// Copyright (c) 2026 Kenneth Stott
// Canary: bdf0e7ea-b56e-4874-b807-35c56d4e79b3
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

export interface Changes {
  name?: string;
}

/** The update of rows.
 * @command
 * @update rows
 */
export async function updateRows(keys: number[], changes: Changes): Promise<number> {
  return 0;
}
