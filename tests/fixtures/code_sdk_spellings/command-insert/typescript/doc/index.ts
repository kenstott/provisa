// Copyright (c) 2026 Kenneth Stott
// Canary: 25a3fd9e-0493-45f5-ae5a-4176ecdb2cfb
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

/** The insert of rows.
 * @command
 * @insert rows
 */
export async function insertRows(rows: Row[]): Promise<number> {
  return 0;
}
