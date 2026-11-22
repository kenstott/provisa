// Copyright (c) 2026 Kenneth Stott
// Canary: 8ed14a70-0b60-4887-ad7e-53332c03af7a
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

/** Rows of one name.
 * @query
 */
export async function lookup(name: string): Promise<Row[]> {
  return [];
}
