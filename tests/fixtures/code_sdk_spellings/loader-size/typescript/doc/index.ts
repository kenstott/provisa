// Copyright (c) 2026 Kenneth Stott
// Canary: 916d54a8-ae0e-4161-9425-78b6174a6482
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

/** Rows of the keys, 100 at a time.
 * @table
 * @loader 100
 */
export async function rows(context: ReadContext): Promise<Row[]> {
  return [];
}
