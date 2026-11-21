// Copyright (c) 2026 Kenneth Stott
// Canary: 0f52efc2-e3d3-4294-adfd-dbaaa3e770e2
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

/** Rows the read asks for.
 * @table
 */
export async function rows(context: ReadContext): Promise<Row[]> {
  return [];
}
