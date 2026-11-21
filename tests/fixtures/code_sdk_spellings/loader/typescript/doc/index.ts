// Copyright (c) 2026 Kenneth Stott
// Canary: 70db5db2-daa5-4f60-9a2e-f53d933b235a
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
export interface Inputs {
  region?: string;
}

/** Rows of every key in one batch.
 * @table
 * @loader
 */
export async function rows(inputs: Inputs, context: ReadContext): Promise<Row[]> {
  return [];
}
