// Copyright (c) 2026 Kenneth Stott
// Canary: f7cd61fc-4ea3-47b8-8b84-ccd7b3d5a553
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { table, type ReadContext } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** Rows the read asks for. */
export const rows = table(async function rows(context: ReadContext): Promise<Row[]> {
  return [];
});
