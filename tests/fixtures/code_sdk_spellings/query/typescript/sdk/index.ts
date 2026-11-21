// Copyright (c) 2026 Kenneth Stott
// Canary: d9bc9aff-2d84-491d-98af-d96e00979dc3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { query } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** Rows of one name. */
export const lookup = query(async function lookup(name: string): Promise<Row[]> {
  return [];
});
