// Copyright (c) 2026 Kenneth Stott
// Canary: b8e6f803-5cd2-4414-8868-0a13d94451fd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { table, type Rows } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Rows of a region. */
export const rows = table(function rows(inputs: Inputs): Rows<Row> {
  return [];
});
