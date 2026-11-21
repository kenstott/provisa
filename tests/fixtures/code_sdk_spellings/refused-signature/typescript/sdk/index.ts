// Copyright (c) 2026 Kenneth Stott
// Canary: 3ab32f00-cfee-46ef-9cbb-60deaf96a52f
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
export interface Inputs {
  region?: string;
}

export const rows = table(function rows(context: ReadContext, inputs: Inputs): Row[] {
  return [];
});
