// Copyright (c) 2026 Kenneth Stott
// Canary: 11283e62-e043-47c6-b3ae-a355b34e034f
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

/** @table */
export function rows(context: ReadContext, inputs: Inputs): Row[] {
  return [];
}
