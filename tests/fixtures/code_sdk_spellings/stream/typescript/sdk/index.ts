// Copyright (c) 2026 Kenneth Stott
// Canary: 46c14f61-b987-410a-b927-220fe125b05f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { stream } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Each row as it arrives. */
export const ticks = stream(async function* ticks(inputs: Inputs): AsyncGenerator<Row> {});
