// Copyright (c) 2026 Kenneth Stott
// Canary: c2bfc517-ebd0-4918-90e2-892164d9634e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { table } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

export const rows = table(
  function rows(inputs: Inputs): Row[] {
    return [];
  },
  { loader: true },
);
