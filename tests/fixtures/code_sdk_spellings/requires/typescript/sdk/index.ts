// Copyright (c) 2026 Kenneth Stott
// Canary: c546deda-ff64-4a8a-a115-ba9aee3e1269
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

/** Rows from the API. */
export const rows = table(
  function rows(): Row[] {
    return [];
  },
  { requires: ["API_KEY:secret", "REGION"] },
);
