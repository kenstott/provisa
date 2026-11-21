// Copyright (c) 2026 Kenneth Stott
// Canary: fdb96441-5f08-491c-b4c0-3683b6580e2d
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

/** Every row. */
export const rows = table(function rows(): Row[] {
  return [];
});
