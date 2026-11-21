// Copyright (c) 2026 Kenneth Stott
// Canary: 736e17be-d99d-4ea2-bdbc-9bb4f47d80be
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

export const rows = table(function rows(): Record<string, unknown>[] {
  return [];
});
