// Copyright (c) 2026 Kenneth Stott
// Canary: d7cad740-aa8b-4eb7-a545-8461b2783d80
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

export interface Row {
  id: number;
  name: string;
}

/** @table */
export function rows(): Record<string, unknown>[] {
  return [];
}
