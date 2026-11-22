// Copyright (c) 2026 Kenneth Stott
// Canary: 7e8348d1-4bd7-4570-bdbd-a4d98f97d9e7
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

/** @health */
export function status(): number {
  return 1;
}
