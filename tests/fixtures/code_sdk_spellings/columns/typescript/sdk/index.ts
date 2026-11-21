// Copyright (c) 2026 Kenneth Stott
// Canary: 602374e4-ab99-4b45-8f9c-9da21e17913b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { columns, type Batch } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

export interface Upper {
  upper: string;
}

/** Each name in capitals. */
export const shout = columns(function shout(names: string[]): Batch<Upper> {
  return names.map((n) => ({ upper: n.toUpperCase() }));
});
