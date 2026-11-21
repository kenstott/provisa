// Copyright (c) 2026 Kenneth Stott
// Canary: 645515f2-5387-4764-9f97-723b17c5ef62
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { tabel } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

export const rows = tabel(function rows(): Row[] {
  return [];
});
