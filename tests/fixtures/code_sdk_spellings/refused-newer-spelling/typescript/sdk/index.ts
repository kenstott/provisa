// Copyright (c) 2026 Kenneth Stott
// Canary: 7788279c-ddb2-4b00-bc1c-36b08c833b78
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Tagged } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

export declare const rows: (() => Row[]) & Tagged<"table", {}, 2>;
