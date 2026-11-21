// Copyright (c) 2026 Kenneth Stott
// Canary: 997aeeab-cffc-45fc-8077-3d6cde8dd0cf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { command } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** Cancel one order. */
export const cancel = command(async function cancel(order_id: string): Promise<number> {
  return 1;
});
