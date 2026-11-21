// Copyright (c) 2026 Kenneth Stott
// Canary: 22a1045c-84ed-4c30-bc9d-7211f934f238
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

/** Cancel one order.
 * @command
 */
export async function cancel(order_id: string): Promise<number> {
  return 1;
}
