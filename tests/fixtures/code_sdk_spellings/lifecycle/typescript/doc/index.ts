// Copyright (c) 2026 Kenneth Stott
// Canary: c768e934-5c1b-4e43-aa01-88341ce954fa
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

/** Open the client.
 * @on-start
 */
export async function start(values: Record<string, string>): Promise<void> {}

/** Close the client.
 * @on-stop
 */
export function stop(): void {}

/** @health */
export async function status(): Promise<string> {
  return "ok";
}

/** @table */
export function rows(): Row[] {
  return [];
}
