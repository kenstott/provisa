// Copyright (c) 2026 Kenneth Stott
// Canary: 79fbdd59-298c-464a-8e97-27250cff8731
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { health, onStart, onStop, table } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** Open the client. */
export const start = onStart(async function start(values: Record<string, string>): Promise<void> {});

/** Close the client. */
export const stop = onStop(function stop(): void {});

export const status = health(async function status(): Promise<string> {
  return "ok";
});

export const rows = table(function rows(): Row[] {
  return [];
});
