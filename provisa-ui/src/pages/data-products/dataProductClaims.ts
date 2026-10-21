// Copyright (c) 2026 Kenneth Stott
// Canary: 5b3e8d17-c942-4a06-81f7-2d9a6e4b3c58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2132: a product's checks summarised: which state each is in, and how their latest runs went.

import type { CheckState, CoreCheck } from "./productCore";

export type DqStatus = "passing" | "failing" | "notRun";

/** The summary of ``checks``' latest runs: some failing, else not yet run, else all passing; null
 * when the role's schema does not say how they ran. */
export function dqStatus(checks: CoreCheck[]): DqStatus | null {
  if (checks.length === 0) return null;
  const outcomes = checks.map((c) => c.checker?.latest_outcome);
  if (outcomes.some((o) => o === undefined)) return null;
  if (outcomes.includes("fail")) return "failing";
  if (outcomes.includes(null)) return "notRun";
  return "passing";
}

export const checksIn = (checks: CoreCheck[] | undefined, state: CheckState) =>
  (checks ?? []).filter((c) => c.state === state);
