// Copyright (c) 2026 Kenneth Stott
// Canary: ae9944fc-c4ef-425c-bb93-8d4cb5ab1fdd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The `replicate` setting (REQ-826): when a table is served from its replica. One integer per
// table, inherited from its source when the table has none. The values mirror
// provisa/core/replicate.py: not set = Default (the global threshold), -1 = Never, N > 0 = once
// the table passes N governed statements per interval (Hot-N), 0 = Always.

export const REPLICATE_NEVER = -1;
export const REPLICATE_ALWAYS = 0;

/** The Hot thresholds the drop-down offers. Any threshold above 0 is a legal stored value. */
export const REPLICATE_HOT_THRESHOLDS = [50, 100, 500, 1000, 10000] as const;

/** The drop-down's option value for a stored setting. */
export function replicateOptionValue(replicate: number | null): string {
  return replicate === null ? "default" : String(replicate);
}

/** The stored setting for a drop-down option value. */
export function parseReplicateOption(option: string): number | null {
  return option === "default" ? null : Number(option);
}

/**
 * The values the drop-down lists, in order: Default, Never, the Hot thresholds, Always. A stored
 * threshold that is not one of the standard ones is listed in its place among them, so it is
 * shown as it is and kept when the form is saved.
 */
export function replicateValues(stored: number | null): (number | null)[] {
  const hot: number[] = [...REPLICATE_HOT_THRESHOLDS];
  if (stored !== null && stored > REPLICATE_ALWAYS && !hot.includes(stored)) {
    hot.push(stored);
    hot.sort((a, b) => a - b);
  }
  return [null, REPLICATE_NEVER, ...hot, REPLICATE_ALWAYS];
}

/** A table's setting: its own value, else its source's. */
export function resolvedReplicate(
  table: number | null | undefined,
  source: number | null | undefined,
): number | null {
  return table ?? source ?? null;
}

/**
 * Whether the settings say the table is replicated — now (load protection, Always) or once it is
 * busy (Hot-N). Default and Never do not. These are the tables whose TTL-based change signal
 * needs a cache TTL (REQ-1907), as provisa.core.replicate.may_replicate judges them.
 */
export function saysReplicated(replicate: number | null, loadProtected: boolean): boolean {
  return loadProtected || (replicate !== null && replicate >= REPLICATE_ALWAYS);
}

/** Load protection with Never is contradictory: a load-protected table is never read live. */
export function replicateContradictsLoadProtection(
  replicate: number | null,
  loadProtected: boolean,
): boolean {
  return loadProtected && replicate === REPLICATE_NEVER;
}
