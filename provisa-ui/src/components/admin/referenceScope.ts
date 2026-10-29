// Copyright (c) 2026 Kenneth Stott
// Canary: 4e9b2c71-0a5d-4f38-86c3-d2f7a1b9e054
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2185: a reference library document's domains, where `*` (the whole org) stands alone.

/** The whole org, as a glossary term's `*`. */
export const WHOLE_ORG = "*";

/** A domain selection where `*` stands alone: choosing it clears the others, and the reverse. */
export function scopeChange(previous: string[], next: string[]): string[] {
  const added = next.filter((d) => !previous.includes(d));
  if (added.includes(WHOLE_ORG)) return [WHOLE_ORG];
  return next.filter((d) => d !== WHOLE_ORG);
}
