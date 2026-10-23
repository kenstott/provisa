// Copyright (c) 2026 Kenneth Stott
// Canary: 3c9e7a12-58d4-4b0f-9e21-a6d47f0c83b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What a term needs a curator to look at: each finding a kind, which names its message and its fix,
// and the values the message names. A term with any finding is To review; a plain draft is not.

import type { GlossaryTermSummary } from "../../api/glossary";

export const FINDING_KINDS = [
  "unlinked_tables",
  "class_and_attribute",
  "tag_wrong_kind",
  "retirement_blocked",
  "no_data",
] as const;
export type FindingKind = (typeof FINDING_KINDS)[number];

export interface TermFinding {
  kind: FindingKind;
  params: Record<string, string>;
}

/** A term's findings. Until the server lists them, the one it computes on every term: grounded in
 * several tables that same-entity relationships do not all join. */
export function termFindings(term: Pick<GlossaryTermSummary, "identity_gap">): TermFinding[] {
  return term.identity_gap ? [{ kind: "unlinked_tables", params: {} }] : [];
}

export const toReview = (term: Pick<GlossaryTermSummary, "identity_gap">): boolean =>
  termFindings(term).length > 0;
