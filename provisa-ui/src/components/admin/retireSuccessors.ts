// Copyright (c) 2026 Kenneth Stott
// Canary: 2a7d5c90-8e14-4f3b-b6a2-d1c9e04f7b38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Who can succeed a retired term (REQ-2144): a term in service, of its role or a concept.

import { termRole, type GlossaryTermDetail, type GlossaryTermSummary } from "../../api/glossary";

/** The terms that can take ``term``'s columns and tables: in service, and of its role or a
 * concept (a concept's own successor may be any term). */
export function successorOptions(term: GlossaryTermDetail, terms: GlossaryTermSummary[]) {
  const role = termRole(term);
  return terms
    .filter((o) => o.id !== term.id && !o.retired)
    .filter((o) => role === "concept" || [role, "concept"].includes(termRole(o)))
    .map((o) => ({ value: String(o.id), label: o.name }));
}
