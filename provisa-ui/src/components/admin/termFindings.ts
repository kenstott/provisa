// Copyright (c) 2026 Kenneth Stott
// Canary: 3c9e7a12-58d4-4b0f-9e21-a6d47f0c83b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What a term needs a curator to look at (REQ-2146): the server's findings, each of a kind, which
// names its chip and its fix. A term with any finding is To review; a plain draft is not one.

import type { GlossaryTermSummary } from "../../api/glossary";

export const toReview = (term: Pick<GlossaryTermSummary, "findings">): boolean => term.findings > 0;

/** The table names in an unlinked_tables finding's ``tables`` (``source.schema.table, …``): what
 * the Relationships page's search finds them by. */
export const tableNames = (tables: string): string =>
  tables
    .split(", ")
    .map((t) => t.slice(t.lastIndexOf(".") + 1))
    .join(" ");
