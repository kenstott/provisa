// Copyright (c) 2026 Kenneth Stott
// Canary: 2b8d4e61-7c35-4a9f-8e02-d5a1c3f6b974
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1386 (ONTOLOGY QUALITY IS A STANDARD REPORT): the report cells that open what they name. A
// report is any ops table, so a cell is a link by its column's name: a term's id opens the term,
// and a placeholder's review column opens the term with the Ontology list filtered to RELATED_TO.

import { entityHref } from "./entityHref";

/** The column naming a term a report row is about, by the term's id. */
const TERM_COLUMNS = new Set(["term_id", "other_term_id"]);
/** The column naming a term whose RELATED_TO placeholder wants review. */
const RELATED_TO_REVIEW_COLUMN = "related_to_review";

/** The Ontology page's URL parameter that turns its RELATED_TO filter on. */
export const RELATED_TO_PARAM = "relatedTo";

/** The page a report cell opens, or null when the cell is plain text. */
export function reportCellHref(column: string, value: unknown): string | null {
  if (typeof value !== "number" && !(typeof value === "string" && /^\d+$/.test(value))) {
    return null;
  }
  if (TERM_COLUMNS.has(column)) return entityHref("term", value);
  if (column === RELATED_TO_REVIEW_COLUMN) {
    return `${entityHref("term", value)}&${RELATED_TO_PARAM}=1`;
  }
  return null;
}
