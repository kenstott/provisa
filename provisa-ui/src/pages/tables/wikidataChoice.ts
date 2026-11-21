// Copyright (c) 2026 Kenneth Stott
// Canary: 3b9d7f52-e18a-4c63-a4f0-8d2e6c1b7a45
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: what choosing a Wikidata table fills the table form with (WikidataTableChooser).

import type { WikidataColumn } from "../../api/wikidata";

/** What a choice fills the form with. */
export interface WikidataChoice {
  tableName: string;
  query: string;
  columns: WikidataColumn[];
}

/** How many of a class's suggested columns start picked. */
export const PICKED_AT_FIRST = 6;

/** A label as a table name: lower case, words joined by underscores. */
export function tableNameOf(label: string): string {
  return label
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}
