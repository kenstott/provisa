// Copyright (c) 2026 Kenneth Stott
// Canary: 64a94ce7-fc0c-4743-92db-4a73d426d692
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The term each column is: a column is one term's at most (its ref), so a term claims a column by
// moving the column's ref to itself. Read from the meta domain as the acting roles, the way the
// Ontology View reads the glossary.

import { dataGraphql, rootQuery } from "../../api/metaGraph";
import { metaReader } from "../graph3d/useProductGraph";

/** The term a column is now: its name, and whether it is still a draft (not live). */
export interface ColumnTerm {
  id: number;
  name: string;
  draft: boolean;
}

export const columnKey = (tableId: number, column: string) => `${tableId}:${column}`;

interface Row {
  id: number;
  name: string;
  live: boolean;
  refs: { table_id: number; column_name: string }[] | null;
}

/** Each column with a term, keyed by ``columnKey``; null when the roles cannot read the glossary. */
export async function columnTerms(role: string): Promise<Map<string, ColumnTerm> | null> {
  const query = rootQuery(await metaReader(role), "ColumnTerms", "_terms", {
    fields: ["id", "name", "live"],
    rels: { refs: { alias: "refs", sel: { fields: ["table_id", "column_name"] } } },
  });
  if (query === null) return null;
  const rows = await dataGraphql<{ _terms: Row[] }>(query, {}, role);
  const out = new Map<string, ColumnTerm>();
  for (const t of rows._terms)
    for (const r of t.refs ?? [])
      out.set(columnKey(r.table_id, r.column_name), { id: t.id, name: t.name, draft: !t.live });
  return out;
}
