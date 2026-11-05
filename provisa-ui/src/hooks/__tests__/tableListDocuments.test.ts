// Copyright (c) 2026 Kenneth Stott
// Canary: a8afd3b7-caf7-451a-a788-66f8e971cd50
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The pages that list tables read a projection of the editor's rows (TableRowsQuery, TableListQuery),
// typed by LISTED_TABLE_FIELDS and LISTED_COLUMN_FIELDS. The documents and the types name the same
// fields, and the editor's TablesQuery selects every one of them: under the table editor a list
// reader answers from the rows TablesQuery cached, with no request of its own.

import { describe, expect, it } from "vitest";
import type { DocumentNode, FieldNode, SelectionSetNode } from "graphql";
import { TableListQuery, TableRowsQuery, TablesQuery } from "../admin.graphql";
import { LISTED_COLUMN_FIELDS, LISTED_TABLE_FIELDS } from "../../types/admin";

/** The field names a selection set reads, its fragments' included. */
function fieldsOf(doc: DocumentNode, set: SelectionSetNode): Map<string, FieldNode> {
  const out = new Map<string, FieldNode>();
  for (const s of set.selections) {
    if (s.kind === "Field") out.set(s.name.value, s);
    if (s.kind === "FragmentSpread") {
      const frag = doc.definitions.find(
        (d) => d.kind === "FragmentDefinition" && d.name.value === s.name.value,
      );
      if (frag?.kind !== "FragmentDefinition") throw new Error(`no fragment ${s.name.value}`);
      for (const [k, v] of fieldsOf(doc, frag.selectionSet)) out.set(k, v);
    }
  }
  return out;
}

function tableFields(doc: DocumentNode): Map<string, FieldNode> {
  const op = doc.definitions.find((d) => d.kind === "OperationDefinition");
  if (op?.kind !== "OperationDefinition") throw new Error("no operation");
  const tables = fieldsOf(doc, op.selectionSet).get("tables");
  if (!tables?.selectionSet) throw new Error("no tables selection");
  return fieldsOf(doc, tables.selectionSet);
}

function columnFields(doc: DocumentNode): string[] {
  const columns = tableFields(doc).get("columns");
  if (!columns?.selectionSet) throw new Error("no columns selection");
  return [...fieldsOf(doc, columns.selectionSet).keys()];
}

describe("the table list documents", () => {
  it("TableRowsQuery reads exactly LISTED_TABLE_FIELDS, without columns", () => {
    expect([...tableFields(TableRowsQuery).keys()].sort()).toEqual([...LISTED_TABLE_FIELDS].sort());
  });

  it("TableListQuery reads LISTED_TABLE_FIELDS and columns of LISTED_COLUMN_FIELDS", () => {
    expect([...tableFields(TableListQuery).keys()].sort()).toEqual(
      [...LISTED_TABLE_FIELDS, "columns"].sort(),
    );
    expect(columnFields(TableListQuery).sort()).toEqual([...LISTED_COLUMN_FIELDS].sort());
  });

  it("TablesQuery reads every field the list documents read", () => {
    const editor = tableFields(TablesQuery);
    for (const f of LISTED_TABLE_FIELDS) expect(editor.has(f), f).toBe(true);
    const editorColumns = new Set(columnFields(TablesQuery));
    for (const f of LISTED_COLUMN_FIELDS) expect(editorColumns.has(f), f).toBe(true);
  });
});
