// Copyright (c) 2026 Kenneth Stott
// Canary: 3d7e1a58-6b2c-4f94-8e05-a9c4d2f7b613
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923 (amended 2026-11-20, THE QUERY IS SHOWN AND EDITED ON THE TABLE): a SPARQL-defined
// table shows its query in Identity; editing it re-derives the columns from the SELECT variables
// and warns before dropping a column a relationship, write grant or mask uses.

import { useEffect, useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, renderInForm, screen, waitFor } from "../../../test-utils/render";
import type { RegisteredTable, Relationship, Source } from "../../../types/admin";
import { draftColumn } from "../draftTable";

const fetchSparqlColumns = vi.hoisted(() => vi.fn());
vi.mock("../../../api/wikidata", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/wikidata")>()),
  fetchSparqlColumns,
}));
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: [{ id: "admin" }, { id: "analyst" }], loading: false }),
}));

const { SparqlQueryField } = await import("../SparqlQueryField");
const { IdentityRead, QueryRead } = await import("../TableSectionsRead");

const QUERY = "SELECT ?id ?name ?capital WHERE { ?c wdt:P297 ?id }";
const SPARQL = { id: "wd", type: "sparql" } as Source;

const TABLE = {
  id: 7,
  sourceId: "wd",
  domainId: "sales",
  schemaName: "sparql",
  tableName: "countries",
  alias: null,
  description: null,
  region: null,
  gqlNamingConvention: null,
  apiEndpoint: null,
  apiEndpointError: null,
  queryTemplate: QUERY,
  writeGrants: [],
  columns: [
    { ...draftColumn({ name: "id", dataType: "text", visibleTo: ["admin"] }, 0), id: 31 },
    {
      ...draftColumn({ name: "name", dataType: "text", visibleTo: ["admin"] }, 1),
      id: 32,
      description: "The country's English label",
    },
    { ...draftColumn({ name: "capital", dataType: "text", visibleTo: ["admin"] }, 2), id: 33 },
  ],
} as unknown as RegisteredTable;

const JOINED: Relationship = {
  id: 1,
  sourceTableId: 9,
  targetTableId: 7,
  sourceColumn: "capital_id",
  targetColumn: "capital",
} as Relationship;

// The table as the editor last left it.
const seen: { table: RegisteredTable | null } = { table: null };
function Editor({ relationships }: { relationships: Relationship[] }) {
  const [table, setTable] = useState<RegisteredTable | null>(TABLE);
  useEffect(() => {
    seen.table = table;
  }, [table]);
  return table ? (
    <SparqlQueryField
      editingTable={table}
      setEditingTable={setTable}
      relationships={relationships}
    />
  ) : null;
}

beforeEach(() => {
  vi.clearAllMocks();
  seen.table = null;
});

describe("a SPARQL-defined table's query", () => {
  it("is shown read-only in Identity", () => {
    render(<IdentityRead table={TABLE} source={SPARQL} />);
    expect(screen.getByTestId("table-identity-sparql-query")).toHaveTextContent(QUERY);
  });

  it("is never shown on the query tab, labelled Cypher or otherwise", () => {
    render(<QueryRead table={TABLE} />);
    expect(screen.queryByText(QUERY)).toBeNull();
    expect(screen.queryByText("Cypher")).toBeNull();
  });

  it("re-derives the columns from the SELECT when edited, keeping the ones it keeps", async () => {
    fetchSparqlColumns.mockResolvedValue([
      { name: "id", data_type: "text" },
      { name: "name", data_type: "text" },
      { name: "capital", data_type: "text" },
      { name: "population", data_type: "text" },
    ]);
    renderInForm(<Editor relationships={[]} />);
    const edited = "SELECT ?id ?name ?capital ?population WHERE { ?c wdt:P297 ?id }";
    fireEvent.change(screen.getByTestId("table-edit-sparql-query"), {
      target: { value: edited },
    });
    await waitFor(() => expect(seen.table?.queryTemplate).toBe(edited));
    expect(fetchSparqlColumns).toHaveBeenCalledWith(edited);
    expect(seen.table!.columns.map((c) => c.columnName)).toEqual([
      "id",
      "name",
      "capital",
      "population",
    ]);
    expect(seen.table!.columns[1]).toMatchObject({
      id: 32,
      description: "The country's English label",
    });
    expect(seen.table!.columns[3]).toMatchObject({
      dataType: "text",
      visibleTo: ["admin", "analyst"],
    });
  });

  it("warns before dropping a column a relationship uses, and drops it once agreed", async () => {
    fetchSparqlColumns.mockResolvedValue([
      { name: "id", data_type: "text" },
      { name: "name", data_type: "text" },
    ]);
    renderInForm(<Editor relationships={[JOINED]} />);
    const edited = "SELECT ?id ?name WHERE { ?c wdt:P297 ?id }";
    fireEvent.change(screen.getByTestId("table-edit-sparql-query"), {
      target: { value: edited },
    });
    expect(await screen.findByTestId("table-edit-sparql-drops")).toHaveTextContent(
      "capital (a relationship)",
    );
    expect(seen.table!.queryTemplate).toBe(QUERY);
    expect(seen.table!.columns).toHaveLength(3);

    fireEvent.click(screen.getByTestId("table-edit-sparql-drop-agree"));
    await waitFor(() => expect(seen.table?.queryTemplate).toBe(edited));
    expect(seen.table!.columns.map((c) => c.columnName)).toEqual(["id", "name"]);
  });

  it("names a mask and a write grant on a column it would drop", async () => {
    fetchSparqlColumns.mockResolvedValue([{ name: "id", data_type: "text" }]);
    const masked = {
      ...TABLE,
      writeGrants: [
        {
          role: "analyst",
          operation: "update",
          columns: ["capital"],
          rows: null,
          check: null,
          presets: [],
        },
      ],
      columns: TABLE.columns.map((c) =>
        c.columnName === "name"
          ? { ...c, masks: [{ roles: ["analyst"], mask: "redact" }] as unknown as typeof c.masks }
          : c,
      ),
    } as RegisteredTable;
    function MaskedEditor() {
      const [table, setTable] = useState<RegisteredTable | null>(masked);
      return table ? (
        <SparqlQueryField editingTable={table} setEditingTable={setTable} relationships={[]} />
      ) : null;
    }
    renderInForm(<MaskedEditor />);
    fireEvent.change(screen.getByTestId("table-edit-sparql-query"), {
      target: { value: "SELECT ?id WHERE { ?c wdt:P297 ?id }" },
    });
    const warning = await screen.findByTestId("table-edit-sparql-drops");
    expect(warning).toHaveTextContent("name (a mask)");
    expect(warning).toHaveTextContent("capital (a write grant)");
  });

  it("says why a query's columns cannot be read from it", async () => {
    fetchSparqlColumns.mockRejectedValue(new Error("The query names no SELECT variables"));
    renderInForm(<Editor relationships={[]} />);
    fireEvent.change(screen.getByTestId("table-edit-sparql-query"), {
      target: { value: "SELECT * WHERE { ?s ?p ?o }" },
    });
    expect(await screen.findByTestId("table-edit-sparql-query-problem")).toHaveTextContent(
      "names no SELECT variables",
    );
    expect(seen.table!.queryTemplate).toBe(QUERY);
  });
});
