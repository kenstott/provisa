// Copyright (c) 2026 Kenneth Stott
// Canary: d8e1f5b6-313f-4577-8a22-e60a2695133e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130 (class grounding): a term grounded in tables (the term editor's "Grounded in tables"),
// a table's "Rows are instances of <term>", and each term's role: Class, Property or Concept.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";

const api = vi.hoisted(() => ({
  grounded: new Map<number, number[]>(),
  calls: [] as string[],
  refuse: null as string | null,
}));
const TABLES = [
  {
    table_id: 10,
    source_id: "erp",
    schema_name: "public",
    table_name: "customers",
    domain_id: "sales",
  },
  {
    table_id: 11,
    source_id: "erp",
    schema_name: "public",
    table_name: "orders",
    domain_id: "sales",
  },
];
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listTermTables: async (termId: number) =>
    (api.grounded.get(termId) ?? []).map((id) => TABLES.find((t) => t.table_id === id)),
  groundTermTable: async (termId: number, tableId: number) => {
    api.calls.push(`ground ${termId} ${tableId}`);
    if (api.refuse) throw new Error(api.refuse);
    api.grounded.set(termId, [...(api.grounded.get(termId) ?? []), tableId]);
  },
  ungroundTermTable: async (termId: number, tableId: number) => {
    api.calls.push(`unground ${termId} ${tableId}`);
    api.grounded.set(
      termId,
      (api.grounded.get(termId) ?? []).filter((id) => id !== tableId),
    );
  },
  listGlossaryTerms: async () => [
    { id: 1, name: "Customer" },
    { id: 2, name: "Order" },
  ],
}));
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useTables: () => ({
    tables: TABLES.map((t) => ({
      id: t.table_id,
      domainId: t.domain_id,
      tableName: t.table_name,
      draft: false,
    })),
    loading: false,
  }),
}));
const auth = vi.hoisted(() => ({ capabilities: ["glossary_read", "glossary_rw"] as string[] }));
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ ...auth, actingRoles: "analyst" }),
}));
// The table's class term as the meta domain answers it: from the grounding above.
vi.mock("../components/graph3d/useProductGraph", () => ({
  metaReader: async () => ({ schema: null, roots: new Map() }),
}));
vi.mock("../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/metaGraph")>()),
  rootQuery: (_m: unknown, _n: string, _v: string, _s: unknown, where: { id: { eq: number } }) =>
    JSON.stringify(where.id.eq),
  dataGraphql: async (query: string) => {
    const tableId = Number(JSON.parse(query));
    const term = [...api.grounded.entries()].find(([, ids]) => ids.includes(tableId))?.[0];
    const names: Record<number, string> = { 1: "Customer", 2: "Order" };
    return {
      registered_tables: [
        { id: tableId, class_term: term === undefined ? null : { id: term, name: names[term] } },
      ],
    };
  },
}));

import { GlossaryGroundedTables } from "../components/admin/GlossaryGroundedTables";
import { TableClassTerm } from "../pages/tables/TableClassTerm";
import { TermListBadges } from "../components/admin/TermRoleBadge";
import { termRole, type GlossaryTermSummary } from "../api/glossary";

beforeEach(() => {
  api.grounded = new Map();
  api.calls = [];
  api.refuse = null;
  auth.capabilities = ["glossary_read", "glossary_rw"];
});

const SUMMARY: GlossaryTermSummary = {
  id: 1,
  name: "Customer",
  definition: "One who buys.",
  is_abstract: false,
  deprecated: false,
  ref_count: 0,
  table_count: 0,
  identity_gap: false,
  export_excluded: false,
  retired: false,
  live: true,
  grounded: true,
  domains: [],
};

describe("a term's role", () => {
  it("is Class with a table, else Property with a column, else Concept", () => {
    expect(termRole({ table_count: 1, ref_count: 3 })).toBe("class");
    expect(termRole({ table_count: 0, ref_count: 2 })).toBe("property");
    expect(termRole({ table_count: 0, ref_count: 0 })).toBe("concept");
  });

  it("is badged on each term in the list", () => {
    render(<TermListBadges term={{ ...SUMMARY, table_count: 1 }} />);
    expect(screen.getByTestId("glossary-role-1")).toHaveTextContent("Class");
    render(<TermListBadges term={{ ...SUMMARY, id: 2, ref_count: 1 }} />);
    expect(screen.getByTestId("glossary-role-2")).toHaveTextContent("Property");
    render(<TermListBadges term={{ ...SUMMARY, id: 3 }} />);
    expect(screen.getByTestId("glossary-role-3")).toHaveTextContent("Concept");
  });
});

describe("a class whose tables are not all declared one entity", () => {
  it("is badged a gap in the list, and the term editor says what to declare", async () => {
    render(<TermListBadges term={{ ...SUMMARY, table_count: 2, identity_gap: true }} />);
    expect(screen.getByTestId("glossary-gap-1")).toHaveTextContent("Gap");
    render(<TermListBadges term={{ ...SUMMARY, id: 2, table_count: 2 }} />);
    expect(screen.queryByTestId("glossary-gap-2")).toBeNull();
    api.grounded.set(1, [10, 11]);
    render(<GlossaryGroundedTables termId={1} identityGap canEdit onChanged={vi.fn()} />);
    expect(await screen.findByTestId("glossary-identity-gap")).toHaveTextContent("same entity");
  });
});

describe("a term's Grounded in tables", () => {
  it("grounds and lets go of each table as it is chosen, and says so", async () => {
    const changed = vi.fn();
    render(<GlossaryGroundedTables termId={1} identityGap={false} canEdit onChanged={changed} />);
    const select = await screen.findByTestId("glossary-grounded-tables-select");
    fireEvent.click(select);
    fireEvent.click(await screen.findByRole("option", { name: "sales.customers", hidden: true }));
    await waitFor(() => expect(api.calls).toEqual(["ground 1 10"]));
    expect(changed).toHaveBeenCalledTimes(1);
    await waitFor(() =>
      expect(screen.getByTestId("glossary-grounded-tables")).toHaveTextContent("sales.customers"),
    );
  });

  it("says why the server refused, and keeps what was grounded", async () => {
    api.refuse = "table orders has no primary key";
    const changed = vi.fn();
    render(<GlossaryGroundedTables termId={1} identityGap={false} canEdit onChanged={changed} />);
    fireEvent.click(await screen.findByTestId("glossary-grounded-tables-select"));
    fireEvent.click(await screen.findByRole("option", { name: "sales.orders", hidden: true }));
    expect(await screen.findByTestId("glossary-grounded-tables-error")).toHaveTextContent(
      "table orders has no primary key",
    );
    expect(changed).not.toHaveBeenCalled();
  });

  it("is listed, not chosen, to a reader who may not curate", async () => {
    api.grounded.set(1, [10, 11]);
    render(
      <GlossaryGroundedTables termId={1} identityGap={false} canEdit={false} onChanged={vi.fn()} />,
    );
    expect(await screen.findByTestId("glossary-grounded-tables-list")).toHaveTextContent(
      "sales.customers, sales.orders",
    );
    expect(screen.queryByTestId("glossary-grounded-tables-select")).toBeNull();
  });
});

describe("a table's Rows are instances of", () => {
  it("names the table's class term, and sets another in its place", async () => {
    api.grounded.set(1, [10]);
    render(<TableClassTerm tableId={10} />);
    const select = await screen.findByTestId("table-class-term-select");
    await waitFor(() => expect(select).toHaveValue("Customer"));
    fireEvent.click(select);
    fireEvent.click(await screen.findByRole("option", { name: "Order", hidden: true }));
    // One class term a table: the old one lets go first.
    await waitFor(() => expect(api.calls).toEqual(["unground 1 10", "ground 2 10"]));
    await waitFor(() => expect(screen.getByTestId("table-class-term-select")).toHaveValue("Order"));
  });

  it("clears it", async () => {
    api.grounded.set(1, [10]);
    render(<TableClassTerm tableId={10} />);
    const box = await screen.findByTestId("table-class-term");
    await waitFor(() =>
      expect(screen.getByTestId("table-class-term-select")).toHaveValue("Customer"),
    );
    const clear = box.querySelector("button");
    if (!clear) throw new Error("no clear button");
    fireEvent.click(clear);
    await waitFor(() => expect(api.calls).toEqual(["unground 1 10"]));
  });

  it("is read, linked to its term, by one who may not curate; none says so", async () => {
    auth.capabilities = ["glossary_read"];
    api.grounded.set(1, [10]);
    render(<TableClassTerm tableId={10} />);
    expect(await screen.findByTestId("table-class-term-link")).toHaveAttribute(
      "href",
      "/admin/ontology?term=1",
    );
    render(<TableClassTerm tableId={11} />);
    expect(await screen.findByTestId("table-class-term-none")).toHaveTextContent("No term");
    expect(within(document.body).queryByTestId("table-class-term-select")).toBeNull();
  });
});
