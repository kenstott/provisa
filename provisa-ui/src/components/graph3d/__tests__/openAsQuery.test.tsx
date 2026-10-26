// Copyright (c) 2026 Kenneth Stott
// Canary: d8f76690-fd86-46fa-95ed-7cfd02dd6178
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2136: a graph view's Open as query (→) opens the view's own query in the surface chosen:
// GraphQL as it runs, SQL and Cypher as it compiles, SPARQL later; none while a focus is set.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { Route, Routes, useLocation } from "react-router-dom";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "analyst" }) }));
const compiles = vi.hoisted(() => ({
  calls: [] as [string, string, boolean | undefined][],
  answer: null as unknown,
}));
vi.mock("../../../hooks/useAdminQueries", () => ({
  useCompileQuery: () => ({
    compileQuery: async (
      role: string,
      query: string,
      _variables?: unknown,
      _flatSql?: boolean,
      _flatCypher?: boolean,
      nodeOnlyCypher?: boolean,
    ) => {
      compiles.calls.push([role, query, nodeOnlyCypher]);
      return compiles.answer;
    },
  }),
}));

import { OpenAsQuery } from "../OpenAsQuery";

const QUERY = "query Ontology {\n  _terms: ont__terms { id: id }\n}";

/** Where the control navigated, and what it handed over. */
function Landed() {
  const location = useLocation();
  return <pre data-testid="landed">{JSON.stringify([location.pathname, location.state])}</pre>;
}

function view(query: string | null, focused = false) {
  render(
    <Routes>
      <Route path="/" element={<OpenAsQuery query={query} focused={focused} testId="x" />} />
      <Route path="*" element={<Landed />} />
    </Routes>,
  );
}

const landed = () => JSON.parse(screen.getByTestId("landed").textContent ?? "null");

beforeEach(() => {
  compiles.calls.length = 0;
  compiles.answer = {
    queries: [
      {
        semantic_sql: 'SELECT "id" FROM "ontology"."_terms"',
        compiled_cypher: "MATCH (n:GlossaryTerm) RETURN n.id",
        cypher_error: null,
      },
    ],
  };
});

describe("a graph view's Open as query", () => {
  it("opens the view's query itself in GraphQL", async () => {
    view(QUERY);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    fireEvent.click(await screen.findByTestId("x-open-as-graphql"));
    expect(landed()).toEqual(["/query", { query: QUERY }]);
  });

  it("opens its compiled semantic SQL in SQL, compiled as the user's role, its Cypher as nodes", async () => {
    view(QUERY);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    await waitFor(() => expect(screen.getByTestId("x-open-as-sql")).not.toBeDisabled());
    expect(compiles.calls).toEqual([["analyst", QUERY, true]]);
    fireEvent.click(screen.getByTestId("x-open-as-sql"));
    expect(landed()).toEqual(["/sql", { sql: 'SELECT "id" FROM "ontology"."_terms"' }]);
  });

  it("opens its Cypher in Cypher, and not when it has none", async () => {
    view(QUERY);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    await waitFor(() => expect(screen.getByTestId("x-open-as-cypher")).not.toBeDisabled());
    fireEvent.click(screen.getByTestId("x-open-as-cypher"));
    expect(landed()).toEqual(["/graph", { query: "MATCH (n:GlossaryTerm) RETURN n.id" }]);
  });

  it("opens each root field as its own Cypher statement, a root with none said in its place", async () => {
    compiles.answer = {
      queries: [
        {
          root_field: "_terms",
          semantic_sql: "SELECT 1",
          compiled_cypher: "MATCH (n:GlossaryTerm) RETURN n.id",
          cypher_error: null,
        },
        {
          root_field: "_term_edges",
          semantic_sql: "SELECT 2",
          compiled_cypher: null,
          cypher_error: "no mapping",
        },
        {
          root_field: "data_products",
          semantic_sql: "SELECT 3",
          compiled_cypher: "MATCH (p:DataProduct) RETURN p.id",
          cypher_error: null,
        },
      ],
    };
    view(QUERY);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    await waitFor(() => expect(screen.getByTestId("x-open-as-cypher")).not.toBeDisabled());
    fireEvent.click(screen.getByTestId("x-open-as-cypher"));
    expect(landed()).toEqual([
      "/graph",
      {
        query:
          "MATCH (n:GlossaryTerm) RETURN n.id;\n\n// _term_edges: no mapping;\n\n" +
          "MATCH (p:DataProduct) RETURN p.id",
      },
    ]);
  });

  it("offers no Cypher for a query that does not map to it", async () => {
    compiles.answer = {
      queries: [{ semantic_sql: "SELECT 1", compiled_cypher: null, cypher_error: "no mapping" }],
    };
    view(QUERY);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    await waitFor(() => expect(screen.getByTestId("x-open-as-sql")).not.toBeDisabled());
    expect(screen.getByTestId("x-open-as-cypher")).toBeDisabled();
  });

  it("offers SPARQL disabled until its endpoint exists", async () => {
    view(QUERY);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    expect(await screen.findByTestId("x-open-as-sparql")).toBeDisabled();
  });

  it("opens nothing while a focus is set, and compiles nothing", async () => {
    view(QUERY, true);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    for (const target of ["graphql", "sql", "cypher", "sparql"])
      expect(await screen.findByTestId(`x-open-as-${target}`)).toBeDisabled();
    expect(compiles.calls).toEqual([]);
  });

  it("is off before the view has read its query", () => {
    view(null);
    expect(screen.getByTestId("x-open-as-query")).toBeDisabled();
  });
});
