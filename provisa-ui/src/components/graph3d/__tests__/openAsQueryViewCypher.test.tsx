// Copyright (c) 2026 Kenneth Stott
// Canary: ed550c10-7db8-464e-b2c9-d565d90f3566
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2136, REQ-2174: a view that says its own Cypher (the ontology's terms and their links) opens
// as it in every state -- focused, or its GraphQL compiled to no Cypher -- never disabled.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { Route, Routes, useLocation } from "react-router-dom";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "analyst" }) }));
const compiles = vi.hoisted(() => ({ answer: null as unknown, calls: 0 }));
vi.mock("../../../hooks/useAdminQueries", () => ({
  useCompileQuery: () => ({
    compileQuery: async () => {
      compiles.calls += 1;
      return compiles.answer;
    },
  }),
}));

import { OpenAsQuery, type CypherOf } from "../OpenAsQuery";
import type { QueryLanguage } from "../useOntology";

const QUERY = "query Ontology {\n  _terms: on_terms { id: id }\n}";
const WALK =
  "MATCH (t:Terms {id: 7})-[:KIND_OF*0..2]-(a:Terms) " +
  "OPTIONAL MATCH (a)-[r:KIND_OF]->(b:Terms) RETURN DISTINCT a, r, b";

function Landed() {
  const location = useLocation();
  return <pre data-testid="landed">{JSON.stringify([location.pathname, location.state])}</pre>;
}

function view(cypher: CypherOf, focused: boolean, language: QueryLanguage = "graphql") {
  render(
    <Routes>
      <Route
        path="/"
        element={
          <OpenAsQuery
            query={QUERY}
            language={language}
            focused={focused}
            cypher={cypher}
            testId="x"
          />
        }
      />
      <Route path="*" element={<Landed />} />
    </Routes>,
  );
}

const landed = () => JSON.parse(screen.getByTestId("landed").textContent ?? "null");

beforeEach(() => {
  compiles.calls = 0;
  // The GraphQL compiles to no Cypher: the view's own is offered all the same.
  compiles.answer = {
    queries: [
      { root_field: "_terms", semantic_sql: "SELECT 1", compiled_cypher: null, cypher_error: "x" },
    ],
  };
});

describe("a view's own Cypher", () => {
  it.each([
    ["unfocused, its GraphQL compiled to no Cypher", false],
    ["focused", true],
  ])("is offered %s, and opens as itself", async (_state, focused) => {
    const build = vi.fn(async () => WALK);
    view({ key: "k", build }, focused);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    await waitFor(() => expect(screen.getByTestId("x-open-as-cypher")).not.toBeDisabled());
    expect(build).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByTestId("x-open-as-cypher"));
    expect(landed()).toEqual(["/graph", { query: WALK }]);
  });

  it("is not opened when it cannot be written", async () => {
    const build = vi.fn(async (): Promise<string> => {
      throw new Error("the ontology's terms have no Cypher label for the role");
    });
    view({ key: "k", build }, false);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    await waitFor(() => expect(build).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(screen.getByTestId("x-open-as-sql")).not.toBeDisabled());
    expect(screen.getByTestId("x-open-as-cypher")).toBeDisabled();
    expect(screen.queryByTestId("landed")).toBeNull();
  });

  it("opens All hops, written in Cypher, as itself", async () => {
    const build = vi.fn(async () => "unused");
    view({ key: "k", build }, false, "cypher");
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    fireEvent.click(await screen.findByTestId("x-open-as-cypher"));
    expect(build).not.toHaveBeenCalled();
    expect(landed()).toEqual(["/graph", { query: QUERY }]);
  });
});
