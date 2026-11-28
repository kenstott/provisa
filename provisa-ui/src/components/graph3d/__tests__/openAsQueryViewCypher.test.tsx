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
// as it in every state -- focused, or its GraphQL compiled to no Cypher -- never disabled; and as
// the SPARQL the server writes from that Cypher, in every state it opens as the Cypher (maintainer
// 2026-11-28: SPARQL was never offered in the 3D graph).

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

const sparqls = vi.hoisted(() => ({
  calls: [] as [string, string][],
  fail: null as string | null,
  refuse: null as string | null,
}));
vi.mock("../ontologyCypher", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../ontologyCypher")>()),
  cypherAsSparql: async (role: string, cypher: string) => {
    sparqls.calls.push([role, cypher]);
    if (sparqls.fail !== null) throw new Error(sparqls.fail);
    return sparqls.refuse !== null
      ? { sparql: null, error: sparqls.refuse }
      : { sparql: `# SPARQL of: ${cypher}`, error: null };
  },
}));

import { OpenAsQuery, type CypherOf } from "../OpenAsQuery";
import { allHopsCypher, drawnCypher } from "../ontologyCypher";
import WALKS from "./ontologyWalks.json";
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
  sparqls.calls.length = 0;
  sparqls.fail = null;
  sparqls.refuse = null;
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

describe("a view's own Cypher as SPARQL", () => {
  // Each state the ontology view opens as its own Cypher: Hops 0, 1, 2, 3 and All from one root
  // and from several, a focus's drawn terms; the Cypher each is, as the view writes it.
  const states = WALKS.walks.map((w) => {
    const written =
      w.writer === "drawn"
        ? drawnCypher(WALKS.graph, w.roots, w.edgeTypes)
        : allHopsCypher(WALKS.graph, w.roots, w.depth, w.edgeTypes);
    return [w.name, written, w.writer === "drawn"] as const;
  });

  it.each(states)(
    "is offered for %s, and opens as the server writes it",
    async (_n, walk, focused) => {
      view({ key: "k", build: async () => walk }, focused);
      fireEvent.click(screen.getByTestId("x-open-as-query"));
      await waitFor(() => expect(screen.getByTestId("x-open-as-sparql")).not.toBeDisabled());
      expect(sparqls.calls).toEqual([["analyst", walk]]);
      fireEvent.click(screen.getByTestId("x-open-as-sparql"));
      expect(landed()).toEqual(["/sparql", { query: `# SPARQL of: ${walk}` }]);
    },
  );

  it("covers Hops 1 to All, several roots and a focus", () => {
    const names = states.map(([n]) => n);
    for (const hops of ["Hops 1", "Hops 2", "Hops 3", "All hops"])
      expect(names.some((n) => n.startsWith(hops))).toBe(true);
    expect(names.some((n) => n.includes("several roots"))).toBe(true);
    expect(states.some(([, , focused]) => focused)).toBe(true);
  });

  it("opens All hops, written in Cypher, as the SPARQL written from it", async () => {
    view({ key: "k", build: async () => "unused" }, false, "cypher");
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    await waitFor(() => expect(screen.getByTestId("x-open-as-sparql")).not.toBeDisabled());
    expect(sparqls.calls).toEqual([["analyst", QUERY]]);
    fireEvent.click(screen.getByTestId("x-open-as-sparql"));
    expect(landed()).toEqual(["/sparql", { query: `# SPARQL of: ${QUERY}` }]);
  });

  it("is off while the Cypher is written, and asks the server nothing until it is", async () => {
    let done: (text: string) => void = () => undefined;
    const build = () => new Promise<string>((resolve) => (done = resolve));
    view({ key: "k", build }, false);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    expect(await screen.findByTestId("x-open-as-sparql")).toBeDisabled();
    expect(sparqls.calls).toEqual([]);
    done(WALK);
    await waitFor(() => expect(screen.getByTestId("x-open-as-sparql")).not.toBeDisabled());
  });

  it.each([
    ["the server says the read has no SPARQL form", "refuse"],
    ["the server cannot be asked", "fail"],
  ])("is off when %s", async (_s, how) => {
    if (how === "refuse") sparqls.refuse = "UNION";
    else sparqls.fail = "HTTP 503: no schema";
    view({ key: "k", build: async () => WALK }, false);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    await waitFor(() => expect(sparqls.calls).toHaveLength(1));
    await waitFor(() => expect(screen.getByTestId("x-open-as-cypher")).not.toBeDisabled());
    expect(screen.getByTestId("x-open-as-sparql")).toBeDisabled();
  });

  it("is off when the Cypher cannot be written", async () => {
    view(
      {
        key: "k",
        build: async () => {
          throw new Error("the ontology's terms have no Cypher label for the role");
        },
      },
      false,
    );
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    await waitFor(() => expect(screen.getByTestId("x-open-as-sql")).not.toBeDisabled());
    expect(screen.getByTestId("x-open-as-sparql")).toBeDisabled();
    expect(sparqls.calls).toEqual([]);
  });
});
